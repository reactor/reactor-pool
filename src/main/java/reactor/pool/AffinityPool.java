/*
 * Copyright (c) 2018-Present Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.pool;

import java.util.Collections;
import java.util.Deque;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;

import org.jctools.queues.MpmcArrayQueue;
import org.jctools.queues.MpscLinkedQueue8;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.util.Loggers;
import reactor.util.annotation.Nullable;

/**
 * @author Simon Baslé
 */
@SuppressWarnings("WeakerAccess")
final class AffinityPool<POOLABLE> extends AbstractPool<POOLABLE> {

    //FIXME put that info on another volatile
    @SuppressWarnings("RawTypeCanBeGeneric")
    static final Map TERMINATED = Collections.emptyMap();

    final Queue<AffinityPooledRef<POOLABLE>> availableElements; //needs to be at least MPSC. producers include fastpath threads, only consumer is slowpath winner thread
    final Function<? super Long, ? extends SubPool<POOLABLE>>     subPoolFactory;

    volatile Map<Long, SubPool<POOLABLE>> pools;
    static final AtomicReferenceFieldUpdater<AffinityPool, Map> POOLS = AtomicReferenceFieldUpdater.newUpdater(AffinityPool.class, Map.class, "pools");

    volatile int slowPathWip;
    static final AtomicIntegerFieldUpdater<AffinityPool> SLOWPATH_WIP = AtomicIntegerFieldUpdater.newUpdater(AffinityPool.class, "slowPathWip");


    public AffinityPool(DefaultPoolConfig<POOLABLE> poolConfig) {
        super(poolConfig, Loggers.getLogger(AffinityPool.class));
        this.pools = new ConcurrentHashMap<>();
        this.subPoolFactory = (poolConfig.isLifo) ?
                it -> new LifoSubPool<>(this) :
                it -> new FifoSubPool<>(this);

        int maxSize = poolConfig.sizeLimitStrategy.estimatePermitCount();
        if (maxSize == Integer.MAX_VALUE) {
            this.availableElements = new ConcurrentLinkedQueue<>();
        }
        else {
            this.availableElements = new MpmcArrayQueue<>(Math.max(maxSize, 2));
        }
    }

    @Override
    public Mono<PooledRef<POOLABLE>> acquire() {
        //Note the pool isn't aware of the mono until requested.
        return new AffinityBorrowerMono<>(this);
    }

    @Override
    void doAcquire(Borrower<POOLABLE> borrower) {
        if (pools == TERMINATED) {
            borrower.fail(new RuntimeException("Pool has been shut down"));
            return;
        }

        SubPool<POOLABLE> subPool = pools.computeIfAbsent(Thread.currentThread().getId(), this.subPoolFactory);

        AffinityPooledRef<POOLABLE> element = availableElements.poll();
        if (element != null) {

            //TODO test this scenario
            if (poolConfig.evictionPredicate.test(element.poolable, element)) {
                destroyPoolable(element).subscribe(); //this returns a permit
                allocateOrPend(subPool, borrower);
            }
            else {
                metricsRecorder.recordFastPath();
                borrower.deliver(element);
            }
        }
        else {
            allocateOrPend(subPool, borrower);
        }
    }

    @Override
    boolean elementOffer(POOLABLE element) {
        return availableElements.offer(new AffinityPooledRef<>(this, element));
    }

    @Override
    int idleSize() {
        return availableElements.size();
    }

    void allocateOrPend(SubPool<POOLABLE> subPool, Borrower<POOLABLE> borrower) {
        int count = poolConfig.sizeLimitStrategy.getPermits(1);

        if (count == 0) {
            //cannot create, add to pendingLocal
            subPool.offerPending(borrower);
            //now it's just a matter of waiting for a #release
            return;
        }

        for (int i = 0; i < count - 1; i++) {
            long start = metricsRecorder.now();
            poolConfig.allocator
                .subscribe(newInstance -> {
                        metricsRecorder.recordAllocationSuccessAndLatency(metricsRecorder.measureTime(start));
                        recycle(new AffinityPooledRef<>(this, newInstance));
                    },
                    e -> {
                        metricsRecorder.recordAllocationFailureAndLatency(metricsRecorder.measureTime(start));
                        poolConfig.sizeLimitStrategy.returnPermits(1);
                    });
        }

        long start = metricsRecorder.now();
        poolConfig.allocator
            //we expect the allocator will publish in the same thread or a "compatible" one
            // (like EventLoopGroup for Netty connections), which makes it more suitable to use with Schedulers.immediate()
//                    .publishOn(poolConfig.acquisitionScheduler())
            .subscribe(newInstance -> {
                    metricsRecorder.recordAllocationSuccessAndLatency(metricsRecorder.measureTime(start));
                    borrower.deliver(new AffinityPooledRef<>(this, newInstance));
                },
                e -> {
                    metricsRecorder.recordAllocationFailureAndLatency(metricsRecorder.measureTime(start));
                    poolConfig.sizeLimitStrategy.returnPermits(1);
                    borrower.fail(e);
                });
    }

    void recycle(AffinityPooledRef<POOLABLE> pooledRef) {
        metricsRecorder.recordRecycled();
        SubPool<POOLABLE> subPool = pools.get(Thread.currentThread().getId());
        if (subPool == null || !subPool.tryDirectRecycle(pooledRef)) {
            availableElements.offer(pooledRef);
            slowPathRecycle();
        }
    }

    void slowPathRecycle() {
        if (SLOWPATH_WIP.getAndIncrement(this) != 0) {
            return;
        }
        //TODO should we randomize the order of subpools to try?
        for(;;) {
            if (availableElements.peek() != null) { //do not poll immediately
                boolean lookAtSubPools = true;
                SubPool<POOLABLE> directMatch = pools.get(Thread.currentThread().getId());
                if (directMatch != null && directMatch.tryLockForSlowPath()) {
                    Borrower<POOLABLE> pending = directMatch.getPendingAndUnlock();
                    if (pending != null) {
                        //this might return null, racing with doAcquire
                        AffinityPooledRef<POOLABLE> ref = availableElements.poll();
                        if (ref != null) {
                            lookAtSubPools = false;
                            metricsRecorder.recordSlowPath();
                            pending.deliver(ref);
                        }
                        else {
                            //this reorders the pending but should happen rarely enough
                            directMatch.offerPending(pending);
                            continue; //loop again to re-evaluate availableElements
                        }
                    }
                }

                if (lookAtSubPools) {
                    //we only arrive at this point if there was no direct match
                    for (SubPool<POOLABLE> subPool : pools.values()) {
                        if (subPool.tryLockForSlowPath()) {
                            Borrower<POOLABLE> pending = subPool.getPendingAndUnlock();
                            if (pending != null) {
                                AffinityPooledRef<POOLABLE> ref = availableElements.poll();
                                if (ref == null) {
                                    subPool.offerPending(pending);
                                    //continue
                                }
                                else {
                                    metricsRecorder.recordSlowPath();
                                    pending.deliver(ref);
                                    break; //break out of the subpool iteration
                                }
                            }
                        }
                    }
                }
            }

            if (SLOWPATH_WIP.decrementAndGet(this) == 0) {
                return;
            }
        }
    }

    void bestEffortAllocateOrPend() {
        SubPool<POOLABLE> directMatch = pools.get(Thread.currentThread().getId());
        if (directMatch != null && directMatch.tryLockForSlowPath()) {
            Borrower<POOLABLE> pending = directMatch.getPendingAndUnlock();
            if (pending != null) {
                allocateOrPend(directMatch, pending);
                //return
            }
        }
        else {
            for (SubPool<POOLABLE> subPool : pools.values()) {
                if (subPool.tryLockForSlowPath()) {
                    Borrower<POOLABLE> pending = subPool.getPendingAndUnlock();
                    if (pending != null) {
                        allocateOrPend(subPool, pending);
                        return;
                    }
                }
            }
        }
    }


    @Override
    public void dispose() {
        @SuppressWarnings("unchecked")
        Map<Long, SubPool<POOLABLE>> toClose = POOLS.getAndSet(this, TERMINATED);
        if (toClose != TERMINATED) {
            for (SubPool<POOLABLE> subPool : toClose.values()) {
                Borrower<POOLABLE> pending;
                while((pending = subPool.pollPending()) != null) {
                    pending.fail(new RuntimeException("Pool has been shut down"));
                }
            }
            toClose.clear();

            while(!availableElements.isEmpty()) {
                destroyPoolable(availableElements.poll()).block();
            }
        }
    }

    @Override
    public boolean isDisposed() {
        return pools == TERMINATED;
    }


    static abstract class SubPool<POOLABLE> {

        final AffinityPool<POOLABLE> parent;

        volatile int directReleaseInProgress;
        static final AtomicIntegerFieldUpdater<SubPool> DIRECT_RELEASE_WIP = AtomicIntegerFieldUpdater.newUpdater(SubPool.class, "directReleaseInProgress");


        protected SubPool(AffinityPool<POOLABLE> parent) {
            this.parent = parent;
        }

        /**
         * Add a new pending {@link Borrower} to the subpool, which
         * can either work in FIFO order (like a {@link Deque#offerLast(Object) queue offer})
         * or in LIFO order (like a {@link Deque#offerFirst(Object) stack push}).
         *
         * @param pending the pending {@link Borrower} to add to the SubPool
         */
        abstract void offerPending(Borrower<POOLABLE> pending);

        /**
         * Remove a pending from the subpool and return it, or {@code null} if it is empty.
         * The subpool can either work in FIFO order (like a {@link Deque#pollLast() queue poll})
         * or in LIFO order (like a {@link Deque#pollFirst() stack pop}.
         *
         * @return the next pending {@link Borrower} to serve, or null if none
         */
        @Nullable
        abstract Borrower<POOLABLE> pollPending();

        public boolean tryLockForSlowPath() {
            return DIRECT_RELEASE_WIP.compareAndSet(this, 0, 1);
        }

        @Nullable
        public Borrower<POOLABLE> getPendingAndUnlock() {
            Borrower<POOLABLE> m = pollPending();
            DIRECT_RELEASE_WIP.decrementAndGet(this);
            return m;
        }

        public boolean tryDirectRecycle(AffinityPooledRef<POOLABLE> ref) {
            if (!DIRECT_RELEASE_WIP.compareAndSet(this, 0, 1)) {
                return false;
            }

            Borrower<POOLABLE> m = pollPending();

            DIRECT_RELEASE_WIP.decrementAndGet(this);

            if (m != null) {
                parent.metricsRecorder.recordFastPath();
                m.deliver(ref);
                return true;
            }
            return false;
        }
    }

    static final class FifoSubPool<POOLABLE> extends SubPool<POOLABLE> {

        final Queue<Borrower<POOLABLE>> localPendings; //needs to be MPSC. Producer: any thread that doAcquire. Consumer: whomever has the LOCKED.

        FifoSubPool(AffinityPool<POOLABLE> parent) {
            super(parent);
            this.localPendings = new MpscLinkedQueue8<>();
        }

        @Override
        public void offerPending(Borrower<POOLABLE> pending) {
            int maxPending = parent.poolConfig.maxPending;
            for (;;) {
                int currentPending = AbstractPool.PENDING_COUNT.get(parent);
                if (maxPending >= 0 && currentPending == maxPending) {
                    pending.fail(new IllegalStateException("Pending acquire queue has reached its maximum size of " + maxPending));
                    return;
                }
                else if (AbstractPool.PENDING_COUNT.compareAndSet(parent, currentPending, currentPending + 1)) {
                    this.localPendings.offer(pending);
                    return;
                }
            }
        }

        @Override
        public Borrower<POOLABLE> pollPending() {
            Borrower<POOLABLE> b = this.localPendings.poll();
            if (b != null) {
                AbstractPool.PENDING_COUNT.decrementAndGet(parent);
            }
            return b;
        }

    }

    static final class LifoSubPool<POOLABLE> extends SubPool<POOLABLE> {

        final TreiberStack<Borrower<POOLABLE>> localPendings;

        LifoSubPool(AffinityPool<POOLABLE> parent) {
            super(parent);
            this.localPendings = new TreiberStack<>();
        }

        @Override
        public void offerPending(Borrower<POOLABLE> pending) {
            int maxPending = parent.poolConfig.maxPending;
            for (;;) {
                int currentPending = AbstractPool.PENDING_COUNT.get(parent);
                if (maxPending >= 0 && currentPending == maxPending) {
                    pending.fail(new IllegalStateException("Pending acquire queue has reached its maximum size of " + maxPending));
                    return;
                }
                else if (AbstractPool.PENDING_COUNT.compareAndSet(parent, currentPending, currentPending + 1)) {
                    this.localPendings.push(pending);
                    return;
                }
            }
        }

        @Override
        public Borrower<POOLABLE> pollPending() {
            Borrower<POOLABLE> b = this.localPendings.pop();
            if (b != null) {
                AbstractPool.PENDING_COUNT.decrementAndGet(parent);
            }
            return b;
        }
    }

    static final class AffinityPooledRef<T> extends AbstractPooledRef<T> {

        final AffinityPool<T> pool;

        AffinityPooledRef(AffinityPool<T> pool, T poolable) {
            super(poolable, pool.metricsRecorder);
            this.pool = pool;
        }

        @Override
        public Mono<Void> release() {
            if (POOLS.get(pool) == TERMINATED) {
                markReleased();
                return pool.destroyPoolable(this);
            }

            Publisher<Void> cleaner;
            try {
                cleaner = pool.poolConfig.releaseHandler.apply(poolable);
            }
            catch (Throwable e) {
                markReleased(); //TODO should this lead to destroy?
                return Mono.error(new IllegalStateException("Couldn't apply cleaner function", e));
            }

            //the PoolRecyclerMono will wrap the cleaning Mono returned by the Function and perform state updates
            return new AffinityPoolRecyclerMono<>(cleaner, this);
        }

        @Override
        public Mono<Void> invalidate() {
            return pool.destroyPoolable(this);
        }
    }

    static final class AffinityBorrowerMono<T> extends Mono<PooledRef<T>> {

        final AffinityPool<T> parent;

        AffinityBorrowerMono(AffinityPool<T> pool) {
            this.parent = pool;
        }

        @Override
        public void subscribe(CoreSubscriber<? super PooledRef<T>> actual) {
            Borrower<T> borrower = new Borrower<>(actual, parent);
            actual.onSubscribe(borrower);
        }
    }

    private static final class AffinityPoolRecyclerInner<T> implements CoreSubscriber<Void>, Scannable, Subscription {

        final CoreSubscriber<? super Void> actual;
        final AffinityPool<T> pool;

        //poolable can be checked for null to protect against protocol errors
        AffinityPooledRef<T> pooledRef;
        Subscription upstream;
        long start;

        AffinityPoolRecyclerInner(CoreSubscriber<? super Void> actual, AffinityPooledRef<T> pooledRef) {
            this.actual = actual;
            this.pooledRef = Objects.requireNonNull(pooledRef, "pooledRef");
            this.pool = pooledRef.pool;
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (Operators.validate(upstream, s)) {
                this.start = pool.metricsRecorder.now();
                this.upstream = s;
                actual.onSubscribe(this);
            }
        }

        @Override
        public void onNext(Void o) {
            //N/A
        }

        @Override
        public void onError(Throwable throwable) {
            AffinityPooledRef<T> slot = pooledRef;
            pooledRef = null;
            pool.metricsRecorder.recordResetLatency(pool.metricsRecorder.measureTime(start));
            if (slot == null) {
                Operators.onErrorDropped(throwable, actual.currentContext());
                return;
            }

            pool.destroyPoolable(slot).subscribe(); //TODO manage further errors?
            actual.onError(throwable);
        }

        @Override
        public void onComplete() {
            AffinityPooledRef<T> slot = pooledRef;
            pooledRef = null;
            pool.metricsRecorder.recordResetLatency(pool.metricsRecorder.measureTime(start));
            if (slot == null) {
                return;
            }

            actual.onComplete();

            if (!pool.poolConfig.evictionPredicate.test(slot.poolable, slot)) {
                pool.recycle(slot);
            }
            else {
                pool.destroyPoolable(slot).subscribe(); //TODO manage errors?

                //simplified version of what we do in doAcquire, with the caveat that we don't try to create a SubPool
                pool.bestEffortAllocateOrPend();
            }
        }

        @Override
        public void request(long l) {
            if (Operators.validate(l)) {
                upstream.request(l);
            }
        }

        @Override
        public void cancel() {
            //NO-OP, once requested, release cannot be cancelled
        }


        @Override
        public Object scanUnsafe(Attr key) {
            if (key == Attr.ACTUAL) return actual;
            if (key == Attr.PARENT) return upstream;
            if (key == Attr.CANCELLED) return false;
            if (key == Attr.TERMINATED) return pooledRef == null;
            if (key == Attr.BUFFERED) return (pooledRef == null) ? 0 : 1;
            return null;
        }
    }

    private static final class AffinityPoolRecyclerMono<T> extends Mono<Void> implements Scannable {

        final Publisher<Void> source;
        final AtomicReference<AffinityPoolRecyclerInner<T>> recyclerRef;

        AffinityPooledRef<T> slot;

        AffinityPoolRecyclerMono(Publisher<Void> source, AffinityPooledRef<T> poolSlot) {
            this.source = source;
            this.recyclerRef = new AtomicReference<>();
            this.slot = poolSlot;
        }

        @Override
        public void subscribe(CoreSubscriber<? super Void> actual) {
            if (recyclerRef.get() != null) {
                Operators.complete(actual);
            }
            else {
                AffinityPoolRecyclerInner<T> apr = new AffinityPoolRecyclerInner<>(actual, slot);
                if (recyclerRef.compareAndSet(null, apr)) {
                    slot.markReleased();
                    source.subscribe(apr);
                    slot = null;
                }
                else {
                    Operators.complete(actual);
                }
            }
        }

        @Override
        @Nullable
        public Object scanUnsafe(Attr key) {
            if (key == Attr.PREFETCH) return Integer.MAX_VALUE;
            if (key == Attr.PARENT) return source;
            return null;
        }
    }
}
