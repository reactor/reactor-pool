/*
 * Copyright (c) 2018-Present Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.pool.impl;

import org.jctools.queues.MpmcArrayQueue;
import org.jctools.queues.MpscLinkedQueue8;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoOperator;
import reactor.core.publisher.Operators;
import reactor.pool.PooledRef;
import reactor.util.Loggers;
import reactor.util.annotation.Nullable;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * @author Simon Baslé
 */
@SuppressWarnings("WeakerAccess")
final class AffinityPool<POOLABLE> extends AbstractPool<POOLABLE> {

    //FIXME put that info on another volatile
    @SuppressWarnings("RawTypeCanBeGeneric")
    static final Map TERMINATED = Collections.emptyMap();

    final Queue<AffinityPooledRef<POOLABLE>> availableElements; //needs to be at least MPSC. producers include fastpath threads, only consumer is slowpath winner thread

    volatile Map<Long, SubPool<POOLABLE>> pools;
    static final AtomicReferenceFieldUpdater<AffinityPool, Map> POOLS = AtomicReferenceFieldUpdater.newUpdater(AffinityPool.class, Map.class, "pools");

    volatile int slowPathWip;
    static final AtomicIntegerFieldUpdater<AffinityPool> SLOWPATH_WIP = AtomicIntegerFieldUpdater.newUpdater(AffinityPool.class, "slowPathWip");


    public AffinityPool(DefaultPoolConfig<POOLABLE> poolConfig) {
        super(poolConfig, Loggers.getLogger(AffinityPool.class));
        this.pools = new ConcurrentHashMap<>();

        int maxSize = poolConfig.allocationStrategy().estimatePermitCount();
        if (maxSize == Integer.MAX_VALUE) {
            this.availableElements = new ConcurrentLinkedQueue<>();
        }
        else {
            this.availableElements = new MpmcArrayQueue<>(Math.max(maxSize, 2));
        }

        int toBuild = poolConfig.allocationStrategy().getPermits(poolConfig.initialSize());

        for (int i = 0; i < toBuild; i++) {
            long start = poolConfig.metricsRecorder().now();
            try {
                POOLABLE poolable = Objects.requireNonNull(poolConfig.allocator().block(), "allocator returned null in constructor");
                poolConfig.metricsRecorder().recordAllocationSuccessAndLatency(poolConfig.metricsRecorder().measureTime(start));
                availableElements.offer(new AffinityPooledRef<>(this, poolable)); //the pool slot won't access this pool instance until after it has been constructed
            }
            catch (Throwable t) {
                poolConfig.metricsRecorder().recordAllocationFailureAndLatency(poolConfig.metricsRecorder().measureTime(start));
                throw t;
            }
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

        SubPool<POOLABLE> subPool = pools.computeIfAbsent(Thread.currentThread().getId(), i -> new SubPool<>(this));

        AffinityPooledRef<POOLABLE> element = availableElements.poll();
        if (element != null) {

            //TODO test this scenario
            if (poolConfig.evictionPredicate().test(element)) {
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

    void allocateOrPend(SubPool<POOLABLE> subPool, Borrower<POOLABLE> borrower) {
        if (poolConfig.allocationStrategy().getPermits(1) == 1) {
            long start = metricsRecorder.now();
            poolConfig.allocator()
                    //we expect the allocator will publish in the same thread or a "compatible" one
                    // (like EventLoopGroup for Netty connections), which makes it more suitable to use with Schedulers.immediate()
//                    .publishOn(poolConfig.deliveryScheduler())
                    .subscribe(newInstance -> {
                                metricsRecorder.recordAllocationSuccessAndLatency(metricsRecorder.measureTime(start));
                                borrower.deliver(new AffinityPooledRef<>(this, newInstance));
                            },
                            e -> {
                                metricsRecorder.recordAllocationFailureAndLatency(metricsRecorder.measureTime(start));
                                poolConfig.allocationStrategy().returnPermits(1);
                                borrower.fail(e);
                            });
        }
        else {
            //cannot create, add to pendingLocal
            subPool.localPendings.offer(borrower);
            //now it's just a matter of waiting for a #release
        }
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
            AffinityPooledRef<POOLABLE> ref = availableElements.poll();
            boolean delivered = false;
            if (ref != null) {
                SubPool<POOLABLE> directMatch = pools.get(Thread.currentThread().getId());
                if (directMatch != null && directMatch.tryLockForSlowPath()) {
                    Borrower<POOLABLE> pending = directMatch.getPendingAndUnlock();
                    if (pending != null) {
                        delivered = true;
                        metricsRecorder.recordSlowPath();
                        pending.deliver(ref);
                    }
                }
                if (!delivered) {
                    for (SubPool<POOLABLE> subPool : pools.values()) {
                        if (subPool.tryLockForSlowPath()) {
                            Borrower<POOLABLE> pending = subPool.getPendingAndUnlock();
                            if (pending != null) {
                                delivered = true;
                                metricsRecorder.recordSlowPath();
                                pending.deliver(ref);
                                break;
                            }
                        }
                    }
                }
                if (!delivered) {
                    availableElements.offer(ref);
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
                Queue<Borrower<POOLABLE>> q = subPool.localPendings;
                while(!q.isEmpty()) {
                    q.poll().fail(new RuntimeException("Pool has been shut down"));
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


    static final class SubPool<POOLABLE> {

        final AffinityPool<POOLABLE> parent;
        final Queue<Borrower<POOLABLE>> localPendings; //needs to be MPSC. Producer: any thread that doAcquire. Consumer: whomever has the LOCKED.

        volatile int directReleaseInProgress;
        static final AtomicIntegerFieldUpdater<SubPool> DIRECT_RELEASE_WIP = AtomicIntegerFieldUpdater.newUpdater(SubPool.class, "directReleaseInProgress");

        SubPool(AffinityPool<POOLABLE> parent) {
            this.parent = parent;
            this.localPendings = new MpscLinkedQueue8<>();
        }

        boolean tryLockForSlowPath() {
            return DIRECT_RELEASE_WIP.compareAndSet(this, 0, 1);
        }

        @Nullable
        Borrower<POOLABLE> getPendingAndUnlock() {
            Borrower<POOLABLE> m = localPendings.poll();
            DIRECT_RELEASE_WIP.decrementAndGet(this);
            return m;
        }

        boolean tryDirectRecycle(AffinityPooledRef<POOLABLE> ref) {
            if (!DIRECT_RELEASE_WIP.compareAndSet(this, 0, 1)) {
                return false;
            }

            Borrower<POOLABLE> m = localPendings.poll();

            DIRECT_RELEASE_WIP.decrementAndGet(this);

            if (m != null) {
                parent.metricsRecorder.recordFastPath();
                m.deliver(ref);
                return true;
            }
            return false;
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

            Mono<Void> cleaner;
            try {
                cleaner = pool.poolConfig.resetResource().apply(poolable);
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
            Objects.requireNonNull(actual, "subscribing with null");
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

            if (!pool.poolConfig.evictionPredicate().test(slot)) {
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

    private static final class AffinityPoolRecyclerMono<T> extends MonoOperator<Void, Void> {

        final AtomicReference<AffinityPoolRecyclerInner<T>> recyclerRef;

        AffinityPooledRef<T> slot;

        AffinityPoolRecyclerMono(Mono<? extends Void> source, AffinityPooledRef<T> poolSlot) {
            super(source);
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
    }
}
