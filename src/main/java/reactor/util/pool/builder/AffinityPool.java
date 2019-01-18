/*
 * Copyright (c) 2011-2019 Pivotal Software Inc, All Rights Reserved.
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

package reactor.util.pool.builder;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoOperator;
import reactor.core.publisher.Operators;
import reactor.util.Loggers;
import reactor.util.annotation.Nullable;
import reactor.util.concurrent.Queues;
import reactor.util.pool.api.PoolConfig;
import reactor.util.pool.api.PooledRef;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * @author Simon Baslé
 */
public final class AffinityPool<POOLABLE> extends AbstractPool<POOLABLE> {

    static final Map TERMINATED = Collections.emptyMap();

    final Queue<AffinityPooledRef<POOLABLE>> availableElements; //needs to be MPSC. producers include fastpath threads, only consumer is slowpath winner thread

    volatile long live;
    static final AtomicLongFieldUpdater<AffinityPool> LIVE = AtomicLongFieldUpdater.newUpdater(AffinityPool.class, "live");

    volatile Map<Long, SubPool<POOLABLE>> pools;
    static final AtomicReferenceFieldUpdater<AffinityPool, Map> POOLS = AtomicReferenceFieldUpdater.newUpdater(AffinityPool.class, Map.class, "pools");

    volatile int slowPathWip;
    static final AtomicIntegerFieldUpdater<AffinityPool> SLOWPATH_WIP = AtomicIntegerFieldUpdater.newUpdater(AffinityPool.class, "slowPathWip");


    public AffinityPool(PoolConfig<POOLABLE> poolConfig) {
        super(poolConfig, Loggers.getLogger(AffinityPool.class));
        this.pools = new ConcurrentHashMap<>();
        this.availableElements = Queues.<AffinityPooledRef<POOLABLE>>unboundedMultiproducer().get();

        for (int i = 0; i < poolConfig.initialSize(); i++) {
            POOLABLE poolable = Objects.requireNonNull(poolConfig.allocator().block(), "allocator returned null in constructor");
            availableElements.offer(new AffinityPooledRef<>(this, poolable)); //the pool slot won't access this pool instance until after it has been constructed
        }
        LIVE.lazySet(this, availableElements.size());
    }

    @Override
    public Mono<PooledRef<POOLABLE>> acquire() {
        return new AffinityPoolMono<>(this);
    }

    void doAcquire(Borrower<POOLABLE> borrower) {
        if (POOLS.get(this) == TERMINATED) {
            borrower.fail(new RuntimeException("Pool has been shut down"));
            return;
        }

        AffinityPooledRef<POOLABLE> element = availableElements.poll();
        if (element != null) {
            borrower.deliver(element);
        }
        else {
            long l = live;
            if (l < poolConfig.maxSize() && LIVE.compareAndSet(this, l, l+1)) {
                poolConfig.allocator()
                        //we expect the allocator will publish in the same thread or a "compatible" one
                        // (like EventLoopGroup for Netty connections), which makes it more suitable to use with Schedulers.immediate()
                        // but we'll still accommodate for the deliveryScheduler
                        .publishOn(poolConfig.deliveryScheduler())
                        .subscribe(newInstance -> borrower.deliver(new AffinityPooledRef<>(this, newInstance)),
                                e -> {
                                    LIVE.decrementAndGet(this);
                                    borrower.fail(e);
                                });
            }
            else {
                //cannot create, add to pendingLocal
                SubPool<POOLABLE> subPool = pools.computeIfAbsent(Thread.currentThread().getId(), i -> new SubPool<>(this));
                subPool.localPendings.offer(borrower);
                //now it's just a matter of waiting for a #release
            }
        }
    }

    void recreateOrPend() {
        SubPool<POOLABLE> subPool = pools.get(Thread.currentThread().getId());
        if (subPool.tryLockForSlowPath()) {
            Borrower<POOLABLE> borrower = subPool.getPendingAndUnlock();
            if (borrower != null) {
                long l = live;
                if (l < poolConfig.maxSize() && LIVE.compareAndSet(this, l, l+1)) {
                    poolConfig.allocator()
                            //we expect the allocator will publish in the same thread or a "compatible" one
                            // (like EventLoopGroup for Netty connections), which makes it more suitable to use with Schedulers.immediate()
                            // but we'll still accommodate for the deliveryScheduler
                            .publishOn(poolConfig.deliveryScheduler())
                            .subscribe(newInstance -> borrower.deliver(new AffinityPooledRef<>(this, newInstance)),
                                    e -> {
                                        LIVE.decrementAndGet(this);
                                        borrower.fail(e);
                                    });
                }
                else {
                    subPool.localPendings.offer(borrower);
                }
            }
        }
    }

    void recycle(AffinityPooledRef<POOLABLE> pooledRef) {
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
        for(;;) {
            AffinityPooledRef<POOLABLE> ref = availableElements.poll();
            boolean delivered = false;
            if (ref != null) {
                SubPool<POOLABLE> directMatch = pools.get(Thread.currentThread().getId());
                if (directMatch != null && directMatch.tryLockForSlowPath()) {
                    Borrower<POOLABLE> pending = directMatch.getPendingAndUnlock();
                    if (pending != null) {
                        delivered = true;
                        pending.deliver(ref);
                    }
                }
                else {
                    for (SubPool<POOLABLE> subPool : pools.values()) {
                        if (subPool.tryLockForSlowPath()) {
                            Borrower<POOLABLE> pending = subPool.getPendingAndUnlock();
                            if (pending != null) {
                                delivered = true;
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
                disposePoolable(availableElements.poll().poolable);
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
            this.localPendings = Queues.<Borrower<POOLABLE>>unboundedMultiproducer().get();
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
            while (!DIRECT_RELEASE_WIP.compareAndSet(this, 0, 1)) { }

            Borrower<POOLABLE> m = localPendings.poll();

            DIRECT_RELEASE_WIP.decrementAndGet(this);

            if (m != null) {
                m.deliver(ref);
                return true;
            }
            return false;
        }
    }

    static final class AffinityPooledRef<T> extends AbstractPooledRef<T> {

        final AffinityPool<T> pool;

        AffinityPooledRef(AffinityPool<T> pool, T poolable) {
            super(poolable);
            this.pool = pool;
        }

        @Override
        public Mono<Void> release() {
            if (POOLS.get(pool) == TERMINATED) {
                pool.disposePoolable(poolable);
                return Mono.empty();
            }

            Mono<Void> cleaner;
            try {
                cleaner = pool.poolConfig.resetResource().apply(poolable);
            }
            catch (Throwable e) {
                return Mono.error(new IllegalStateException("Couldn't apply cleaner function", e));
            }

            //the PoolRecyclerMono will wrap the cleaning Mono returned by the Function and perform state updates
            return new AffinityPoolRecyclerMono<>(cleaner, this);
        }

        @Override
        public void invalidate() {
            //immediately clean up state
            pool.disposePoolable(poolable);
        }
    }

    private static final class AffinityPoolMono<T> extends Mono<PooledRef<T>> {

        final AffinityPool<T> parent;

        AffinityPoolMono(AffinityPool<T> pool) {
            this.parent = pool;
        }

        @Override
        public void subscribe(CoreSubscriber<? super PooledRef<T>> actual) {
            Objects.requireNonNull(actual, "subscribing with null");

            Borrower<T> p = new Borrower<>(actual, parent);
            actual.onSubscribe(p);
        }
    }

    private static final class AffinityPoolRecyclerInner<T> implements CoreSubscriber<Void>, Scannable, Subscription {

        final CoreSubscriber<? super Void> actual;
        final AffinityPool<T> pool;

        //poolable can be checked for null to protect against protocol errors
        AffinityPooledRef<T> pooledRef;
        Subscription upstream;

        AffinityPoolRecyclerInner(CoreSubscriber<? super Void> actual, AffinityPooledRef<T> pooledRef) {
            this.actual = actual;
            this.pooledRef = Objects.requireNonNull(pooledRef, "pooledRef");
            this.pool = pooledRef.pool;
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (Operators.validate(upstream, s)) {
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
            if (slot == null) {
                Operators.onErrorDropped(throwable, actual.currentContext());
                return;
            }

            LIVE.decrementAndGet(pool);
            pool.disposePoolable(slot.poolable);
            actual.onError(throwable);
        }

        @Override
        public void onComplete() {
            AffinityPooledRef<T> slot = pooledRef;
            pooledRef = null;
            if (slot == null) {
                return;
            }

            actual.onComplete();

            if (!pool.poolConfig.evictionPredicate().test(slot)) {
                pool.recycle(slot);
            }
            else {
                LIVE.decrementAndGet(pool);
                pool.disposePoolable(slot.poolable);
                pool.recreateOrPend();
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
