/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
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

package reactor.util.pool.impl;

import org.jctools.queues.MpscArrayQueue;
import org.jctools.queues.MpscLinkedQueue8;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoOperator;
import reactor.core.publisher.Operators;
import reactor.util.Loggers;
import reactor.util.concurrent.Queues;
import reactor.util.pool.api.PoolConfig;
import reactor.util.pool.api.PooledRef;

import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * A simple {@link reactor.util.pool.api.Pool} implementation based on MPSC queues for both idle resources and pending
 * borrowers. It uses non-blocking drain loops to deliver resources to borrowers, which means that a resource could
 * be handed off on any of the following {@link Thread threads}:
 * <ul>
 *     <li>any thread on which a resource was last allocated</li>
 *     <li>any thread on which a resource was recently released</li>
 *     <li>any thread on which an Pool{@link #acquire()} {@link Mono} was subscribed</li>
 * </ul>
 * <p>
 * For a more deterministic approach, the {@link PoolConfig#deliveryScheduler()} property of the {@link PoolConfig} can
 * be used.
 *
 * @author Simon Baslé
 */
public final class QueuePool<POOLABLE> extends AbstractPool<POOLABLE> {

    private static final Queue TERMINATED = Queues.empty().get();

    final Queue<QueuePooledRef<POOLABLE>> elements;

    volatile int acquired;
    private static final AtomicIntegerFieldUpdater<QueuePool> ACQUIRED = AtomicIntegerFieldUpdater.newUpdater(QueuePool.class, "acquired");

    volatile Queue<Borrower<POOLABLE>> pending;
    private static final AtomicReferenceFieldUpdater<QueuePool, Queue> PENDING = AtomicReferenceFieldUpdater.newUpdater(QueuePool.class, Queue.class, "pending");

    volatile int wip;
    private static final AtomicIntegerFieldUpdater<QueuePool> WIP = AtomicIntegerFieldUpdater.newUpdater(QueuePool.class, "wip");


    QueuePool(PoolConfig<POOLABLE> poolConfig) {
        super(poolConfig, Loggers.getLogger(QueuePool.class));
        this.pending = new MpscLinkedQueue8<>(); //unbounded MPSC
        int maxSize = poolConfig.allocationStrategy().estimatePermitCount();
        if (maxSize == Integer.MAX_VALUE) {
            this.elements = new MpscLinkedQueue8<>();
        }
        else {
            this.elements = new MpscArrayQueue<>(Math.max(2, maxSize));
        }

        int initSize = poolConfig.allocationStrategy().getPermits(poolConfig.initialSize());
        for (int i = 0; i < initSize; i++) {
            POOLABLE poolable = Objects.requireNonNull(poolConfig.allocator().block(), "allocator returned null in constructor");
            elements.offer(new QueuePooledRef<>(this, poolable)); //the pool slot won't access this pool instance until after it has been constructed
        }
    }

    @Override
    public Mono<PooledRef<POOLABLE>> acquire() {
        return new QueueBorrowerMono<>(this); //the mono is unknown to the pool until subscribed
    }

    //the actual acquire logic happens in the QueueBorrower mono

    @SuppressWarnings("WeakerAccess")
    final void maybeRecycleAndDrain(QueuePooledRef<POOLABLE> poolSlot) {
        if (pending != TERMINATED) {
            if (!poolConfig.evictionPredicate().test(poolSlot)) {
                elements.offer(poolSlot);
            }
            else {
                poolConfig.allocationStrategy().addPermit();
                destroyPoolable(poolSlot.poolable).subscribe(); //TODO manage errors?
            }
            drain();
        }
        else {
            poolConfig.allocationStrategy().addPermit();
            destroyPoolable(poolSlot.poolable).subscribe(); //TODO manage errors?
        }
    }

    private void drain() {
        if (WIP.getAndIncrement(this) == 0) {
            drainLoop();
        }
    }

    private void drainLoop() {
        int missed = 1;

        for (;;) {
            int availableCount = elements.size();
            int pendingCount = pending.size();
            int permits = poolConfig.allocationStrategy().estimatePermitCount();

            if (availableCount == 0) {
                if (pendingCount > 0 && permits > 0) {
                    final Borrower<POOLABLE> borrower = pending.poll(); //shouldn't be null
                    if (borrower == null) {
                        continue;
                    }
                    ACQUIRED.incrementAndGet(this);
                    if (borrower.get() || !poolConfig.allocationStrategy().getPermit()) {
                        ACQUIRED.decrementAndGet(this);
                        continue;
                    }
                    poolConfig.allocator()
                            .publishOn(poolConfig.deliveryScheduler())
                            .subscribe(newInstance -> borrower.deliver(new QueuePooledRef<>(this, newInstance)),
                                    e -> {
                                        ACQUIRED.decrementAndGet(this);
                                        poolConfig.allocationStrategy().addPermit();
                                        borrower.fail(e);
                                    });
                }
            }
            else if (pendingCount > 0) {
                //there are objects ready and unclaimed in the pool + a pending
                QueuePooledRef<POOLABLE> slot = elements.poll();
                if (slot == null) continue;

                //there is a party currently pending acquiring
                Borrower<POOLABLE> inner = pending.poll();
                if (inner == null) {
                    elements.offer(slot);
                    continue;
                }
                ACQUIRED.incrementAndGet(this);
                poolConfig.deliveryScheduler().schedule(() -> inner.deliver(slot));
            }

            missed = WIP.addAndGet(this, -missed);
            if (missed == 0) {
                break;
            }
        }
    }

    @Override
    public void dispose() {
        @SuppressWarnings("unchecked")
        Queue<Borrower<POOLABLE>> q = PENDING.getAndSet(this, TERMINATED);
        if (q != TERMINATED) {
            while(!q.isEmpty()) {
                q.poll().fail(new RuntimeException("Pool has been shut down"));
            }

            while (!elements.isEmpty()) {
                destroyPoolable(elements.poll().poolable()).block();
            }
        }
    }

    @Override
    public boolean isDisposed() {
        return pending == TERMINATED;
    }


    static final class QueuePooledRef<T> extends AbstractPooledRef<T> {

        final QueuePool<T> pool;

        QueuePooledRef(QueuePool<T> pool, T poolable) {
            super(poolable);
            this.pool = pool;
        }

        @Override
        public Mono<Void> release() {
            if (PENDING.get(pool) == TERMINATED) {
                ACQUIRED.decrementAndGet(pool); //immediately clean up state
                return pool.destroyPoolable(poolable);
            }

            Mono<Void> cleaner;
            try {
                cleaner = pool.poolConfig.resetResource().apply(poolable);
            }
            catch (Throwable e) {
                ACQUIRED.decrementAndGet(pool); //immediately clean up state
                return Mono.error(new IllegalStateException("Couldn't apply cleaner function", e));
            }
            //the PoolRecyclerMono will wrap the cleaning Mono returned by the Function and perform state updates
            return new QueuePoolRecyclerMono<>(cleaner, this);
        }

        @Override
        public Mono<Void> invalidate() {
            return Mono.defer(() -> {
                //immediately clean up state
                ACQUIRED.decrementAndGet(pool);
                return pool.destroyPoolable(poolable);
            });
        }
    }

    static final class QueueBorrowerMono<T> extends Mono<PooledRef<T>> {

        final QueuePool<T> parent;

        QueueBorrowerMono(QueuePool<T> pool) {
            this.parent = pool;
        }

        @Override
        public void subscribe(CoreSubscriber<? super PooledRef<T>> actual) {
            Objects.requireNonNull(actual, "subscribing with null");

            @SuppressWarnings("unchecked")
            Queue<Borrower<T>> pending = PENDING.get(parent);
            if (pending == TERMINATED) {
                Operators.error(actual, new RuntimeException("Pool has been shut down"));
                return;
            }

            Borrower<T> borrower = new Borrower<>(actual);
            pending.add(borrower);
            actual.onSubscribe(borrower);
            parent.drain();
        }
    }

    private static final class QueuePoolRecyclerInner<T> implements CoreSubscriber<Void>, Scannable, Subscription {

        final CoreSubscriber<? super Void> actual;
        final QueuePool<T> pool;

        //poolable can be checked for null to protect against protocol errors
        QueuePooledRef<T> pooledRef;
        Subscription upstream;

        //once protects against multiple requests
        volatile int once;
        static final AtomicIntegerFieldUpdater<QueuePoolRecyclerInner> ONCE = AtomicIntegerFieldUpdater.newUpdater(QueuePoolRecyclerInner.class, "once");

        QueuePoolRecyclerInner(CoreSubscriber<? super Void> actual, QueuePooledRef<T> pooledRef) {
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
            QueuePooledRef<T> slot = pooledRef;
            pooledRef = null;
            if (slot == null) {
                Operators.onErrorDropped(throwable, actual.currentContext());
                return;
            }

            //some operators might immediately produce without request (eg. fromRunnable)
            // we decrement ACQUIRED EXACTLY ONCE to indicate that the poolable was released by the user
            if (ONCE.compareAndSet(this, 0, 1)) {
                ACQUIRED.decrementAndGet(pool);
            }

            pool.poolConfig.allocationStrategy().addPermit();
            pool.destroyPoolable(slot.poolable).subscribe(); //TODO manage errors?
            pool.drain();

            actual.onError(throwable);
        }

        @Override
        public void onComplete() {
            QueuePooledRef<T> slot = pooledRef;
            pooledRef = null;
            if (slot == null) {
                return;
            }

            //some operators might immediately produce without request (eg. fromRunnable)
            // we decrement ACQUIRED EXACTLY ONCE to indicate that the poolable was released by the user
            if (ONCE.compareAndSet(this, 0, 1)) {
                ACQUIRED.decrementAndGet(pool);
            }

            pool.maybeRecycleAndDrain(slot);
            actual.onComplete();
        }

        @Override
        public void request(long l) {
            if (Operators.validate(l)) {
                upstream.request(l);
                // we decrement ACQUIRED EXACTLY ONCE to indicate that the poolable was released by the user
                if (ONCE.compareAndSet(this, 0, 1)) {
                    ACQUIRED.decrementAndGet(pool);
                }
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

    private static final class QueuePoolRecyclerMono<T> extends MonoOperator<Void, Void> {

        final AtomicReference<QueuePooledRef<T>> slotRef;

        QueuePoolRecyclerMono(Mono<? extends Void> source, QueuePooledRef<T> poolSlot) {
            super(source);
            this.slotRef = new AtomicReference<>(poolSlot);
        }

        @Override
        public void subscribe(CoreSubscriber<? super Void> actual) {
            QueuePooledRef<T> slot = slotRef.getAndSet(null);
            if (slot == null) {
                Operators.complete(actual);
            }
            else {
                slot.markReleased();
                QueuePoolRecyclerInner<T> qpr = new QueuePoolRecyclerInner<>(actual, slot);
                source.subscribe(qpr);
            }
        }
    }

}
