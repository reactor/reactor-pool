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

import java.time.Duration;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;

import org.jctools.queues.MpscArrayQueue;
import org.jctools.queues.MpscLinkedQueue8;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.Loggers;
import reactor.util.annotation.Nullable;

/**
 * The {@link SimplePool} is based on MPSC queues for idle resources and FIFO or LIFO data structures for
 * pending {@link Pool#acquire()} Monos.
 * It uses non-blocking drain loops to deliver resources to borrowers, which means that a resource could
 * be handed off on any of the following {@link Thread threads}:
 * <ul>
 *     <li>any thread on which a resource was last allocated</li>
 *     <li>any thread on which a resource was recently released</li>
 *     <li>any thread on which an {@link Pool#acquire()} {@link Mono} was subscribed</li>
 * </ul>
 * For a more deterministic approach, the {@link PoolBuilder#acquisitionScheduler(Scheduler)} property of the builder can be used.
 *
 * @author Simon Baslé
 */
abstract class SimplePool<POOLABLE> extends AbstractPool<POOLABLE> {

    final Queue<QueuePooledRef<POOLABLE>> elements;

    volatile int                                               acquired;
    private static final AtomicIntegerFieldUpdater<SimplePool> ACQUIRED = AtomicIntegerFieldUpdater.newUpdater(
            SimplePool.class, "acquired");

    volatile int                                               wip;
    private static final AtomicIntegerFieldUpdater<SimplePool> WIP = AtomicIntegerFieldUpdater.newUpdater(
            SimplePool.class, "wip");


    SimplePool(DefaultPoolConfig<POOLABLE> poolConfig) {
        super(poolConfig, Loggers.getLogger(SimplePool.class));
        int maxSize = poolConfig.allocationStrategy.estimatePermitCount();
        if (maxSize == Integer.MAX_VALUE) {
            this.elements = new MpscLinkedQueue8<>();
        }
        else {
            this.elements = new MpscArrayQueue<>(Math.max(2, maxSize));
        }

        int initSize = poolConfig.allocationStrategy.getPermits(poolConfig.initialSize);
        for (int i = 0; i < initSize; i++) {
            long start = metricsRecorder.now();
            try {
                POOLABLE poolable = Objects.requireNonNull(poolConfig.allocator.block(), "allocator returned null in constructor");
                metricsRecorder.recordAllocationSuccessAndLatency(metricsRecorder.measureTime(start));
                elements.offer(new QueuePooledRef<>(this, poolable)); //the pool slot won't access this pool instance until after it has been constructed
            }
            catch (Throwable e) {
                metricsRecorder.recordAllocationFailureAndLatency(metricsRecorder.measureTime(start));
                throw e;
            }
        }
    }

    /**
     * @return the next {@link reactor.pool.AbstractPool.Borrower} to serve
     */
    @Nullable
    abstract Borrower<POOLABLE> pendingPoll();

    /**
     * @param pending a new {@link reactor.pool.AbstractPool.Borrower} to register as pending
     * @return true if the pool had capacity to register this new pending
     */
    abstract boolean pendingOffer(Borrower<POOLABLE> pending);

    @Override
    public Mono<PooledRef<POOLABLE>> acquire() {
        return new QueueBorrowerMono<>(this, Duration.ZERO); //the mono is unknown to the pool until requested
    }

    @Override
    public Mono<PooledRef<POOLABLE>> acquire(Duration timeout) {
        return new QueueBorrowerMono<>(this, timeout); //the mono is unknown to the pool until requested
    }

    @Override
    void doAcquire(Borrower<POOLABLE> borrower) {
        if (isDisposed()) {
            borrower.fail(new RuntimeException("Pool has been shut down"));
            return;
        }

        pendingOffer(borrower);
        drain();
    }

    @Override
    boolean elementOffer(POOLABLE element) {
        return elements.offer(new QueuePooledRef<>(this, element));
    }

    @Override
    public int idleSize() {
        return elements.size();
    }

    @SuppressWarnings("WeakerAccess")
    final void maybeRecycleAndDrain(QueuePooledRef<POOLABLE> poolSlot) {
        if (!isDisposed()) {
            if (!poolConfig.evictionPredicate.test(poolSlot.poolable, poolSlot)) {
                metricsRecorder.recordRecycled();
                elements.offer(poolSlot);
                drain();
            }
            else {
                destroyPoolable(poolSlot).subscribe(null, e -> drain(), this::drain); //TODO manage errors?
            }
        }
        else {
            destroyPoolable(poolSlot).subscribe(null, e -> drain(), this::drain); //TODO manage errors?
        }
    }

    void drain() {
        if (WIP.getAndIncrement(this) == 0) {
            drainLoop();
        }
    }

    private void drainLoop() {
        int missed = 1;

        for (;;) {
            int availableCount = elements.size();
            int pendingCount = PENDING_COUNT.get(this);
            int permits = poolConfig.allocationStrategy.estimatePermitCount();

            if (availableCount == 0) {
                if (pendingCount > 0 && permits > 0) {
                    final Borrower<POOLABLE> borrower = pendingPoll(); //shouldn't be null
                    if (borrower == null) {
                        continue;
                    }
                    ACQUIRED.incrementAndGet(this);
                    if (borrower.get() || poolConfig.allocationStrategy.getPermits(1) != 1) {
                        ACQUIRED.decrementAndGet(this);
                        continue;
                    }
                    borrower.stopPendingCountdown();
                    long start = metricsRecorder.now();
                    Mono<POOLABLE> allocator = poolConfig.allocator;
                    Scheduler s = poolConfig.acquisitionScheduler;
                    if (s != Schedulers.immediate())  {
                        allocator = allocator.publishOn(s);
                    }
                    allocator.subscribe(newInstance -> borrower.deliver(new QueuePooledRef<>(this, newInstance)),
                                    e -> {
                                        metricsRecorder.recordAllocationFailureAndLatency(metricsRecorder.measureTime(start));
                                        ACQUIRED.decrementAndGet(this);
                                        poolConfig.allocationStrategy.returnPermits(1);
                                        borrower.fail(e);
                                    },
                                    () -> metricsRecorder.recordAllocationSuccessAndLatency(metricsRecorder.measureTime(start)));
                }
            }
            else if (pendingCount > 0) {
                //there are objects ready and unclaimed in the pool + a pending
                QueuePooledRef<POOLABLE> slot = elements.poll();
                if (slot == null) continue;

                //TODO test the idle eviction scenario
                if (poolConfig.evictionPredicate.test(slot.poolable, slot)) {
                    destroyPoolable(slot).subscribe(null, e -> drain(), this::drain);
                    continue;
                }

                //there is a party currently pending acquiring
                Borrower<POOLABLE> inner = pendingPoll();
                if (inner == null) {
                    elements.offer(slot);
                    continue;
                }
                inner.stopPendingCountdown();
                ACQUIRED.incrementAndGet(this);
                poolConfig.acquisitionScheduler.schedule(() -> inner.deliver(slot));
            }

            missed = WIP.addAndGet(this, -missed);
            if (missed == 0) {
                break;
            }
        }
    }

    static final class QueuePooledRef<T> extends AbstractPooledRef<T> {

        final SimplePool<T> pool;

        QueuePooledRef(SimplePool<T> pool, T poolable) {
            super(poolable, pool.metricsRecorder);
            this.pool = pool;
        }

        @Override
        public Mono<Void> release() {
            if (pool.isDisposed()) {
                ACQUIRED.decrementAndGet(pool); //immediately clean up state
                markReleased();
                return pool.destroyPoolable(this);
            }

            Publisher<Void> cleaner;
            try {
                cleaner = pool.poolConfig.releaseHandler.apply(poolable);
            }
            catch (Throwable e) {
                ACQUIRED.decrementAndGet(pool); //immediately clean up state
                markReleased();
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
                return pool.destroyPoolable(this).then(Mono.fromRunnable(pool::drain));
            });
        }
    }

    static final class QueueBorrowerMono<T> extends Mono<PooledRef<T>> {

        final SimplePool<T> parent;
        final Duration      acquireTimeout;

        QueueBorrowerMono(SimplePool<T> pool, Duration acquireTimeout) {
            this.parent = pool;
            this.acquireTimeout = acquireTimeout;
        }

        @Override
        public void subscribe(CoreSubscriber<? super PooledRef<T>> actual) {
            Objects.requireNonNull(actual, "subscribing with null");
            Borrower<T> borrower = new Borrower<>(actual, parent, acquireTimeout);
            actual.onSubscribe(borrower);
        }
    }

    private static final class QueuePoolRecyclerInner<T> implements CoreSubscriber<Void>, Scannable, Subscription {

        final CoreSubscriber<? super Void> actual;
        final SimplePool<T>                pool;

        //poolable can be checked for null to protect against protocol errors
        QueuePooledRef<T> pooledRef;
        Subscription upstream;
        long start;

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
                this.start = pool.metricsRecorder.now();
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

            //TODO should we separate reset errors?
            pool.metricsRecorder.recordResetLatency(pool.metricsRecorder.measureTime(start));

            pool.destroyPoolable(slot).subscribe(null, null, pool::drain); //TODO manage errors?

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

            pool.metricsRecorder.recordResetLatency(pool.metricsRecorder.measureTime(start));

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

    private static final class QueuePoolRecyclerMono<T> extends Mono<Void> implements Scannable {

        final Publisher<Void> source;
        final AtomicReference<QueuePooledRef<T>> slotRef;

        QueuePoolRecyclerMono(Publisher<Void> source, QueuePooledRef<T> poolSlot) {
            this.source = source;
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

        @Override
        @Nullable
        public Object scanUnsafe(Attr key) {
            if (key == Attr.PREFETCH) return Integer.MAX_VALUE;
            if (key == Attr.PARENT) return source;
            return null;
        }
    }

}
