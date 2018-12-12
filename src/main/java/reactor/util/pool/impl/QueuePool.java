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

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoOperator;
import reactor.core.publisher.Operators;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.annotation.Nullable;
import reactor.util.concurrent.Queues;
import reactor.util.pool.Pool;
import reactor.util.pool.PoolConfig;
import reactor.util.pool.PoolSlot;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;

/**
 * A {@link Queues MPSC Queue}-based implementation of {@link Pool}.
 *
 * @author Simon Baslé
 */
public class QueuePool<POOLABLE> implements Pool<POOLABLE>, Disposable {

    private static final Queue TERMINATED = Queues.empty().get();

    //A pool should be rare enough that having instance loggers should be ok
    //This helps with testability of some methods that for now mainly log
    private final Logger logger = Loggers.getLogger(QueuePool.class);

    private final PoolConfig<POOLABLE> poolConfig;
    final Queue<QueuePoolSlot<POOLABLE>> elements;

    volatile int borrowed;
    private static final AtomicIntegerFieldUpdater<QueuePool> BORROWED = AtomicIntegerFieldUpdater.newUpdater(QueuePool.class, "borrowed");

    volatile int live;
    private static final AtomicIntegerFieldUpdater<QueuePool> LIVE = AtomicIntegerFieldUpdater.newUpdater(QueuePool.class, "live");

    volatile Queue<PoolInner<POOLABLE>> pending = Queues.<PoolInner<POOLABLE>>unboundedMultiproducer().get();
    private static final AtomicReferenceFieldUpdater<QueuePool, Queue> PENDING = AtomicReferenceFieldUpdater.newUpdater(QueuePool.class, Queue.class, "pending");

    volatile int wip;
    private static final AtomicIntegerFieldUpdater<QueuePool> WIP = AtomicIntegerFieldUpdater.newUpdater(QueuePool.class, "wip");


    QueuePool(PoolConfig<POOLABLE> poolConfig) {
        this.poolConfig = poolConfig;
        this.elements = Queues.<QueuePoolSlot<POOLABLE>>get(poolConfig.maxSize()).get();

        for (int i = 0; i < poolConfig.minSize(); i++) {
            POOLABLE poolable = Objects.requireNonNull(poolConfig.allocator().block(), "allocator returned null in constructor");
            elements.offer(new QueuePoolSlot<>(this, poolable)); //the pool slot won't access this pool instance until after it has been constructed
        }
        this.live = elements.size();
    }

    @Override
    public Mono<PoolSlot<POOLABLE>> acquire() {
        return new QueuePoolMono<>(this); //the mono is unknown to the pool until both subscribed and requested
    }

    @Override
    public <V> Flux<V> borrow(Function<Mono<POOLABLE>, Publisher<V>> processingFunction) {
        return Flux.usingWhen(acquire(),
                slot -> processingFunction.apply(Mono.justOrEmpty(slot.poolable())),
                PoolSlot::releaseMono,
                PoolSlot::releaseMono);
    }

    @SuppressWarnings("WeakerAccess")
    final void registerPendingBorrower(PoolInner<POOLABLE> s) {
        if (pending != TERMINATED) {
            pending.add(s);
            drain();
        }
        else {
            s.fail(new RuntimeException("Pool has been shut down"));
        }
    }

    @SuppressWarnings("WeakerAccess")
    final void maybeRecycleAndDrain(QueuePoolSlot<POOLABLE> poolSlot) {
        if (pending != TERMINATED) {
            if (poolConfig.validator().test(poolSlot.poolable())) {
                elements.offer(poolSlot);
            }
            else {
                LIVE.decrementAndGet(this);
            }
            drain();
        }
        else {
            LIVE.decrementAndGet(this);
            dispose(poolSlot.poolable());
        }
    }

    @SuppressWarnings("WeakerAccess")
    void dispose(@Nullable POOLABLE poolable) {
        if (poolable instanceof Disposable) {
            ((Disposable) poolable).dispose();
        }
        else if (poolable instanceof Closeable) {
            try {
                ((Closeable) poolable).close();
            } catch (IOException e) {
                logger.trace("Failure while discarding a released Poolable that is Closeable, could not close", e);
            }
        }
        //TODO anything else to throw away the Poolable?
    }

    private void drain() {
        if (WIP.getAndIncrement(this) == 0) {
            drainLoop();
        }
    }

    private void drainLoop() {
        int missed = 1;
        int maxElements = poolConfig.maxSize();

        for (;;) {
            int availableCount = elements.size();
            int pendingCount = pending.size();
            int total = LIVE.get(this);

            if (availableCount == 0) {
                if (pendingCount > 0 && total < maxElements) {
                    final PoolInner<POOLABLE> borrower = pending.poll(); //shouldn't be null
                    if (borrower == null) {
                        continue;
                    }
                    BORROWED.incrementAndGet(this);
                    if (borrower.state == PoolInner.STATE_CANCELLED || !LIVE.compareAndSet(this, total, total + 1)) {
                        BORROWED.decrementAndGet(this);
                        continue;
                    }
                    poolConfig.allocator()
                            .publishOn(poolConfig.deliveryScheduler())
                            .subscribe(newInstance -> borrower.deliver(new QueuePoolSlot<>(this, newInstance)),
                                    e -> {
                                        BORROWED.decrementAndGet(this);
                                        LIVE.decrementAndGet(this);
                                        borrower.fail(e);
                                    });
                }
            }
            else if (pendingCount > 0) {
                //there are objects ready and unclaimed in the pool + a pending
                QueuePoolSlot<POOLABLE> slot = elements.poll();
                if (slot == null) continue;

                //there is a party currently pending borrowing
                PoolInner<POOLABLE> inner = pending.poll();
                if (inner == null) {
                    elements.offer(slot);
                    continue;
                }
                BORROWED.incrementAndGet(this);
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
        Queue<PoolInner<POOLABLE>> q = PENDING.getAndSet(this, TERMINATED);
        if (q != TERMINATED) {
            while(!q.isEmpty()) {
                q.poll().fail(new RuntimeException("Pool has been shut down"));
            }

            while (!elements.isEmpty()) {
                dispose(elements.poll().poolable());
            }
        }
    }

    @Override
    public boolean isDisposed() {
        return pending == TERMINATED;
    }


    static final class QueuePoolSlot<T> implements PoolSlot<T> {

        final QueuePool<T> pool;
        final long creationTimestamp;

        volatile T poolable;

        volatile int borrowCount;

        static final AtomicIntegerFieldUpdater<QueuePoolSlot> BORROW = AtomicIntegerFieldUpdater.newUpdater(QueuePoolSlot.class, "borrowCount");

        QueuePoolSlot(QueuePool<T> pool, T poolable) {
            this.pool = pool;
            this.poolable = poolable;
            this.creationTimestamp = System.currentTimeMillis();
        }

        @Override
        public T poolable() {
            return poolable;
        }

        /**
         * Atomically increment the {@link #borrowCount()} of this slot, returning the new value.
         *
         * @return the incremented {@link #borrowCount()}
         */
        int borrowIncrement() {
            return BORROW.incrementAndGet(this);
        }

        @Override
        public int borrowCount() {
            return BORROW.get(this);
        }

        @Override
        public long age() {
            return System.currentTimeMillis() - creationTimestamp;
        }

        @Override
        public Mono<Void> releaseMono() {
            if (PENDING.get(pool) == TERMINATED) {
                BORROWED.decrementAndGet(pool); //immediately clean up state
                pool.dispose(poolable);
                return Mono.empty();
            }

            Mono<Void> cleaner;
            try {
                cleaner = pool.poolConfig.cleaner().apply(poolable);
            }
            catch (Throwable e) {
                BORROWED.decrementAndGet(pool); //immediately clean up state
                return Mono.error(new IllegalStateException("Couldn't apply cleaner function", e));
            }
            //the PoolRecyclerMono will wrap the cleaning Mono returned by the Function and perform state updates
            return new QueuePoolRecyclerMono<>(cleaner, this);
        }

        @Override
        public void release() {
            releaseMono().subscribe(v -> {}, e -> pool.logger.debug("error while releasing with release()", e));
        }

        @Override
        public void invalidate() {
            //immediately clean up state
            BORROWED.decrementAndGet(pool);
            pool.dispose(poolable);
        }

        @Override
        public String toString() {
            return "PoolSlot{" +
                    "poolable=" + poolable +
                    ", age=" + age() + "ms" +
                    ", borrowCount=" + borrowCount +
                    '}';
        }
    }

    private static final class PoolInner<T> implements Scannable, Subscription {

        final CoreSubscriber<? super QueuePoolSlot<T>> actual;

        final QueuePool<T> parent;

        private static final int STATE_INIT = 0;
        private static final int STATE_REQUESTED = 1;
        private static final int STATE_CANCELLED = 2;

        volatile int state;
        static final AtomicIntegerFieldUpdater<PoolInner> STATE = AtomicIntegerFieldUpdater.newUpdater(PoolInner.class, "state");


        PoolInner(CoreSubscriber<? super QueuePoolSlot<T>> actual, QueuePool<T> parent) {
            this.actual = actual;
            this.parent = parent;
        }

        @Override
        public void request(long n) {
            if (Operators.validate(n) && STATE.compareAndSet(this, STATE_INIT, STATE_REQUESTED)) {
                parent.registerPendingBorrower(this);
            }
        }

        @Override
        public void cancel() {
            STATE.getAndSet(this, STATE_CANCELLED);
        }

        @Override
        @Nullable
        public Object scanUnsafe(Attr key) {
            if (key == Attr.PARENT) return parent;
            if (key == Attr.CANCELLED) return state == STATE_CANCELLED;
            if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return state == STATE_REQUESTED ? 1 : 0;
            if (key == Attr.ACTUAL) return actual;

            return null;
        }

        private void deliver(QueuePoolSlot<T> poolSlot) {
            if (parent.logger.isTraceEnabled()) {
                parent.logger.info("deliver(" + poolSlot + ") in state " + state);
            }
            switch (state) {
                case STATE_REQUESTED:
                    poolSlot.borrowIncrement();
                    actual.onNext(poolSlot);
                    actual.onComplete();
                    break;
                case STATE_CANCELLED:
                    poolSlot.releaseMono().subscribe(aVoid -> {}, actual::onError);
                    break;
                default:
                    //shouldn't happen since the PoolInner isn't registered with the pool before having requested
                    poolSlot.releaseMono().subscribe(aVoid -> {}, actual::onError, () -> actual.onError(Exceptions.failWithOverflow()));
            }
        }

        private void fail(Throwable error) {
            if (state == STATE_REQUESTED) {
                actual.onError(error);
            }
        }

    }
    private static final class QueuePoolMono<T> extends Mono<PoolSlot<T>> {

        final QueuePool<T> parent;

        QueuePoolMono(QueuePool<T> pool) {
            this.parent = pool;
        }

        @Override
        public void subscribe(CoreSubscriber<? super PoolSlot<T>> actual) {
            Objects.requireNonNull(actual, "subscribing with null");

            PoolInner<T> p = new PoolInner<>(actual, parent);
            actual.onSubscribe(p);
        }
    }

    private static final class QueuePoolRecyclerInner<T> implements CoreSubscriber<Void>, Scannable, Subscription {

        final CoreSubscriber<? super Void> actual;
        final QueuePool<T> pool;

        //poolable can be checked for null to protect against protocol errors
        QueuePoolSlot<T> poolSlot;
        Subscription upstream;

        //once protects against multiple requests
        volatile int once;
        static final AtomicIntegerFieldUpdater<QueuePoolRecyclerInner> ONCE = AtomicIntegerFieldUpdater.newUpdater(QueuePoolRecyclerInner.class, "once");

        QueuePoolRecyclerInner(CoreSubscriber<? super Void> actual, QueuePoolSlot<T> poolSlot) {
            this.actual = actual;
            this.poolSlot = Objects.requireNonNull(poolSlot, "poolSlot");
            this.pool = poolSlot.pool;
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
            QueuePoolSlot<T> slot = poolSlot;
            poolSlot = null;
            if (slot == null) {
                Operators.onErrorDropped(throwable, actual.currentContext());
                return;
            }

            //some operators might immediately produce without request (eg. fromRunnable)
            // we decrement BORROWED EXACTLY ONCE to indicate that the poolable was released by the user
            if (ONCE.compareAndSet(this, 0, 1)) {
                BORROWED.decrementAndGet(pool);
            }

            LIVE.decrementAndGet(pool);
            pool.dispose(slot.poolable);
            pool.drain();

            actual.onError(throwable);
        }

        @Override
        public void onComplete() {
            QueuePoolSlot<T> slot = poolSlot;
            poolSlot = null;
            if (slot == null) {
                return;
            }

            //some operators might immediately produce without request (eg. fromRunnable)
            // we decrement BORROWED EXACTLY ONCE to indicate that the poolable was released by the user
            if (ONCE.compareAndSet(this, 0, 1)) {
                BORROWED.decrementAndGet(pool);
            }

            pool.maybeRecycleAndDrain(slot);
            actual.onComplete();
        }

        @Override
        public void request(long l) {
            if (Operators.validate(l)) {
                upstream.request(l);
                // we decrement BORROWED EXACTLY ONCE to indicate that the poolable was released by the user
                if (ONCE.compareAndSet(this, 0, 1)) {
                    BORROWED.decrementAndGet(pool);
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
            if (key == Attr.TERMINATED) return poolSlot == null;
            if (key == Attr.BUFFERED) return (poolSlot == null) ? 0 : 1;
            return null;
        }
    }

    private static final class QueuePoolRecyclerMono<T> extends MonoOperator<Void, Void> {

        final AtomicReference<QueuePoolSlot<T>> slotRef;

        QueuePoolRecyclerMono(Mono<? extends Void> source, QueuePoolSlot<T> poolSlot) {
            super(source);
            this.slotRef = new AtomicReference<>(poolSlot);
        }

        @Override
        public void subscribe(CoreSubscriber<? super Void> actual) {
            QueuePoolSlot<T> slot = slotRef.getAndSet(null);
            if (slot == null) {
                Operators.complete(actual);
            }
            else {
                QueuePoolRecyclerInner<T> qpr = new QueuePoolRecyclerInner<>(actual, slot);
                source.subscribe(qpr);
            }
        }
    }

}
