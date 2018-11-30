package reactor.util.pool.impl;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.core.publisher.SignalType;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.annotation.Nullable;
import reactor.util.concurrent.Queues;
import reactor.util.pool.Pool;
import reactor.util.pool.PoolConfig;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * @author Simon Baslé
 */
public class QueuePool<POOLABLE> implements Pool<POOLABLE>, Disposable {

    private static final Queue TERMINATED = Queues.empty().get();

    //A pool should be rare enough that having instance loggers should be ok
    //This helps with testability of some methods that for now mainly log
    private final Logger logger = Loggers.getLogger(QueuePool.class);

    final PoolConfig<POOLABLE> poolConfig;
    final Queue<POOLABLE> elements;

    volatile int borrowed;
    private static final AtomicIntegerFieldUpdater<QueuePool> BORROWED = AtomicIntegerFieldUpdater.newUpdater(QueuePool.class, "borrowed");

    volatile Queue<PoolInner<POOLABLE>> pending = Queues.<PoolInner<POOLABLE>>unboundedMultiproducer().get();
    private static final AtomicReferenceFieldUpdater<QueuePool, Queue> PENDING = AtomicReferenceFieldUpdater.newUpdater(QueuePool.class, Queue.class, "pending");

    volatile int wip;
    private static final AtomicIntegerFieldUpdater<QueuePool> WIP = AtomicIntegerFieldUpdater.newUpdater(QueuePool.class, "wip");


    public QueuePool(PoolConfig<POOLABLE> poolConfig) {
        this.poolConfig = poolConfig;
        this.elements = Queues.<POOLABLE>get(poolConfig.maxSize()).get();

        for (int i = 0; i < poolConfig.minSize(); i++) {
            elements.offer(poolConfig.allocator().block());
        }
    }

    @Override
    public Mono<POOLABLE> borrow() {
        return new QueuePoolMono<>(this); //the mono is unknown to the pool until both subscribed and requested
    }

    @Override
    public Mono<Void> release(final POOLABLE poolable) {
        //busy loop waiting for the drain loop to finish
        // /!\ TAKE CARE TO ALWAYS RESET wip IN ALL EXIT PATHS
        while (!WIP.compareAndSet(this, 0, 1)) {}

        //once we are sure the drain loop isn't in the process of allocating or looking at BORROWED, we decrement
        // BORROWED to indicate that the poolable was released by the user
        BORROWED.decrementAndGet(this);

        if (PENDING.get(this) == TERMINATED) {
            dispose(poolable);
            return Mono.fromRunnable(() -> WIP.set(this, 0));
        }

        try {
            Mono<Void> recycler = poolConfig.cleaner().apply(poolable);
            return recycler
                    .doFinally(sig -> {
                        if (sig == SignalType.ON_COMPLETE) {
                            maybeRecycleAndDrain(poolable);
                        }
                        else {
                            dispose(poolable);
                            WIP.set(this, 0);
                            drainLoop();
                        }
                    });
        }
        catch (Throwable e) {
            WIP.set(this, 0);
            return Mono.error(new IllegalStateException("Couldn't apply cleaner function", e));
        }
    }

    @Override
    public void releaseSync(final POOLABLE poolable) {
        release(poolable).block();
    }

    final void registerPendingBorrower(PoolInner<POOLABLE> s) {
        if (pending != TERMINATED) {
            pending.add(s);
            drain();
        }
        else {
            s.fail(new RuntimeException("Pool has been shut down"));
        }
    }

    final void maybeRecycleAndDrain(POOLABLE poolable) {
        if (pending != TERMINATED) {
            if (poolConfig.validator().test(poolable)) {
                elements.offer(poolable);
            }
            WIP.set(this, 0);
            drainLoop();
        }
        else {
            dispose(poolable);
        }
    }

    void dispose(POOLABLE poolable) {
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
            int borrowedCount = BORROWED.get(this);

            if (availableCount == 0) {
                if (pendingCount > 0 && borrowedCount < maxElements) {
                    BORROWED.incrementAndGet(this);
                    final PoolInner<POOLABLE> borrower = pending.poll(); //shouldn't be null
                    if (borrower == null) {
                        BORROWED.decrementAndGet(this);
                        throw new NullPointerException("borrower null when pending " + pendingCount);
                    }
                    if (borrower.state == PoolInner.STATE_CANCELLED) {
                        BORROWED.decrementAndGet(this);
                        continue;
                    }

                    poolConfig.allocator()
                            .publishOn(poolConfig.deliveryScheduler())
                            .subscribe(borrower::deliver,
                                    e -> {
                                        BORROWED.decrementAndGet(this);
                                        borrower.fail(e);
                                    });
                }
            }
            else if (pendingCount > 0) {
                //there are objects ready and unclaimed in the pool + a pending
                POOLABLE poolable = elements.poll();
                if (poolable == null) throw new NullPointerException("poolable null when available " + availableCount);

                //there is a party currently pending borrowing
                PoolInner<POOLABLE> inner = pending.poll();
                if (inner == null) {
                    elements.offer(poolable);
                    throw new NullPointerException("inner null when pendingCount " + pendingCount + ", recomputed pending size is " + pending.size());
                }
                BORROWED.incrementAndGet(this);
                poolConfig.deliveryScheduler().schedule(() -> inner.deliver(poolable));
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
                dispose(elements.poll());
            }
        }
    }

    @Override
    public boolean isDisposed() {
        return pending == TERMINATED;
    }

    private static final class PoolInner<T> implements Scannable, Subscription {

        final CoreSubscriber<? super T> actual;

        final QueuePool<T> parent;

        private static final int STATE_INIT = 0;
        private static final int STATE_REQUESTED = 1;
        private static final int STATE_CANCELLED = 2;

        volatile int state;
        static final AtomicIntegerFieldUpdater<PoolInner> STATE = AtomicIntegerFieldUpdater.newUpdater(PoolInner.class, "state");


        PoolInner(CoreSubscriber<? super T> actual, QueuePool<T> parent) {
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

        private void deliver(T poolable) {
            if (parent.logger.isTraceEnabled()) {
                parent.logger.info("deliver(" + poolable + ") in state " + state);
            }
            switch (state) {
                case STATE_REQUESTED:
                    actual.onNext(poolable);
                    actual.onComplete();
                    break;
                case STATE_CANCELLED:
                    parent.release(poolable).subscribe(aVoid -> {}, actual::onError);
                    break;
                default:
                    //shouldn't happen since the PoolInner isn't registered with the pool before having requested
                    parent.release(poolable).subscribe(aVoid -> {}, actual::onError, () -> actual.onError(Exceptions.failWithOverflow()));
            }
        }

        private void fail(Throwable error) {
            if (state == STATE_REQUESTED) {
                actual.onError(error);
            }
        }

    }

    private static final class QueuePoolMono<T> extends Mono<T> {

        QueuePool<T> parent;

        QueuePoolMono(QueuePool<T> pool) {
            this.parent = pool;
        }

        @Override
        public void subscribe(CoreSubscriber<? super T> actual) {
            Objects.requireNonNull(actual, "subscribe");

            PoolInner<T> p = new PoolInner<>(actual, parent);
            actual.onSubscribe(p);
        }
    }
}
