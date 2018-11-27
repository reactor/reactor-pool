package reactor.util.pool.impl;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
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

    private static final Logger LOGGER = Loggers.getLogger(QueuePool.class);

    final PoolConfig<POOLABLE> poolConfig;
    final Queue<POOLABLE> elements;

    volatile Queue<PoolInner<POOLABLE>> pending = Queues.<PoolInner<POOLABLE>>unboundedMultiproducer().get();
    volatile int borrowed;
    volatile int wip;

    private static final AtomicIntegerFieldUpdater<QueuePool> BORROWED = AtomicIntegerFieldUpdater.newUpdater(QueuePool.class, "borrowed");
    private static final AtomicReferenceFieldUpdater<QueuePool, Queue> PENDING = AtomicReferenceFieldUpdater.newUpdater(QueuePool.class, Queue.class, "pending");
    private static final AtomicIntegerFieldUpdater<QueuePool> WIP = AtomicIntegerFieldUpdater.newUpdater(QueuePool.class, "wip");

    private final Queue<PoolInner<POOLABLE>> TERMINATED = Queues.<PoolInner<POOLABLE>>empty().get();

    public QueuePool(PoolConfig<POOLABLE> poolConfig) {
        this.poolConfig = poolConfig;
        this.elements = Queues.<POOLABLE>get(poolConfig.maxSize()).get();

        for (int i = 0; i < poolConfig.minSize(); i++) {
            elements.offer(poolConfig.allocator().block());
        }
    }

    void doBorrow(PoolInner<POOLABLE> s) {
        if (pending != TERMINATED) {
            pending.add(s);
            drain();
        }
        //TODO handle mono subscription when pool is terminated
    }

    void doOffer(POOLABLE poolable) {
        elements.offer(poolable);
        drain();
    }

    @Override
    public Mono<Void> release(final POOLABLE poolable) {
        BORROWED.decrementAndGet(this);
        return poolConfig.cleaner()
                .apply(poolable)
                .doOnSuccess(aVoid -> this.cleaned(poolable))
                .doOnError(e -> discarded(poolable, e));
    }

    @Override
    public void releaseSync(final POOLABLE poolable) {
        release(poolable).block();
    }

    void discarded(POOLABLE poolable, Throwable cause) {
        if (poolable instanceof Disposable) {
            ((Disposable) poolable).dispose();
        }
        else if (poolable instanceof Closeable) {
            try {
                ((Closeable) poolable).close();
            } catch (IOException e) {
                e.printStackTrace(); //TODO logs
            }
        }
        //TODO logs
        //TODO anything else to throw away poolable?
    }

    void cleaned(POOLABLE poolable) {
        if (pending != TERMINATED) {
            if (poolConfig.validator().test(poolable)) {
                doOffer(poolable);
            }
            else {
                //the element was dirty, will replace with a new one
                drain();
            }
        }
        //TODO handle release when pool is terminated
    }

    @Override
    public Mono<POOLABLE> borrow() {
        return new QueuePoolMono<>(this); //the mono is unknown to the pool until both subscribed and requested
    }

    private void onAllocatorError(Throwable cause) {
        if (LOGGER.isErrorEnabled()) {
            LOGGER.error("Failure during allocation of a new Poolable", cause);
        }
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
            int elementCount = elements.size();
            int pendingCount = pending.size();
            int borrowedCount = borrowed;

            if (elementCount == 0 && pendingCount > 0 && borrowedCount < maxElements) {
                poolConfig.allocator().subscribe(this::doOffer,
                        this::onAllocatorError);
            }
            else {
                POOLABLE poolable = elements.poll();
                if (poolable != null) {
                    //there are objects ready and unclaimed in the pool
                    PoolInner<POOLABLE> inner = pending.poll();
                    if (inner != null) {
                        //there is a party currently borrowing
                        poolConfig.deliveryScheduler().schedule(() -> inner.deliver(poolable));
                        BORROWED.incrementAndGet(this);
                    }
                    else {
                        //no party borrowing, return the element to the pool
                        elements.offer(poolable);
                    }
                }
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
                parent.doBorrow(this);
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
            if (LOGGER.isTraceEnabled()) {
                LOGGER.info("deliver(" + poolable + ") in state " + state);
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
