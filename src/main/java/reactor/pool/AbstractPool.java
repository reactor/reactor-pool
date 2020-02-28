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

import java.io.Closeable;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.Scannable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.core.scheduler.Schedulers;
import reactor.util.Logger;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * An abstract base version of a {@link Pool}, mutualizing small amounts of code and allowing to build common
 * related classes like {@link AbstractPooledRef} or {@link Borrower}.
 *
 * @author Simon Baslé
 */
abstract class AbstractPool<POOLABLE> implements InstrumentedPool<POOLABLE>,
                                                 InstrumentedPool.PoolMetrics {

    //A pool should be rare enough that having instance loggers should be ok
    //This helps with testability of some methods that for now mainly log
    final Logger logger;

    final PoolConfig<POOLABLE> poolConfig;

    final PoolMetricsRecorder metricsRecorder;
    final Clock clock;

    volatile     int                                     pendingCount;
    @SuppressWarnings("rawtypes")
    static final AtomicIntegerFieldUpdater<AbstractPool> PENDING_COUNT = AtomicIntegerFieldUpdater.newUpdater(AbstractPool.class, "pendingCount");

    AbstractPool(PoolConfig<POOLABLE> poolConfig, Logger logger) {
        this.poolConfig = poolConfig;
        this.logger = logger;
        this.metricsRecorder = poolConfig.metricsRecorder();
        this.clock = poolConfig.clock();
    }

    // == pool introspection methods ==

    @Override
    public PoolMetrics metrics() {
        return this;
    }

    @Override
    public int pendingAcquireSize() {
        return PENDING_COUNT.get(this);
    }

    @Override
    public int allocatedSize() {
        return poolConfig.allocationStrategy().permitGranted();
    }

    @Override
    abstract public int idleSize();

    @Override
    public int acquiredSize() {
        return allocatedSize() - idleSize();
    }

    @Override
    public int getMaxAllocatedSize() {
        return poolConfig.allocationStrategy().permitMaximum();
    }

    @Override
    public int getMaxPendingAcquireSize() {
        return poolConfig.maxPending() < 0 ? Integer.MAX_VALUE : poolConfig.maxPending();
    }

    // == common methods to interact with idle/pending queues ==

    abstract boolean elementOffer(POOLABLE element);

    /**
     * Note to implementors: stop the {@link Borrower} countdown by calling
     * {@link Borrower#stopPendingCountdown()} as soon as it is known that a resource is
     * available or is in the process of being allocated.
     */
    abstract void doAcquire(Borrower<POOLABLE> borrower);
    abstract void cancelAcquire(Borrower<POOLABLE> borrower);

    private void defaultDestroy(@Nullable POOLABLE poolable) {
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

    /**
     * Apply the configured destroyHandler to get the destroy {@link Mono} AND return a permit to the {@link AllocationStrategy},
     * which assumes that the {@link Mono} will always be subscribed immediately.
     *
     * @param ref the {@link PooledRef} that is not part of the live set
     * @return the destroy {@link Mono}, which MUST be subscribed immediately
     */
    Mono<Void> destroyPoolable(AbstractPooledRef<POOLABLE> ref) {
        POOLABLE poolable = ref.poolable();
        poolConfig.allocationStrategy().returnPermits(1);
        long start = clock.millis();
        metricsRecorder.recordLifetimeDuration(ref.lifeTime());
        Function<POOLABLE, ? extends Publisher<Void>> factory = poolConfig.destroyHandler();
        if (factory == PoolBuilder.NOOP_HANDLER) {
            return Mono.fromRunnable(() -> {
                defaultDestroy(poolable);
                metricsRecorder.recordDestroyLatency(clock.millis() - start);
            });
        }
        else {
            return Mono.from(factory.apply(poolable))
                       .doFinally(fin -> metricsRecorder.recordDestroyLatency(clock.millis() - start));
        }
    }

    /**
     * An abstract base for most common statistics operator of {@link PooledRef}.
     *
     * @author Simon Baslé
     */
    abstract static class AbstractPooledRef<T> implements PooledRef<T>, PooledRefMetadata {

        final long                creationTimestamp;
        final PoolMetricsRecorder metricsRecorder;
        final Clock               clock;
        final T                   poolable;
        final int                 acquireCount;

        long timeSinceRelease;

        volatile int state;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<AbstractPooledRef> STATE = AtomicIntegerFieldUpdater.newUpdater(AbstractPooledRef.class, "state");

        /**
         * Use this constructor the first time a resource is created and wrapped in a {@link PooledRef}.
         * @param poolable the newly created poolable
         * @param metricsRecorder the recorder to use for metrics
         * @param clock the {@link Clock} to use for timestamps
         */
        AbstractPooledRef(T poolable, PoolMetricsRecorder metricsRecorder, Clock clock) {
            this.poolable = poolable;
            this.metricsRecorder = metricsRecorder;
            this.clock = clock;
            this.creationTimestamp = clock.millis();
            this.acquireCount = 0;
            this.timeSinceRelease = -2L;
            this.state = STATE_IDLE;
        }

        /**
         * Use this constructor when a resource is passed to another borrower.
         */
        AbstractPooledRef(AbstractPooledRef<T> oldRef) {
            this.poolable = oldRef.poolable;
            this.metricsRecorder = oldRef.metricsRecorder;
            this.clock = oldRef.clock;
            this.creationTimestamp = oldRef.creationTimestamp;
            this.acquireCount = oldRef.acquireCount(); //important to use method since the count variable is final
            this.timeSinceRelease = oldRef.timeSinceRelease; //important to carry over the markReleased for metrics
            this.state = STATE_IDLE; //we're dealing with a new slot that was created when the previous one was released
        }

        @Override
        public T poolable() {
            return poolable;
        }

        @Override
        public PooledRefMetadata metadata() {
            return this;
        }

        void markAcquired() {
            if (STATE.compareAndSet(this, STATE_IDLE, STATE_ACQUIRED)) {
                long tsr = timeSinceRelease;
                if (tsr > 0) {
                    metricsRecorder.recordIdleTime(clock.millis() - tsr);
                }
                else {
                    metricsRecorder.recordIdleTime(clock.millis() - creationTimestamp);
                }
            }
        }

        boolean markReleased() {
            for(;;) {
                int s = state;
                if (s == STATE_RELEASED) {
                    return false;
                }
                else if (STATE.compareAndSet(this, s, STATE_RELEASED)) {
                    this.timeSinceRelease = clock.millis();
                    return true;
                }
            }
        }

        boolean markInvalidate() {
            for(;;) {
                int s = state;
                if (s == STATE_RELEASED) {
                    return false;
                }
                else if (STATE.compareAndSet(this, s, STATE_RELEASED)) {
                    return true;
                }
            }
        }

        @Override
        public int acquireCount() {
            if (STATE.get(this) == STATE_IDLE) {
                return this.acquireCount;
            }
            return this.acquireCount + 1;
        }

        @Override
        public long lifeTime() {
            return clock.millis() - creationTimestamp;
        }

        @Override
        public long idleTime() {
            if (STATE.get(this) == STATE_ACQUIRED) {
                return 0L;
            }
            long tsr = this.timeSinceRelease;
            if (tsr < 0L) tsr = creationTimestamp; //any negative date other than -1 is considered "never yet released"
            return clock.millis() - tsr;
        }

        /**
         * Implementors MUST have the Mono call {@link #markReleased()} upon subscription.
         * <p>
         * {@inheritDoc}
         */
        @Override
        public abstract Mono<Void> release();

        /**
         * Implementors MUST have the Mono call {@link #markInvalidate()} upon subscription.
         * <p>
         * {@inheritDoc}
         */
        @Override
        public abstract Mono<Void> invalidate();

        @Override
        public String toString() {
            return "PooledRef{" +
                    "poolable=" + poolable +
                    ", lifeTime=" + lifeTime() + "ms" +
                    ", idleTime=" + idleTime() + "ms" +
                    ", acquireCount=" + acquireCount +
                    '}';
        }

        static final int STATE_IDLE = 0;
        static final int STATE_ACQUIRED = 1;
        static final int STATE_RELEASED = 2;
    }

    /**
     * Common inner {@link Subscription} to be used to deliver poolable elements wrapped in {@link AbstractPooledRef} from
     * an {@link AbstractPool}.
     *
     * @author Simon Baslé
     */
    static final class Borrower<POOLABLE> extends AtomicBoolean implements Scannable, Subscription, Runnable  {

        static final Disposable TIMEOUT_DISPOSED = Disposables.disposed();

        final CoreSubscriber<? super AbstractPooledRef<POOLABLE>> actual;
        final AbstractPool<POOLABLE> pool;
        final Duration acquireTimeout;

        Disposable timeoutTask;

        Borrower(CoreSubscriber<? super AbstractPooledRef<POOLABLE>> actual,
                AbstractPool<POOLABLE> pool,
                Duration acquireTimeout) {
            this.actual = actual;
            this.pool = pool;
            this.acquireTimeout = acquireTimeout;
            this.timeoutTask = TIMEOUT_DISPOSED;
        }

        @Override
        public void run() {
            if (Borrower.this.compareAndSet(false, true)) {
                pool.cancelAcquire(Borrower.this);
                actual.onError(new PoolAcquireTimeoutException(acquireTimeout));
            }
        }

        @Override
        public void request(long n) {
            if (Operators.validate(n)) {
                //start the countdown

                boolean noIdle = pool.idleSize() == 0;
                boolean noPermits = pool.poolConfig.allocationStrategy().estimatePermitCount() == 0;

                if (!acquireTimeout.isZero() && noIdle && noPermits) {
                    timeoutTask = Schedulers.parallel().schedule(this, acquireTimeout.toMillis(), TimeUnit.MILLISECONDS);
                }
                //doAcquire should interrupt the countdown if there is either an available
                //resource or the pool can allocate one
                pool.doAcquire(this);
            }
        }

        /**
         * Stop the countdown started when calling {@link AbstractPool#doAcquire(Borrower)}.
         */
        void stopPendingCountdown() {
            timeoutTask.dispose();
        }

        @Override
        public void cancel() {
            set(true);
            pool.cancelAcquire(this);
            stopPendingCountdown();
        }

        @Override
        @Nullable
        @SuppressWarnings("rawtypes")
        public Object scanUnsafe(Attr key) {
            if (key == Attr.CANCELLED) return get();
            if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return 1;
            if (key == Attr.ACTUAL) return actual;

            return null;
        }

        void deliver(AbstractPooledRef<POOLABLE> poolSlot) {
            stopPendingCountdown();
            if (get()) {
                //CANCELLED
                poolSlot.release().subscribe(aVoid -> {}, e -> Operators.onErrorDropped(e, Context.empty())); //actual mustn't receive onError
            }
            else {
                poolSlot.markAcquired();
                actual.onNext(poolSlot);
                actual.onComplete();
            }
        }

        void fail(Throwable error) {
            stopPendingCountdown();
            if (!get()) {
                actual.onError(error);
            }
        }

        @Override
        public String toString() {
            return get() ? "Borrower(cancelled)" : "Borrower";
        }
    }

}
