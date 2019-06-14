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
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
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

    volatile     int                                     pendingCount;
    static final AtomicIntegerFieldUpdater<AbstractPool> PENDING_COUNT = AtomicIntegerFieldUpdater.newUpdater(AbstractPool.class, "pendingCount");

    AbstractPool(PoolConfig<POOLABLE> poolConfig, Logger logger) {
        this.poolConfig = poolConfig;
        this.logger = logger;
        this.metricsRecorder = poolConfig.metricsRecorder;
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
        return poolConfig.allocationStrategy.permitGranted();
    }

    @Override
    abstract public int idleSize();

    @Override
    public int acquiredSize() {
        return allocatedSize() - idleSize();
    }

    @Override
    public int getMaxAllocatedSize() {
        return poolConfig.allocationStrategy.permitMaximum();
    }

    @Override
    public int getMaxPendingAcquireSize() {
        return poolConfig.maxPending < 0 ? Integer.MAX_VALUE : poolConfig.maxPending;
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
        poolConfig.allocationStrategy.returnPermits(1);
        long start = metricsRecorder.now();
        metricsRecorder.recordLifetimeDuration(ref.lifeTime());
        Function<POOLABLE, ? extends Publisher<Void>> factory = poolConfig.destroyHandler;
        if (factory == PoolBuilder.NOOP_HANDLER) {
            return Mono.fromRunnable(() -> {
                defaultDestroy(poolable);
                metricsRecorder.recordDestroyLatency(metricsRecorder.measureTime(start));
            });
        }
        else {
            return Mono.from(factory.apply(poolable))
                       .doFinally(fin -> metricsRecorder.recordDestroyLatency(metricsRecorder.measureTime(start)));
        }
    }

    /**
     * An abstract base for most common statistics operator of {@link PooledRef}.
     *
     * @author Simon Baslé
     */
    abstract static class AbstractPooledRef<T> implements PooledRef<T>, PooledRefMetadata {

        final long            creationTimestamp;
        final PoolMetricsRecorder metricsRecorder;
        final T poolable;

        volatile int acquireCount;
        static final AtomicIntegerFieldUpdater<AbstractPooledRef> ACQUIRE = AtomicIntegerFieldUpdater.newUpdater(AbstractPooledRef.class, "acquireCount");

        //might be peeked at by multiple threads, in which case a value of -1 indicates it is currently held/acquired
        volatile long timeSinceRelease;
        static final AtomicLongFieldUpdater<AbstractPooledRef> TIME_SINCE_RELEASE = AtomicLongFieldUpdater.newUpdater(AbstractPooledRef.class, "timeSinceRelease");

        AbstractPooledRef(T poolable, PoolMetricsRecorder metricsRecorder) {
            this.poolable = poolable;
            this.metricsRecorder = metricsRecorder;
            this.creationTimestamp = metricsRecorder.now();
            this.timeSinceRelease = -2L;
        }

        @Override
        public T poolable() {
            return poolable;
        }

        @Override
        public PooledRefMetadata metadata() {
            return this;
        }

        /**
         * Atomically increment the {@link #acquireCount()} of this slot, returning the new value.
         *
         * @return the incremented {@link #acquireCount()}
         */
        int markAcquired() {
            int acq = ACQUIRE.incrementAndGet(this);
            long tsr = TIME_SINCE_RELEASE.getAndSet(this, -1);
            if (tsr > 0) {
                metricsRecorder.recordIdleTime(metricsRecorder.measureTime(tsr));
            }
            else if (tsr < -1L) { //allocated, never acquired
                metricsRecorder.recordIdleTime(metricsRecorder.measureTime(creationTimestamp));
            }
            return acq;
        }

        void markReleased() {
            this.timeSinceRelease = metricsRecorder.now();
        }

        @Override
        public int acquireCount() {
            return ACQUIRE.get(this);
        }

        @Override
        public long lifeTime() {
            return metricsRecorder.measureTime(creationTimestamp);
        }

        @Override
        public long idleTime() {
            long tsr = this.timeSinceRelease;
            if (tsr == -1L) { //-1 is when it's been marked as acquired
                return 0L;
            }
            if (tsr < 0L) tsr = creationTimestamp; //any negative date other than -1 is considered "never yet released"
            return metricsRecorder.measureTime(tsr);
        }

        /**
         * Implementors MUST have the Mono call {@link #markReleased()} upon subscription.
         * <p>
         * {@inheritDoc}
         */
        @Override
        public abstract Mono<Void> release();

        /**
         * Implementors MUST have the Mono call {@link #markReleased()} upon subscription.
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
                boolean noPermits = pool.poolConfig.allocationStrategy.estimatePermitCount() == 0;

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
