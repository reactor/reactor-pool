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
package reactor.pool;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.Function;
import java.util.function.Predicate;

import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Scannable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.core.scheduler.Scheduler;
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
abstract class AbstractPool<POOLABLE> implements Pool<POOLABLE> {

    //A pool should be rare enough that having instance loggers should be ok
    //This helps with testability of some methods that for now mainly log
    final Logger logger;

    final DefaultPoolConfig<POOLABLE> poolConfig;

    final PoolMetricsRecorder metricsRecorder;


    AbstractPool(DefaultPoolConfig<POOLABLE> poolConfig, Logger logger) {
        this.poolConfig = poolConfig;
        this.logger = logger;
        this.metricsRecorder = poolConfig.metricsRecorder;
    }

    abstract boolean elementOffer(POOLABLE element);
    abstract int idleSize();

    abstract void doAcquire(Borrower<POOLABLE> borrower);

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
    Mono<Void> destroyPoolable(PooledRef<POOLABLE> ref) {
        POOLABLE poolable = ref.poolable();
        poolConfig.allocationStrategy.returnPermits(1);
        long start = metricsRecorder.now();
        metricsRecorder.recordLifetimeDuration(ref.lifeTime());
        Function<POOLABLE, Mono<Void>> factory = poolConfig.destroyHandler;
        if (factory == PoolBuilder.NOOP_HANDLER) {
            return Mono.fromRunnable(() -> {
                defaultDestroy(poolable);
                metricsRecorder.recordDestroyLatency(metricsRecorder.measureTime(start));
            });
        }
        else {
            return factory.apply(poolable)
                    .doFinally(fin -> metricsRecorder.recordDestroyLatency(metricsRecorder.measureTime(start)));
        }
    }

    /**
     * An abstract base for most common statistics operator of {@link PooledRef}.
     *
     * @author Simon Baslé
     */
    abstract static class AbstractPooledRef<T> implements PooledRef<T> {

        final long            creationTimestamp;
        final PoolMetricsRecorder metricsRecorder;

        volatile T poolable;

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
    static final class Borrower<POOLABLE> extends AtomicBoolean implements Scannable, Subscription  {

        final CoreSubscriber<? super AbstractPooledRef<POOLABLE>> actual;
        final AbstractPool<POOLABLE> pool;

        Borrower(CoreSubscriber<? super AbstractPooledRef<POOLABLE>> actual, AbstractPool<POOLABLE> pool) {
            this.actual = actual;
            this.pool = pool;
        }

        @Override
        public void request(long n) {
            if (Operators.validate(n)) {
                pool.doAcquire(this);
            }
        }

        @Override
        public void cancel() {
            set(true);
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
            if (!get()) {
                actual.onError(error);
            }
        }

        @Override
        public String toString() {
            return get() ? "Borrower(cancelled)" : "Borrower";
        }
    }

    /**
     * A inner representation of a {@link AbstractPool} configuration.
     *
     * @author Simon Baslé
     */
    static class DefaultPoolConfig<POOLABLE> {

        /**
         * The asynchronous factory that produces new resources, represented as a {@link Mono}.
         */
        final Mono<POOLABLE>                 allocator;
        //TODO to be removed
        /**
         * The minimum number of objects a {@link Pool} should create at initialization.
         */
        final int                            initialSize;
        /**
         * {@link AllocationStrategy} defines a strategy / limit for the number of pooled object to allocate.
         */
        final AllocationStrategy             allocationStrategy;
        /**
         * When a resource is {@link PooledRef#release() released}, defines a mechanism of resetting any lingering state of
         * the resource in order for it to become usable again. The {@link #evictionPredicate} is applied AFTER this reset.
         * <p>
         * For example, a buffer could have a readerIndex and writerIndex that need to be flipped back to zero.
         */
        final Function<POOLABLE, Mono<Void>> releaseHandler;
        /**
         * Defines a mechanism of resource destruction, cleaning up state and OS resources it could maintain (eg. off-heap
         * objects, file handles, socket connections, etc...).
         * <p>
         * For example, a database connection could need to cleanly sever the connection link by sending a message to the database.
         */
        final Function<POOLABLE, Mono<Void>> destroyHandler;
        /**
         * A {@link Predicate} that checks if a resource should be disposed ({@code true}) or is still in a valid state
         * for recycling. This is primarily applied when a resource is released, to check whether or not it can immediately
         * be recycled, but could also be applied during an acquire attempt (detecting eg. idle resources) or by a background
         * reaping process.
         */
        final Predicate<PooledRef<POOLABLE>> evictionPredicate;
        /**
         * The {@link Scheduler} on which the {@link Pool} should publish resources, independently of which thread called
         * {@link Pool#acquire()} or {@link PooledRef#release()} or on which thread the {@link #allocator} produced new
         * resources.
         * <p>
         * Use {@link Schedulers#immediate()} if determinism is less important than staying on the same threads.
         */
        final Scheduler                      acquisitionScheduler;
        /**
         * The {@link PoolMetricsRecorder} to use to collect instrumentation data of the {@link Pool}
         * implementations.
         */
        final PoolMetricsRecorder            metricsRecorder;

        DefaultPoolConfig(Mono<POOLABLE> allocator,
                          int initialSize,
                          AllocationStrategy allocationStrategy,
                          Function<POOLABLE, Mono<Void>> releaseHandler,
                          Function<POOLABLE, Mono<Void>> destroyHandler,
                          Predicate<PooledRef<POOLABLE>> evictionPredicate,
                          Scheduler acquisitionScheduler,
                          PoolMetricsRecorder metricsRecorder) {
            this.allocator = allocator;
            this.initialSize = initialSize;
            this.allocationStrategy = allocationStrategy;
            this.releaseHandler = releaseHandler;
            this.destroyHandler = destroyHandler;
            this.evictionPredicate = evictionPredicate;
            this.acquisitionScheduler = acquisitionScheduler;
            this.metricsRecorder = metricsRecorder;
        }
    }
}
