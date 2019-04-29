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

import java.util.function.BiPredicate;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.Logger;

/**
 * An abstract base version of a {@link Pool} that holds configuration properties.
 *
 * @author Simon Baslé
 * @author Stephane Maldini
 */
abstract class ConfigurablePool<POOLABLE> implements InstrumentedPool<POOLABLE>,
                                                     InstrumentedPool.PoolMetrics {

    final PoolConfiguration<POOLABLE> poolConfig;

    ConfigurablePool(PoolConfiguration<POOLABLE> poolConfig) {
        this.poolConfig = poolConfig;
    }

    // == pool introspection methods ==

    @Override
    public PoolMetrics metrics() {
        return this;
    }

    @Override
    public int allocatedSize() {
        return poolConfig.allocationStrategy.permitGranted();
    }

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

    /**
     * A inner representation of a {@link ConfigurablePool} configuration.
     *
     * @author Simon Baslé
     */
    static class PoolConfiguration<POOLABLE> {

        /**
         * The asynchronous factory that produces new resources, represented as a {@link Mono}.
         */
        final Mono<POOLABLE>                                allocator;
        //TODO to be removed
        /**
         * The minimum number of objects a {@link Pool} should create at initialization.
         */
        final int                                           initialSize;
        /**
         * {@link AllocationStrategy} defines a strategy / limit for the number of pooled object to allocate.
         */
        final AllocationStrategy                            allocationStrategy;
        /**
         * The maximum number of pending borrowers to enqueue before failing fast. 0 will immediately fail any acquire
         * when no idle resource is available and the pool cannot grow. Use a negative number to deactivate.
         */
        final int                                           maxPending;
        /**
         * When a resource is {@link PooledRef#release() released}, defines a mechanism of resetting any lingering state of
         * the resource in order for it to become usable again. The {@link #evictionPredicate} is applied AFTER this reset.
         * <p>
         * For example, a buffer could have a readerIndex and writerIndex that need to be flipped back to zero.
         */
        final Function<POOLABLE, ? extends Publisher<Void>> releaseHandler;
        /**
         * Defines a mechanism of resource destruction, cleaning up state and OS resources it could maintain (eg. off-heap
         * objects, file handles, socket connections, etc...).
         * <p>
         * For example, a database connection could need to cleanly sever the connection link by sending a message to the database.
         */
        final Function<POOLABLE, ? extends Publisher<Void>> destroyHandler;
        /**
         * A {@link BiPredicate} that checks if a resource should be destroyed ({@code true}) or is still in a valid state
         * for recycling. This is primarily applied when a resource is released, to check whether or not it can immediately
         * be recycled, but could also be applied during an acquire attempt (detecting eg. idle resources) or by a background
         * reaping process. Both the resource and some {@link PooledRefMetadata metrics} about the resource's life within the pool are provided.
         */
        final BiPredicate<POOLABLE, PooledRefMetadata>      evictionPredicate;
        /**
         * The {@link Scheduler} on which the {@link Pool} should publish resources, independently of which thread called
         * {@link Pool#acquire()} or {@link PooledRef#release()} or on which thread the {@link #allocator} produced new
         * resources.
         * <p>
         * Use {@link Schedulers#immediate()} if determinism is less important than staying on the same threads.
         */
        final Scheduler                                     acquisitionScheduler;
        /**
         * The {@link PoolMetricsRecorder} to use to collect instrumentation data of the {@link Pool}
         * implementations.
         */
        final PoolMetricsRecorder                           metricsRecorder;

        PoolConfiguration(Mono<POOLABLE> allocator,
                          int initialSize,
                          AllocationStrategy allocationStrategy,
                          int maxPending,
                          Function<POOLABLE, ? extends Publisher<Void>> releaseHandler,
                          Function<POOLABLE, ? extends Publisher<Void>> destroyHandler,
                          BiPredicate<POOLABLE, PooledRefMetadata> evictionPredicate,
                          Scheduler acquisitionScheduler,
                          PoolMetricsRecorder metricsRecorder) {
            this.allocator = allocator;
            this.initialSize = initialSize;
            this.allocationStrategy = allocationStrategy;
            this.maxPending = maxPending;
            this.releaseHandler = releaseHandler;
            this.destroyHandler = destroyHandler;
            this.evictionPredicate = evictionPredicate;
            this.acquisitionScheduler = acquisitionScheduler;
            this.metricsRecorder = metricsRecorder;
        }
    }
}
