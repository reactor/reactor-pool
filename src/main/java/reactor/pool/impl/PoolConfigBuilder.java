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
package reactor.pool.impl;

import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.pool.AllocationStrategy;
import reactor.pool.Pool;
import reactor.pool.PoolConfig;
import reactor.pool.PooledRef;
import reactor.pool.metrics.PoolMetricsRecorder;
import reactor.pool.util.AllocationStrategies;
import reactor.pool.util.EvictionPredicates;
import reactor.util.annotation.Nullable;

import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A builder for {@link PoolConfig}. Start with mandatory {@link #allocateWith(Mono)}.
 *
 * @author Simon Baslé
 */
public class PoolConfigBuilder<T> {

    //TODO tests

    /**
     * Start building a {@link PoolConfig} by describing how new objects are to be asynchronously allocated.
     * Note that the {@link Mono} {@code allocator} should NEVER block its thread (thus adapting from blocking code,
     * eg. a constructor, via {@link Mono#fromCallable(Callable)} should be augmented with {@link Mono#subscribeOn(Scheduler)}).
     *
     * @param allocator the asynchronous creator of poolable resources.
     * @param <T> the type of resource created and recycled by the {@link Pool}
     * @return a builder of {@link PoolConfig}
     */
    public static <T> PoolConfigBuilder<T> allocateWith(Mono<T> allocator) {
        return new PoolConfigBuilder<>(allocator);
    }

    private final Mono<T> allocator;

    private int initialSize = 0;

    @Nullable
    private AllocationStrategy allocationStrategy;
    @Nullable
    private Function<T, Mono<Void>> resetFactory;
    @Nullable
    private Function<T, Mono<Void>> destroyFactory;
    @Nullable
    private Predicate<PooledRef<T>> evictionPredicate;
    @Nullable
    private Scheduler deliveryScheduler;
    @Nullable
    private PoolMetricsRecorder metricsRecorder;

    PoolConfigBuilder(Mono<T> allocator) {
        this.allocator = allocator;
    }

    /**
     * How many resources the {@link Pool} should allocate upon creation.
     * This parameter MAY be ignored by some implementations (although they should state so in their documentation).
     * <p>
     * Defaults to {@code 0}.
     *
     * @param n the initial size of the {@link Pool}.
     * @return this {@link PoolConfig} builder
     */
    public PoolConfigBuilder<T> initialSizeOf(int n) {
        this.initialSize = n;
        return this;
    }

    /**
     * Limits in how many resources can be allocated and managed by the {@link Pool} are driven by the
     * provided {@link AllocationStrategy}.
     * <p>
     * Defaults to an unbounded creation of resources, although it is not a recommended one.
     * See {@link AllocationStrategies} for readily available strategies based on counters.
     *
     * @param allocationStrategy the {@link AllocationStrategy} to use
     * @return this {@link PoolConfig} builder
     */
    public PoolConfigBuilder<T> witAllocationLimit(AllocationStrategy allocationStrategy) {
        this.allocationStrategy = allocationStrategy;
        return this;
    }

    /**
     * Provide a {@link Function factory} that will derive a reset {@link Mono} whenever a resource is released.
     * The reset procedure is applied asynchronously before vetting the object through {@link #evictionPredicate}.
     * If the reset Mono couldn't put the resource back in a usable state, it will be {@link #destroyResourcesWith(Function) destroyed}.
     * <p>
     * Defaults to not resetting anything.
     *
     * @param resetFactory the {@link Function} supplying the state-resetting {@link Mono}
     * @return this {@link PoolConfig} builder
     */
    public PoolConfigBuilder<T> resetResourcesWith(Function<T, Mono<Void>> resetFactory) {
        this.resetFactory = resetFactory;
        return this;
    }

    /**
     * Provide a {@link Function factory} that will derive a destroy {@link Mono} whenever a resource isn't fit for
     * usage anymore (either through eviction, manual invalidation, or because something went wrong with it).
     * The destroy procedure is applied asynchronously and errors are swallowed.
     * <p>
     * Defaults to recognizing {@link Disposable} and {@link java.io.Closeable} elements and disposing them.
     *
     * @param destroyFactory the {@link Function} supplying the state-resetting {@link Mono}
     * @return this {@link PoolConfig} builder
     */
    public PoolConfigBuilder<T> destroyResourcesWith(Function<T, Mono<Void>> destroyFactory) {
        this.destroyFactory = destroyFactory;
        return this;
    }

    /**
     * Provide an eviction {@link Predicate} that allows to decide if a resource is fit for being placed in the {@link Pool}.
     * This can happen whenever a resource is {@link PooledRef#release() released} back to the {@link Pool} (after
     * it has been {@link #resetResourcesWith(Function) reset}), but also when being {@link Pool#acquire() acquired}
     * from the pool (triggering a second pass if the object is found to be unfit, eg. it has been idle for too long).
     * Finally, some pool implementations MAY implement a reaper thread mechanism that detect idle resources through
     * this predicate and destroy them.
     * <p>
     * Defaults to never evicting. See {@link EvictionPredicates} for pre-build eviction predicates.
     *
     * @param evictionPredicate a {@link Predicate} that returns {@code true} if the resource is unfit for the pool and should be destroyed
     * @return this {@link PoolConfig} builder
     */
    public PoolConfigBuilder<T> evictionPredicate(Predicate<PooledRef<T>> evictionPredicate) {
        this.evictionPredicate = evictionPredicate;
        return this;
    }

    /**
     * Provide a {@link Scheduler} that can optionally be used by a {@link Pool} to deliver its resources in a more
     * deterministic (albeit potentially less efficient) way, thread-wise. Other implementations MAY completely ignore
     * this parameter.
     * <p>
     * Defaults to {@link Schedulers#immediate()}.
     *
     * @param deliveryScheduler the {@link Scheduler} on which to deliver acquired resources
     * @return this {@link PoolConfig} builder
     */
    public PoolConfigBuilder<T> deliveryScheduler(Scheduler deliveryScheduler) {
        this.deliveryScheduler = deliveryScheduler;
        return this;
    }

    /**
     * Set up the optional {@link PoolMetricsRecorder} for {@link Pool} to use for instrumentation purposes.
     *
     * @param recorder the {@link PoolMetricsRecorder}
     * @return this {@link PoolConfig} builder
     */
    public PoolConfigBuilder<T> recordMetricsWith(PoolMetricsRecorder recorder) {
        this.metricsRecorder = recorder;
        return this;
    }

    /**
     * Build the {@link PoolConfig}.
     *
     * @return the {@link PoolConfig}
     */
    public PoolConfig<T> buildConfig() {
        return new AbstractPool.DefaultPoolConfig<>(allocator, initialSize, allocationStrategy, resetFactory, destroyFactory,
                evictionPredicate, deliveryScheduler, metricsRecorder);
    }

}
