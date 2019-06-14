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
import java.util.concurrent.Callable;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;

import org.reactivestreams.Publisher;

import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * A builder for {@link Pool} implementations, which tuning methods map
 * to a {@link PoolConfig} subclass, {@code CONF}.
 *
 * @param <T> the type of elements in the produced {@link Pool}
 * @param <CONF> the {@link PoolConfig} flavor this builder will provide to the create {@link Pool}
 *
 * @author Simon Baslé
 */
public class PoolBuilder<T, CONF extends PoolConfig<T>> {

    /**
     * Start building a {@link Pool} by describing how new objects are to be asynchronously allocated.
     * Note that the {@link Publisher} {@code allocator} is subscribed to each time a new resource is
     * needed and will be cancelled past the first received element (unless it is already a {@link Mono}).
     * <p>
     * Adapting from blocking code is only acceptable if ensuring the work is offset on another {@link Scheduler}
     * (eg. a constructor materialized via {@link Mono#fromCallable(Callable)} should be augmented
     * with {@link Mono#subscribeOn(Scheduler)}).
     *
     * @param allocator the asynchronous creator of poolable resources, subscribed each time a new
     * resource needs to be created.
     * @param <T> the type of resource created and recycled by the {@link Pool}
     * @return a builder of {@link Pool}
     */
    public static <T> PoolBuilder<T, PoolConfig<T>> from(Publisher<? extends T> allocator) {
        Mono<T> source = Mono.from(allocator);
        return new PoolBuilder<>(source, Function.identity());
    }

    final Mono<T> allocator;
    final Function<PoolConfig<T>, CONF>    configModifier;
    int                                    initialSize          = 0;
    int                                    maxPending           = -1;
    AllocationStrategy                     allocationStrategy   = null;
    Function<T, ? extends Publisher<Void>> releaseHandler       = noopHandler();
    Function<T, ? extends Publisher<Void>> destroyHandler       = noopHandler();
    BiPredicate<T, PooledRefMetadata>      evictionPredicate    = neverPredicate();
    Scheduler                              acquisitionScheduler = Schedulers.immediate();
    PoolMetricsRecorder                    metricsRecorder      = NoOpPoolMetricsRecorder.INSTANCE;

    PoolBuilder(Mono<T> allocator, Function<PoolConfig<T>, CONF> configModifier) {
        this.allocator = allocator;
        this.configModifier = configModifier;
    }

    PoolBuilder(PoolBuilder<T, ?> source, Function<PoolConfig<T>, CONF> configModifier) {
        this.configModifier = configModifier;

        this.allocator = source.allocator;
        this.initialSize = source.initialSize;
        this.maxPending = source.maxPending;
        this.allocationStrategy = source.allocationStrategy;
        this.releaseHandler = source.releaseHandler;
        this.destroyHandler = source.destroyHandler;
        this.evictionPredicate = source.evictionPredicate;
        this.acquisitionScheduler = source.acquisitionScheduler;
        this.metricsRecorder = source.metricsRecorder;
    }

    /**
     * Provide a {@link Scheduler} that can optionally be used by a {@link Pool} to deliver its resources in a more
     * deterministic (albeit potentially less efficient) way, thread-wise. Other implementations MAY completely ignore
     * this parameter.
     * <p>
     * Defaults to {@link Schedulers#immediate()}.
     *
     * @param acquisitionScheduler the {@link Scheduler} on which to deliver acquired resources
     * @return this {@link Pool} builder
     */
    public PoolBuilder<T, CONF> acquisitionScheduler(Scheduler acquisitionScheduler) {
        this.acquisitionScheduler = Objects.requireNonNull(acquisitionScheduler, "acquisitionScheduler");
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
     * @return this {@link Pool} builder
     * @see #sizeMax(int)
     * @see #sizeUnbounded()
     */
    public PoolBuilder<T, CONF> allocationStrategy(AllocationStrategy allocationStrategy) {
        this.allocationStrategy = Objects.requireNonNull(allocationStrategy, "allocationStrategy");
        return this;
    }


    /**
     * Provide a {@link Function handler} that will derive a destroy {@link Publisher} whenever a resource isn't fit for
     * usage anymore (either through eviction, manual invalidation, or because something went wrong with it).
     * The destroy procedure is applied asynchronously and errors are swallowed.
     * <p>
     * Defaults to recognizing {@link Disposable} and {@link java.io.Closeable} elements and disposing them.
     *
     * @param destroyHandler the {@link Function} supplying the state-resetting {@link Publisher}
     * @return this {@link Pool} builder
     */
    public PoolBuilder<T, CONF> destroyHandler(Function<T, ? extends Publisher<Void>> destroyHandler) {
        this.destroyHandler = Objects.requireNonNull(destroyHandler, "destroyHandler");
        return this;
    }

    /**
     * Use an {@link #evictionPredicate(BiPredicate) eviction predicate} that causes eviction (ie returns {@code true})
     * of resources that have been idle (ie released and available in the {@link Pool}) for more than the {@code ttl}
     * {@link Duration} (inclusive).
     * Such a predicate could be used to evict too idle objects when next encountered by an {@link Pool#acquire()}.
     *
     * @param maxIdleTime the {@link Duration} after which an object should not be passed to a borrower, but destroyed (resolution: ms)
     * @return this {@link Pool} builder
     * @see #evictionPredicate(BiPredicate)
     */
    public PoolBuilder<T, CONF> evictionIdle(Duration maxIdleTime) {
        return evictionPredicate(idlePredicate(maxIdleTime));
    }

    /**
     * Provide an eviction {@link BiPredicate} that allows to decide if a resource is fit for being placed in the {@link Pool}.
     * This can happen whenever a resource is {@link PooledRef#release() released} back to the {@link Pool} (after
     * it was processed by the {@link #releaseHandler(Function)}), but also when being {@link Pool#acquire() acquired}
     * from the pool (triggering a second pass if the object is found to be unfit, eg. it has been idle for too long).
     * Finally, some pool implementations MAY implement a reaper thread mechanism that detect idle resources through
     * this predicate and destroy them.
     * <p>
     * Defaults to never evicting (a {@link BiPredicate} that always returns false).
     *
     * @param evictionPredicate a {@link Predicate} that returns {@code true} if the resource is unfit for the pool and should
     * be destroyed, {@code false} if it should be put back into the pool.
     * @return this {@link Pool} builder
     * @see #evictionIdle(Duration)
     */
    public PoolBuilder<T, CONF> evictionPredicate(BiPredicate<T, PooledRefMetadata> evictionPredicate) {
        this.evictionPredicate = Objects.requireNonNull(evictionPredicate, "evictionPredicate");
        return this;
    }

    /**
     * How many resources the {@link Pool} should allocate upon creation.
     * This parameter MAY be ignored by some implementations (although they should state so in their documentation).
     * <p>
     * Defaults to {@code 0}.
     *
     * @param n the initial size of the {@link Pool}.
     * @return this {@link Pool} builder
     */
    public PoolBuilder<T, CONF> initialSize(int n) {
        if (n < 0) {
            throw new IllegalArgumentException("initialSize must be >= 0");
        }
        this.initialSize = n;
        return this;
    }

    /**
     * Set the maximum number of <i>subscribed</i> {@link Pool#acquire()} Monos that can
     * be in a pending state (ie they wait for a resource to be released, as no idle
     * resource was immediately available, and the pool add already allocated the maximum
     * permitted amount). Set to {@code 0} to immediately fail all such acquisition attempts.
     * Set to {@code -1} to deactivate (or prefer using the more explicit {@link #maxPendingAcquireUnbounded()}).
     * <p>
     * Default to -1.
     *
     * @param maxPending the maximum number of registered acquire monos to keep in a pending queue
     * @return a builder of {@link Pool} with a maximum pending queue size.
     */
    public PoolBuilder<T, CONF> maxPendingAcquire(int maxPending) {
        this.maxPending = maxPending;
        return this;
    }

    /**
     * Uncap the number of <i>subscribed</i> {@link Pool#acquire()} Monos that can be in a
     * pending state (ie they wait for a resource to be released, as no idle resource was
     * immediately available, and the pool add already allocated the maximum permitted amount).
     * <p>
     * This is the default.
     *
     * @return a builder of {@link Pool} with no maximum pending queue size.
     */
    public PoolBuilder<T, CONF> maxPendingAcquireUnbounded() {
        this.maxPending = -1;
        return this;
    }

    /**
     * Set up the optional {@link PoolMetricsRecorder} for {@link Pool} to use for instrumentation purposes.
     *
     * @param recorder the {@link PoolMetricsRecorder}
     * @return this {@link Pool} builder
     */
    public PoolBuilder<T, CONF> metricsRecorder(PoolMetricsRecorder recorder) {
        this.metricsRecorder = Objects.requireNonNull(recorder, "recorder");
        return this;
    }

    /**
     * Provide a {@link Function handler} that will derive a reset {@link Publisher} whenever a resource is released.
     * The reset procedure is applied asynchronously before vetting the object through {@link #evictionPredicate}.
     * If the reset Publisher couldn't put the resource back in a usable state, it will be {@link #destroyHandler(Function) destroyed}.
     * <p>
     * Defaults to not resetting anything.
     *
     * @param releaseHandler the {@link Function} supplying the state-resetting {@link Publisher}
     * @return this {@link Pool} builder
     */
    public PoolBuilder<T, CONF> releaseHandler(Function<T, ? extends Publisher<Void>> releaseHandler) {
        this.releaseHandler = Objects.requireNonNull(releaseHandler, "releaseHandler");
        return this;
    }

    /**
     * Let the {@link Pool} allocate at most {@code max} resources, rejecting further allocations until
     * some resources have been {@link PooledRef#release() released}.
     *
     * @param max the maximum number of live resources to keep in the pool
     * @return this {@link Pool} builder
     */
    public PoolBuilder<T, CONF> sizeMax(int max) {
        return allocationStrategy(new AllocationStrategies.SizeBasedAllocationStrategy(max));
    }

    /**
     * Let the {@link Pool} allocate new resources when no idle resource is available, without limit.
     * <p>
     * Note this is the default, if no previous call to {@link #allocationStrategy(AllocationStrategy)}
     * or {@link #sizeMax(int)} has been made on this {@link PoolBuilder}.
     *
     * @return this {@link Pool} builder
     */
    public PoolBuilder<T, CONF> sizeUnbounded() {
        return allocationStrategy(new AllocationStrategies.UnboundedAllocationStrategy());
    }

    /**
     * Add implementation-specific configuration, changing the type of {@link PoolConfig}
     * passed to the {@link Pool} factory in {@link #build(Function)}.
     *
     * @param configModifier {@link Function} to transform the type of {@link PoolConfig}
     * create by this builder for the benefit of the pool factory, allowing for custom
     * implementations with custom configurations
     * @param <CONF2> new type for the configuration
     * @return a new PoolBuilder that now produces a different type of {@link PoolConfig}
     */
    public <CONF2 extends PoolConfig<T>> PoolBuilder<T, CONF2> extraConfiguration(Function<? super CONF, CONF2> configModifier) {
        return new PoolBuilder<>(this, this.configModifier.andThen(configModifier));
    }

    /**
     * Build a LIFO flavor of {@link Pool}, that is to say a flavor where the last
     * {@link Pool#acquire()} {@link Mono Mono} that was pending is served first
     * whenever a resource becomes available.
     *
     * @return a builder of {@link Pool} with LIFO pending acquire ordering.
     */
    public Pool<T> lifo() {
        return build(SimpleLifoPool::new);
    }

    /**
     * Build the default flavor of {@link Pool}, which has FIFO semantics on pending
     * {@link Pool#acquire()} {@link Mono Mono}, serving the oldest pending acquire first
     * whenever a resource becomes available.
     *
     * @return a builder of {@link Pool} with FIFO pending acquire ordering.
     */
    public Pool<T> fifo() {
        return build(SimpleFifoPool::new);
    }

    /**
     * Build a custom flavor of {@link Pool}, given a Pool factory {@link Function} that
     * is provided with a {@link PoolConfig} copy of this builder's configuration.
     *
     * @param poolFactory the factory of pool implementation
     * @return the {@link Pool}
     */
    public <POOL extends Pool<T>> POOL build(Function<? super CONF, POOL> poolFactory) {
        CONF config = buildConfig();
        return poolFactory.apply(config);
    }

    //kept package-private for the benefit of tests
    CONF buildConfig() {
        PoolConfig<T> baseConfig = new PoolConfig<>(allocator,
                initialSize,
                allocationStrategy == null ?
                        new AllocationStrategies.UnboundedAllocationStrategy() :
                        allocationStrategy,
                maxPending,
                releaseHandler,
                destroyHandler,
                evictionPredicate,
                acquisitionScheduler,
                metricsRecorder);

        return this.configModifier.apply(baseConfig);
    }

    @SuppressWarnings("unchecked")
    static <T> Function<T, Mono<Void>> noopHandler() {
        return (Function<T, Mono<Void>>) NOOP_HANDLER;
    }

    @SuppressWarnings("unchecked")
    static <T> BiPredicate<T, PooledRefMetadata> neverPredicate() {
        return (BiPredicate<T, PooledRefMetadata>) NEVER_PREDICATE;
    }

    static <T> BiPredicate<T, PooledRefMetadata> idlePredicate(Duration maxIdleTime) {
        return (poolable, meta) -> meta.idleTime() >= maxIdleTime.toMillis();
    }

    static final Function<?, Mono<Void>> NOOP_HANDLER    = it -> Mono.empty();
    static final BiPredicate<?, ?>       NEVER_PREDICATE = (ignored1, ignored2) -> false;

}
