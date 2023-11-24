/*
 * Copyright (c) 2018-2024 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.pool;

import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;

import org.reactivestreams.Publisher;

import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

/**
 * A builder for {@link Pool} implementations, which tuning methods map
 * to a {@link PoolConfig} subclass, {@code CONF}.
 *
 * @param <T> the type of elements in the produced {@link Pool}
 * @param <CONF> the {@link PoolConfig} flavor this builder will provide to the created {@link Pool}
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

	final Mono<T>                          allocator;
	final Function<PoolConfig<T>, CONF>    configModifier;
	int                                    maxPending                  = -1;
	AllocationStrategy                     allocationStrategy          = null;
	Function<T, ? extends Publisher<Void>> releaseHandler              = noopHandler();
	Function<T, ? extends Publisher<Void>> destroyHandler              = noopHandler();
	BiPredicate<T, PooledRefMetadata>      evictionPredicate           = neverPredicate();
	Duration                               evictionBackgroundInterval  = Duration.ZERO;
	Scheduler                              evictionBackgroundScheduler = Schedulers.immediate();
	Scheduler                              acquisitionScheduler        = Schedulers.immediate();
	Clock                                  clock                       = Clock.systemUTC();
	PoolMetricsRecorder                    metricsRecorder             = NoOpPoolMetricsRecorder.INSTANCE;
	boolean                                idleLruOrder         = true;
	BiFunction<Runnable, Duration, Disposable> pendingAcquireTimer = DEFAULT_PENDING_ACQUIRE_TIMER;

	ResourceManager resourceManager = DEFAULT_RESOURCE_MANAGER;

	PoolBuilder(Mono<T> allocator, Function<PoolConfig<T>, CONF> configModifier) {
		this.allocator = allocator;
		this.configModifier = configModifier;
	}

	PoolBuilder(PoolBuilder<T, ?> source, Function<PoolConfig<T>, CONF> configModifier) {
		this.configModifier = configModifier;

		this.allocator = source.allocator;
		this.maxPending = source.maxPending;
		this.allocationStrategy = source.allocationStrategy;
		this.releaseHandler = source.releaseHandler;
		this.destroyHandler = source.destroyHandler;
		this.evictionPredicate = source.evictionPredicate;
		this.evictionBackgroundInterval = source.evictionBackgroundInterval;
		this.evictionBackgroundScheduler = source.evictionBackgroundScheduler;
		this.acquisitionScheduler = source.acquisitionScheduler;
		this.metricsRecorder = source.metricsRecorder;
		this.clock = source.clock;
		this.idleLruOrder = source.idleLruOrder;
		this.pendingAcquireTimer = source.pendingAcquireTimer;
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
	 * Define how timeouts are scheduled when a {@link Pool#acquire(Duration)} call is made and the acquisition is pending.
	 * i.e. there is no idle resource and no new resource can be created currently, so a timeout is scheduled using the
	 * provided function.
	 * <p>
	 *
	 * By default, the {@link Schedulers#parallel()} scheduler is used.
	 *
	 * @param pendingAcquireTimer the function to apply when scheduling timers for acquisitions that are added to the pending queue.
	 * @return this {@link Pool} builder
	 */
	public PoolBuilder<T, CONF> pendingAcquireTimer(BiFunction<Runnable, Duration, Disposable> pendingAcquireTimer) {
		this.pendingAcquireTimer = Objects.requireNonNull(pendingAcquireTimer, "pendingAcquireTimer");
		return this;
	}

	/**
	 * Limits in how many resources can be allocated and managed by the {@link Pool} are driven by the
	 * provided {@link AllocationStrategy}. This is a customization escape hatch that replaces the last
	 * configured strategy, but most cases should be covered by the {@link #sizeBetween(int, int)} or {@link #sizeUnbounded()}
	 * pre-made strategies.
	 * <p>
	 * Without a call to any of these 3 methods, the builder defaults to an {@link #sizeUnbounded() unbounded creation of resources},
	 * although it is not a recommended one.
	 *
	 * @param allocationStrategy the {@link AllocationStrategy} to use
	 * @return this {@link Pool} builder
	 * @see #sizeBetween(int, int)
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
	 * Set the {@link #evictionPredicate(BiPredicate) eviction predicate} to cause eviction (ie returns {@code true})
	 * of resources that have been idle (ie released and available in the {@link Pool}) for more than the {@code ttl}
	 * {@link Duration} (inclusive).
	 * Such a predicate could be used to evict too idle objects when next encountered by an {@link Pool#acquire()}.
	 * <p>
	 * This replaces any {@link #evictionPredicate(BiPredicate)} previously set. If you need to combine idle predicate
	 * with more custom logic, prefer directly providing a {@link BiPredicate}. Note that the idle predicate from this
	 * method is written as {@code (poolable, meta) -> meta.idleTime() >= maxIdleTime.toMillis()}.
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
	 * <p>
	 * In case some asynchronous verification of the health of a resource is needed, one possible approach is
	 * to rely on {@link Pool#acquire()} and {@link PooledRef#invalidate()} instead of the evictionPredicate:
	 * performing the check in a flatMap after having acquired the resource, then further chain an invalidate call
	 * if the resource is not healthy. The acquisition should then be retried, and a good way of doing so is by
	 * continuing the invalidate call with a {@link Mono#error(Throwable)} with a custom exception which would trigger
	 * an outer {@link Mono#retryWhen(Retry)}.
	 *
	 * @param evictionPredicate a {@link Predicate} that returns {@code true} if the resource is unfit for the pool and should
	 * be destroyed, {@code false} if it should be put back into the pool
	 * @return this {@link Pool} builder
	 * @see #evictionIdle(Duration)
	 */
	public PoolBuilder<T, CONF> evictionPredicate(BiPredicate<T, PooledRefMetadata> evictionPredicate) {
		this.evictionPredicate = Objects.requireNonNull(evictionPredicate, "evictionPredicate");
		return this;
	}

	/**
	 * Disable background eviction entirely, so that {@link #evictionPredicate(BiPredicate) evictionPredicate}
	 * is only checked upon {@link Pool#acquire() acquire} and {@link PooledRef#release() release} (ie only
	 * when there is pool activity).
	 *
	 * @return this {@link Pool} builder
	 * @see #evictInBackground(Duration)
	 */
	public PoolBuilder<T, CONF> evictInBackgroundDisabled() {
		return evictInBackground(Duration.ZERO);
	}

	/**
	 * Enable background eviction so that {@link #evictionPredicate(BiPredicate) evictionPredicate} is regularly
	 * applied to elements that are idle in the pool when there is no pool activity (i.e. {@link Pool#acquire() acquire}
	 * and {@link PooledRef#release() release}).
	 * <p>
	 * Providing an {@code evictionInterval} of {@link Duration#ZERO zero} is similar to {@link #evictInBackgroundDisabled() disabling}
	 * background eviction.
	 * <p>
	 * Background eviction support is optional: although all vanilla reactor-pool implementations DO support it,
	 * other implementations MAY ignore it.
	 * The background eviction process can be implemented in a best effort fashion, backing off if it detects any pool activity.
	 *
	 * @return this {@link Pool} builder
	 * @see #evictInBackground(Duration, Scheduler)
	 */
	public PoolBuilder<T, CONF> evictInBackground(Duration evictionInterval) {
		if (evictionInterval == Duration.ZERO) {
			this.evictionBackgroundInterval = Duration.ZERO;
			this.evictionBackgroundScheduler = Schedulers.immediate();
			return this;
		}
		return this.evictInBackground(evictionInterval, Schedulers.parallel());
	}

	/**
	 * Enable background eviction so that {@link #evictionPredicate(BiPredicate) evictionPredicate} is regularly
	 * applied to elements that are idle in the pool when there is no pool activity (i.e. {@link Pool#acquire() acquire}
	 * and {@link PooledRef#release() release}).
	 * <p>
	 * Providing an {@code evictionInterval} of {@link Duration#ZERO zero} is similar to {@link #evictInBackgroundDisabled() disabling}
	 * background eviction.
	 * <p>
	 * Background eviction support is optional: although all vanilla reactor-pool implementations DO support it,
	 * other implementations MAY ignore it.
	 * The background eviction process can be implemented in a best effort fashion, backing off if it detects any pool activity.
	 *
	 * @return this {@link Pool} builder
	 * @see #evictInBackground(Duration)
	 */
	public PoolBuilder<T, CONF> evictInBackground(Duration evictionInterval, Scheduler reaperTaskScheduler) {
		this.evictionBackgroundInterval = evictionInterval;
		this.evictionBackgroundScheduler = reaperTaskScheduler;
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
	 * @return a builder of {@link Pool} with no maximum pending queue size
	 */
	public PoolBuilder<T, CONF> maxPendingAcquireUnbounded() {
		this.maxPending = -1;
		return this;
	}

	/**
	 * Set the {@link Clock} to use for timestamps, notably marking the times at which a resource is
	 * allocated, released and acquired. The {@link Clock#millis()} method is used for this purpose,
	 * which produces timestamps and durations in milliseconds for eg. the {@link #evictionPredicate(BiPredicate)}.
	 * <p>
	 * These durations can also be exported as metrics, through the {@link #metricsRecorder}, but having
	 * a separate {@link Clock} separates the concerns and allows to disregard metrics without crippling
	 * eviction.
	 *
	 * @param clock the {@link Clock} to use to measure timestamps and durations
	 * @return this {@link Pool} builder
	 */
	public PoolBuilder<T, CONF> clock(Clock clock) {
		this.clock = Objects.requireNonNull(clock, "clock");
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
	 * Replace the {@link AllocationStrategy} with one that lets the {@link Pool} allocate between {@code min} and {@code max} resources.
	 * When acquiring and there is no available resource, the pool should strive to warm up enough resources to reach
	 * {@code min} live resources before serving the acquire with (one of) the newly created resource(s).
	 * At the same time it MUST NOT allocate any resource if that would bring the number of live resources
	 * over the {@code max}, rejecting further allocations until some resources have been {@link PooledRef#release() released}.
	 * <p>
	 * Pre-allocation of warmed-up resources, if any, will be performed sequentially by subscribing to the allocator
	 * one at a time. The process waits for a resource to be created before subscribing again to the allocator.
	 * This sequence continues until all pre-allocated resources have been successfully created.
	 *
	 * @param min the minimum number of live resources to keep in the pool (can be best effort)
	 * @param max the maximum number of live resources to keep in the pool. use {@link Integer#MAX_VALUE} when you only need a
	 * minimum and no upper bound
	 * @return this {@link Pool} builder
	 * @see #sizeUnbounded()
	 * @see #allocationStrategy(AllocationStrategy)
	 */
	public PoolBuilder<T, CONF> sizeBetween(int min, int max) {
		return sizeBetween(min, max, DEFAULT_WARMUP_PARALLELISM);
	}

	/**
	 * Replace the {@link AllocationStrategy} with one that lets the {@link Pool} allocate between {@code min} and {@code max} resources.
	 * When acquiring and there is no available resource, the pool should strive to warm up enough resources to reach
	 * {@code min} live resources before serving the acquire with (one of) the newly created resource(s).
	 * At the same time it MUST NOT allocate any resource if that would bring the number of live resources
	 * over the {@code max}, rejecting further allocations until some resources have been {@link PooledRef#release() released}.
	 *
	 * @param min the minimum number of live resources to keep in the pool (can be best effort)
	 * @param max the maximum number of live resources to keep in the pool. use {@link Integer#MAX_VALUE} when you only need a
	 * minimum and no upper bound
	 * @param warmupParallelism Specifies the concurrency level used when the allocator is subscribed to during the warmup phase, if any.
	 * During warmup, resources that can be pre-allocated will be created eagerly, but at most {@code warmupParallelism} resources are
	 * subscribed to at the same time.
	 * A {@code warmupParallelism} of 1 means that pre-allocation of resources is achieved by sequentially subscribing to the allocator,
	 * waiting for a resource to be created before subscribing a next time to the allocator, and so on until the last allocation
	 * completes.
	 * @return this {@link Pool} builder
	 * @see #sizeUnbounded()
	 * @see #allocationStrategy(AllocationStrategy)
	 * @since 1.0.1
	 */
	public PoolBuilder<T, CONF> sizeBetween(int min, int max, int warmupParallelism) {
		return allocationStrategy(new AllocationStrategies.SizeBasedAllocationStrategy(min, max, warmupParallelism));
	}

	/**
	 * Replace the {@link AllocationStrategy} with one that lets the {@link Pool} allocate new resources
	 * when no idle resource is available, without limit.
	 * <p>
	 * Note this is the default, if no previous call to {@link #allocationStrategy(AllocationStrategy)}
	 * or {@link #sizeBetween(int, int)} has been made on this {@link PoolBuilder}.
	 *
	 * @return this {@link Pool} builder
	 * @see #sizeBetween(int, int)
	 * @see #allocationStrategy(AllocationStrategy)
	 */
	public PoolBuilder<T, CONF> sizeUnbounded() {
		return allocationStrategy(new AllocationStrategies.UnboundedAllocationStrategy());
	}

	/**
	 * Configure the pool so that if there are idle resources (ie pool is under-utilized),
	 * the next {@link Pool#acquire()} will get the <b>Least Recently Used</b> resource
	 * (LRU, ie. the resource that was released first among the current idle resources).
	 *
	 * @return this {@link Pool} builder
	 */
	public PoolBuilder<T, CONF> idleResourceReuseLruOrder() {
		return idleResourceReuseOrder(true);
	}

	/**
	 * Configure the pool so that if there are idle resources (ie pool is under-utilized),
	 * the next {@link Pool#acquire()} will get the <b>Most Recently Used</b> resource
	 * (MRU, ie. the resource that was released last among the current idle resources).
	 *
	 * @return this {@link Pool} builder
	 */
	public PoolBuilder<T, CONF> idleResourceReuseMruOrder() {
		return idleResourceReuseOrder(false);
	}

	/**
	 * Configure the order in which idle resources are used when the next {@link Pool#acquire()}
	 * is performed (while the pool is under-utilized). Allows to chose between
	 * the <b>Least Recently Used</b> order when {@code true} (LRU, ie. the resource that
	 * was released first among the current idle resources, the default) and
	 * <b>Most Recently Used</b> order (MRU, ie. the resource that was released last among
	 * the current idle resources).
	 *
	 * @param isLru {@code true} for LRU (the default) or {@code false} for MRU
	 * @return this {@link Pool} builder
	 * @see #idleResourceReuseLruOrder()
	 * @see #idleResourceReuseMruOrder()
	 */
	public PoolBuilder<T, CONF> idleResourceReuseOrder(boolean isLru) {
		this.idleLruOrder = isLru;
		return this;
	}

	public PoolBuilder<T, CONF> resourceManager(ResourceManager resourceManager) {
		this.resourceManager = resourceManager;
		return this;
	}

	/**
	 * Add implementation-specific configuration, changing the type of {@link PoolConfig}
	 * passed to the {@link Pool} factory in {@link #build(Function)}.
	 *
	 * @param configModifier {@link Function} to transform the type of {@link PoolConfig}
	 * created by this builder for the benefit of the pool factory, allowing for custom
	 * implementations with custom configurations
	 * @param <CONF2> new type for the configuration
	 * @return a new PoolBuilder that now produces a different type of {@link PoolConfig}
	 */
	public <CONF2 extends PoolConfig<T>> PoolBuilder<T, CONF2> extraConfiguration(Function<? super CONF, CONF2> configModifier) {
		return new PoolBuilder<>(this, this.configModifier.andThen(configModifier));
	}

	/**
	 * Construct a default reactor pool with the builder's configuration.
	 *
	 * @return an {@link InstrumentedPool}
	 */
	public InstrumentedPool<T> buildPool() {
		return new SimpleDequePool<>(this.buildConfig());
	}

	/**
	 * Construct a default reactor pool with the builder's configuration, then wrap it
	 * into a decorator implementation using the provided {@link Function}.
	 *
	 * @param decorator a decorator {@link Function} returning a decorated version of the {@link InstrumentedPool}
	 * @param <P> the type of decorated pool, must extend {@link InstrumentedPool} (with same type of resource)
	 * @return the built-then-decorated pool
	 */
	public <P extends InstrumentedPool<T>> P buildPoolAndDecorateWith(Function<? super InstrumentedPool<T>, P> decorator) {
		return decorator.apply(buildPool());
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
		PoolConfig<T> baseConfig = new DefaultPoolConfig<>(allocator,
				allocationStrategy == null ?
						new AllocationStrategies.UnboundedAllocationStrategy() :
						allocationStrategy,
				maxPending,
				pendingAcquireTimer,
				releaseHandler,
				destroyHandler,
				evictionPredicate,
				evictionBackgroundInterval,
				evictionBackgroundScheduler,
				acquisitionScheduler,
				metricsRecorder,
				clock,
				idleLruOrder,
				resourceManager);

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
	static final BiFunction<Runnable, Duration, Disposable> DEFAULT_PENDING_ACQUIRE_TIMER = (r, d) -> Schedulers.parallel().schedule(r, d.toNanos(), TimeUnit.NANOSECONDS);
	static final int DEFAULT_WARMUP_PARALLELISM = 1;

	static final ResourceManager DEFAULT_RESOURCE_MANAGER = () -> {};
}
