/*
 * Copyright (c) 2019-2024 VMware Inc. or its affiliates, All Rights Reserved.
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
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;

import org.reactivestreams.Publisher;

import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

/**
 * A default {@link PoolConfig} that can be extended to bear more configuration options
 * with access to a copy constructor for the basic options.
 *
 * @author Simon Baslé
 */
public class DefaultPoolConfig<POOLABLE> implements PoolConfig<POOLABLE> {

	protected final BiFunction<Runnable, Duration, Disposable> 	  pendingAcquireTimer;
	protected final Mono<POOLABLE>                                allocator;
	protected final AllocationStrategy                            allocationStrategy;
	protected final int                                           maxPending;
	protected final Function<POOLABLE, ? extends Publisher<Void>> releaseHandler;
	protected final Function<POOLABLE, ? extends Publisher<Void>> destroyHandler;
	protected final BiPredicate<POOLABLE, PooledRefMetadata>      evictionPredicate;
	protected final Duration                                      evictInBackgroundInterval;
	protected final Scheduler                                     evictInBackgroundScheduler;
	protected final Scheduler                                     acquisitionScheduler;
	protected final PoolMetricsRecorder                           metricsRecorder;
	protected final Clock                                         clock;
	protected final boolean                                       isIdleLRU;
	protected final ResourceManager resourceManager;

	public DefaultPoolConfig(Mono<POOLABLE> allocator,
			AllocationStrategy allocationStrategy,
			int maxPending,
			BiFunction<Runnable, Duration, Disposable> pendingAcquireTimer,
			Function<POOLABLE, ? extends Publisher<Void>> releaseHandler,
			Function<POOLABLE, ? extends Publisher<Void>> destroyHandler,
			BiPredicate<POOLABLE, PooledRefMetadata> evictionPredicate,
			Duration evictInBackgroundInterval,
			Scheduler evictInBackgroundScheduler,
			Scheduler acquisitionScheduler,
			PoolMetricsRecorder metricsRecorder,
			Clock clock,
			boolean isIdleLRU) {
		this(allocator, allocationStrategy, maxPending, pendingAcquireTimer, releaseHandler, destroyHandler,
				evictionPredicate, evictInBackgroundInterval, evictInBackgroundScheduler, acquisitionScheduler,
				metricsRecorder, clock, isIdleLRU,
				PoolBuilder.DEFAULT_RESOURCE_MANAGER);
	}

	public DefaultPoolConfig(Mono<POOLABLE> allocator,
			AllocationStrategy allocationStrategy,
			int maxPending,
			BiFunction<Runnable, Duration, Disposable> pendingAcquireTimer,
			Function<POOLABLE, ? extends Publisher<Void>> releaseHandler,
			Function<POOLABLE, ? extends Publisher<Void>> destroyHandler,
			BiPredicate<POOLABLE, PooledRefMetadata> evictionPredicate,
			Duration evictInBackgroundInterval,
			Scheduler evictInBackgroundScheduler,
			Scheduler acquisitionScheduler,
			PoolMetricsRecorder metricsRecorder,
			Clock clock,
			boolean isIdleLRU,
			ResourceManager resourceManager) {
		this.pendingAcquireTimer = pendingAcquireTimer;
		this.allocator = allocator;
		this.allocationStrategy = allocationStrategy;
		this.maxPending = maxPending;
		this.releaseHandler = releaseHandler;
		this.destroyHandler = destroyHandler;
		this.evictionPredicate = evictionPredicate;
		this.evictInBackgroundInterval = evictInBackgroundInterval;
		this.evictInBackgroundScheduler = evictInBackgroundScheduler;
		this.acquisitionScheduler = acquisitionScheduler;
		this.metricsRecorder = metricsRecorder;
		this.clock = clock;
		this.isIdleLRU = isIdleLRU;
		this.resourceManager = resourceManager;
	}

	/**
	 * Copy constructor for the benefit of specializations of {@link PoolConfig}.
	 *
	 * @param toCopy the original {@link PoolConfig} to copy (only standard {@link PoolConfig}
	 * options are copied)
	 */
	protected DefaultPoolConfig(PoolConfig<POOLABLE> toCopy) {
		if (toCopy instanceof DefaultPoolConfig) {
			DefaultPoolConfig<POOLABLE> toCopyDpc = (DefaultPoolConfig<POOLABLE>) toCopy;
			this.allocator = toCopyDpc.allocator;
			this.allocationStrategy = toCopyDpc.allocationStrategy;
			this.maxPending = toCopyDpc.maxPending;
			this.pendingAcquireTimer = toCopyDpc.pendingAcquireTimer;
			this.releaseHandler = toCopyDpc.releaseHandler;
			this.destroyHandler = toCopyDpc.destroyHandler;
			this.evictionPredicate = toCopyDpc.evictionPredicate;
			this.evictInBackgroundInterval = toCopyDpc.evictInBackgroundInterval;
			this.evictInBackgroundScheduler = toCopyDpc.evictInBackgroundScheduler;
			this.acquisitionScheduler = toCopyDpc.acquisitionScheduler;
			this.metricsRecorder = toCopyDpc.metricsRecorder;
			this.clock = toCopyDpc.clock;
			this.isIdleLRU = toCopyDpc.isIdleLRU;
			this.resourceManager = toCopyDpc.resourceManager;
		}
		else {
			this.allocator = toCopy.allocator();
			this.allocationStrategy = toCopy.allocationStrategy();
			this.maxPending = toCopy.maxPending();
			this.pendingAcquireTimer = toCopy.pendingAcquireTimer();
			this.releaseHandler = toCopy.releaseHandler();
			this.destroyHandler = toCopy.destroyHandler();
			this.evictionPredicate = toCopy.evictionPredicate();
			this.evictInBackgroundInterval = toCopy.evictInBackgroundInterval();
			this.evictInBackgroundScheduler = toCopy.evictInBackgroundScheduler();
			this.acquisitionScheduler = toCopy.acquisitionScheduler();
			this.metricsRecorder = toCopy.metricsRecorder();
			this.clock = toCopy.clock();
			this.isIdleLRU = toCopy.reuseIdleResourcesInLruOrder();
			this.resourceManager = toCopy.resourceManager();
		}
	}

	@Override
	public Mono<POOLABLE> allocator() {
		return this.allocator;
	}

	@Override
	public AllocationStrategy allocationStrategy() {
		return this.allocationStrategy;
	}

	@Override
	public int maxPending() {
		return this.maxPending;
	}

	@Override
	public BiFunction<Runnable, Duration, Disposable> pendingAcquireTimer() {
		return this.pendingAcquireTimer;
	}

	@Override
	public Function<POOLABLE, ? extends Publisher<Void>> releaseHandler() {
		return this.releaseHandler;
	}

	@Override
	public Function<POOLABLE, ? extends Publisher<Void>> destroyHandler() {
		return this.destroyHandler;
	}

	@Override
	public BiPredicate<POOLABLE, PooledRefMetadata> evictionPredicate() {
		return this.evictionPredicate;
	}

	@Override
	public Duration evictInBackgroundInterval() {
		return this.evictInBackgroundInterval;
	}

	@Override
	public Scheduler evictInBackgroundScheduler() {
		return this.evictInBackgroundScheduler;
	}

	@Override
	public Scheduler acquisitionScheduler() {
		return this.acquisitionScheduler;
	}

	@Override
	public PoolMetricsRecorder metricsRecorder() {
		return this.metricsRecorder;
	}

	@Override
	public Clock clock() {
		return this.clock;
	}

	@Override
	public boolean reuseIdleResourcesInLruOrder() {
		return isIdleLRU;
	}

	@Override
	public ResourceManager resourceManager() {
		return resourceManager;
	}
}
