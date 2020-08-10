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

import java.time.Clock;
import java.util.function.BiPredicate;
import java.util.function.Function;

import org.reactivestreams.Publisher;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

/**
 * A default {@link PoolConfig} that can be extended to bear more configuration options
 * with access to a copy constructor for the basic options.
 *
 * @author Simon Baslé
 */
public class DefaultPoolConfig<POOLABLE> implements PoolConfig<POOLABLE> {

	protected final Mono<POOLABLE>                                allocator;
	protected final AllocationStrategy                            allocationStrategy;
	protected final int                                           maxPending;
	protected final Function<POOLABLE, ? extends Publisher<Void>> releaseHandler;
	protected final Function<POOLABLE, ? extends Publisher<Void>> destroyHandler;
	protected final BiPredicate<POOLABLE, PooledRefMetadata>      evictionPredicate;
	protected final Scheduler                                     acquisitionScheduler;
	protected final PoolMetricsRecorder                           metricsRecorder;
	protected final Clock                                         clock;
	protected final boolean                                       isIdleLRU;

	public DefaultPoolConfig(Mono<POOLABLE> allocator,
			AllocationStrategy allocationStrategy,
			int maxPending,
			Function<POOLABLE, ? extends Publisher<Void>> releaseHandler,
			Function<POOLABLE, ? extends Publisher<Void>> destroyHandler,
			BiPredicate<POOLABLE, PooledRefMetadata> evictionPredicate,
			Scheduler acquisitionScheduler,
			PoolMetricsRecorder metricsRecorder,
			Clock clock,
			boolean isIdleLRU) {
		this.allocator = allocator;
		this.allocationStrategy = allocationStrategy;
		this.maxPending = maxPending;
		this.releaseHandler = releaseHandler;
		this.destroyHandler = destroyHandler;
		this.evictionPredicate = evictionPredicate;
		this.acquisitionScheduler = acquisitionScheduler;
		this.metricsRecorder = metricsRecorder;
		this.clock = clock;
		this.isIdleLRU = isIdleLRU;
	}

	/**
	 * @deprecated use the {@link #DefaultPoolConfig(Mono, AllocationStrategy, int, Function, Function, BiPredicate, Scheduler, PoolMetricsRecorder, Clock, boolean) other constructor}
	 * with explicit setting of {@code isIdleLru}, to be removed in 0.3.x
	 */
	@Deprecated
	public DefaultPoolConfig(Mono<POOLABLE> allocator,
			AllocationStrategy allocationStrategy,
			int maxPending,
			Function<POOLABLE, ? extends Publisher<Void>> releaseHandler,
			Function<POOLABLE, ? extends Publisher<Void>> destroyHandler,
			BiPredicate<POOLABLE, PooledRefMetadata> evictionPredicate,
			Scheduler acquisitionScheduler,
			PoolMetricsRecorder metricsRecorder,
			Clock clock) {
		this(allocator, allocationStrategy, maxPending, releaseHandler, destroyHandler, evictionPredicate, acquisitionScheduler, metricsRecorder, clock,
				true);
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
			this.releaseHandler = toCopyDpc.releaseHandler;
			this.destroyHandler = toCopyDpc.destroyHandler;
			this.evictionPredicate = toCopyDpc.evictionPredicate;
			this.acquisitionScheduler = toCopyDpc.acquisitionScheduler;
			this.metricsRecorder = toCopyDpc.metricsRecorder;
			this.clock = toCopyDpc.clock;
			this.isIdleLRU = toCopyDpc.isIdleLRU;
		}
		else {
			this.allocator = toCopy.allocator();
			this.allocationStrategy = toCopy.allocationStrategy();
			this.maxPending = toCopy.maxPending();
			this.releaseHandler = toCopy.releaseHandler();
			this.destroyHandler = toCopy.destroyHandler();
			this.evictionPredicate = toCopy.evictionPredicate();
			this.acquisitionScheduler = toCopy.acquisitionScheduler();
			this.metricsRecorder = toCopy.metricsRecorder();
			this.clock = toCopy.clock();
			this.isIdleLRU = toCopy.reuseIdleResourcesInLruOrder();
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
}
