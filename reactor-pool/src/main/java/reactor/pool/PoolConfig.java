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
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;

import org.reactivestreams.Publisher;

import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * A representation of the common configuration options of a {@link Pool}.
 * For a default implementation that is open for extension, see {@link DefaultPoolConfig}.
 *
 * @author Simon Baslé
 */
public interface PoolConfig<POOLABLE> {

	/**
	 * The asynchronous factory that produces new resources, represented as a {@link Mono}.
	 */
	Mono<POOLABLE> allocator();

	/**
	 * {@link AllocationStrategy} defines a strategy / limit for the number of pooled object to allocate.
	 */
	AllocationStrategy allocationStrategy();

	/**
	 * The maximum number of pending borrowers to enqueue before failing fast. 0 will immediately fail any acquire
	 * when no idle resource is available and the pool cannot grow. Use a negative number to deactivate.
	 */
	int maxPending();

	/**
	 * When a resource is {@link PooledRef#release() released}, defines a mechanism of resetting any lingering state of
	 * the resource in order for it to become usable again. The {@link #evictionPredicate} is applied AFTER this reset.
	 * <p>
	 * For example, a buffer could have a readerIndex and writerIndex that need to be flipped back to zero.
	 */
	Function<POOLABLE, ? extends Publisher<Void>> releaseHandler();

	/**
	 * Defines a mechanism of resource destruction, cleaning up state and OS resources it could maintain (eg. off-heap
	 * objects, file handles, socket connections, etc...).
	 * <p>
	 * For example, a database connection could need to cleanly sever the connection link by sending a message to the database.
	 */
	Function<POOLABLE, ? extends Publisher<Void>> destroyHandler();

	/**
	 * A {@link BiPredicate} that checks if a resource should be destroyed ({@code true}) or is still in a valid state
	 * for recycling. This is primarily applied when a resource is released, to check whether or not it can immediately
	 * be recycled, but could also be applied during an acquire attempt (detecting eg. idle resources) or by a background
	 * reaping process. Both the resource and some {@link PooledRefMetadata metrics} about the resource's life within the pool are provided.
	 */
	BiPredicate<POOLABLE, PooledRefMetadata> evictionPredicate();

	/**
	 * If the pool is configured to perform regular eviction checks on the background, returns the {@link Duration} representing
	 * the interval at which such checks are made. Otherwise returns {@link Duration#ZERO} (the default).
	 */
	default Duration evictInBackgroundInterval() {
		return Duration.ZERO; //TODO remove the default implementation in 0.2.0
	}

	/**
	 * If the pool is configured to perform regular eviction checks on the background, returns the {@link Scheduler} on
	 * which these checks are made. Otherwise returns {@link Schedulers#immediate()} (the default).
	 */
	default Scheduler evictInBackgroundScheduler() {
		return Schedulers.immediate(); //TODO remove the default implementation in 0.2.0
	}

	/**
	 * When set, {@link Pool} implementation MAY decide to use the {@link Scheduler}
	 * to publish resources in a more deterministic way: the publishing thread would then
	 * always be the same, independently of which thread called {@link Pool#acquire()} or
	 * {@link PooledRef#release()} or on which thread the {@link #allocator} produced new
	 * resources. Note that not all pool implementations are guaranteed to enforce this,
	 * as they might have their own thread publishing semantics.
	 * <p>
	 * Defaults to {@link Schedulers#immediate()}, which inhibits this behavior.
	 */
	Scheduler acquisitionScheduler();

	/**
	 * The {@link PoolMetricsRecorder} to use to collect instrumentation data of the {@link Pool}
	 * implementations.
	 */
	PoolMetricsRecorder metricsRecorder();

	/**
	 * The {@link java.time.Clock} to use to timestamp pool lifecycle events like allocation
	 * and eviction, which can influence eg. the {@link #evictionPredicate()}.
	 */
	Clock clock();

	/**
	 * The order in which idle (aka available) resources should be used when the pool was
	 * under-utilized and a new {@link Pool#acquire()} is performed. Returns {@code true}
	 * if LRU (Least-Recently Used, the resource that was released first is emitted) or
	 * {@code false} for MRU (Most-Recently Used, the resource that was released last is
	 * emitted).
	 *
	 * @return {@code true} for LRU, {@code false} for MRU
	 */
	boolean reuseIdleResourcesInLruOrder();

	/**
	 * The function that defines how timeouts are scheduled when a {@link Pool#acquire(Duration)} call is made and the acquisition is pending.
	 * i.e. there is no idle resource and no new resource can be created currently, so a timeout is scheduled using the returned function.
	 * <p>
	 *
	 * By default, the {@link Schedulers#parallel()} scheduler is used.
	 *
	 * @return the function to apply when scheduling timers for pending acquisitions
	 */
	default BiFunction<Runnable, Duration, Disposable> pendingAcquireTimer() {
		return PoolBuilder.DEFAULT_PENDING_ACQUIRE_TIMER;
	}

	default ResourceManager resourceManager() {
		return PoolBuilder.DEFAULT_RESOURCE_MANAGER;
	}

}
