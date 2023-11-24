/*
 * Copyright (c) 2021-2024 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.pool.decorators;

import org.reactivestreams.Publisher;
import reactor.pool.InstrumentedPool;
import reactor.pool.PoolBuilder;
import reactor.pool.PoolConfig;
import reactor.pool.PoolScheduler;
import reactor.util.function.Tuple2;

import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.function.Function;

/**
 * Utility class to expose various {@link InstrumentedPool} decorators, which can also be used
 * via {@link reactor.pool.PoolBuilder#buildPoolAndDecorateWith(Function)}.
 *
 * @author Simon Baslé
 */
public final class InstrumentedPoolDecorators {

	/**
	 * Decorate the pool with the capacity to {@link GracefulShutdownInstrumentedPool gracefully shutdown},
	 * via {@link GracefulShutdownInstrumentedPool#disposeGracefully(Duration)}.
	 *
	 * @param pool the original pool
	 * @param <T> the type of resources in the pool
	 * @return the decorated pool which can now gracefully shutdown via additional methods
	 * @see GracefulShutdownInstrumentedPool
	 */
	public static <T> GracefulShutdownInstrumentedPool<T> gracefulShutdown(InstrumentedPool<T> pool) {
		return new GracefulShutdownInstrumentedPool<>(pool);
	}

	/**
	 * Creates a pool composed of multiple sub pools, each managing a portion of resources. Resource acquisitions will
	 * be concurrently distributed across sub pools using sub pool executors, in a work stealing style.
	 * Each sub pool must be assigned to a dedicated Executor (which must not be shared between sub pools).
	 *
	 * @param size      The max number of sub pools to create.
	 * @param allocator the asynchronous creator of poolable resources, subscribed each time a new
	 * 	                resource needs to be created. This allocator will be shared by all created sub pools.
	 * @param factory   A factory method creating sub pools called with the resourceManager of the sub pool to create.
	 *                  This Function takes a PoolBuilder that is pre-initialized with the specified allocator must return
	 *                  a new Pool instance with it's dedicated Executor in the form of a Tuple2 obejct.
	 *                  Executors must not be shared between sub pools.
	 * @param <T> the type of resources in the pool
	 * @return a decorated concurrent InstrumentedPool that will distribute resource acquisitions across all sub pools in a work
	 * stealing way, using dedicated sub pool executors
	 */
	public static <T> PoolScheduler<T> concurrentPools(int size,
													   Publisher<? extends T> allocator,
													   Function<PoolBuilder<T, PoolConfig<T>>, Tuple2<InstrumentedPool<T>, Executor>> factory) {
		return new WorkStealingPool<>(size, resourceManager ->
				factory.apply(PoolBuilder.<T>from(allocator).resourceManager(resourceManager)));
	}

	private InstrumentedPoolDecorators() { }

}
