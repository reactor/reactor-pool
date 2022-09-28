/*
 * Copyright (c) 2021-2022 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.time.Duration;
import java.util.function.Function;

import reactor.pool.InstrumentedPool;

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

	private InstrumentedPoolDecorators() { }

}
