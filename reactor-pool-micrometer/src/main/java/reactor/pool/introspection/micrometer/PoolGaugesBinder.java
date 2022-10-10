/*
 * Copyright (c) 2022 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.pool.introspection.micrometer;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.MeterBinder;

import reactor.pool.InstrumentedPool;
import reactor.pool.PoolBuilder;

import static reactor.pool.introspection.micrometer.PoolMetersDocumentation.CommonTags.POOL_NAME;

/**
 * A {@link MeterBinder} that registers Micrometer gauges around a {@link InstrumentedPool}'s {@link reactor.pool.InstrumentedPool.PoolMetrics},
 * publishing to the provided {@link MeterRegistry}. One can differentiate between pools thanks to the provided {@code poolName},
 * which will be set on all meters as the value for the {@link PoolMetersDocumentation.CommonTags#POOL_NAME} tag.
 * <p>
 * {@link PoolMetersDocumentation} include the gauges which are:
 * <ul>
 *     <li> {@link PoolMetersDocumentation#ACQUIRED} </li>
 *     <li> {@link PoolMetersDocumentation#ALLOCATED}, </li>
 *     <li> {@link PoolMetersDocumentation#IDLE} </li>
 *     <li> {@link PoolMetersDocumentation#PENDING_ACQUIRE} </li>
 * </ul>
 * <p>
 * Note that this doesn't cover metrics that show evolution of the pool's state and timings, which are separately
 * measured using a {@link reactor.pool.PoolMetricsRecorder} provided when building the pool.
 * See {@link Micrometer#recorder(String, MeterRegistry)}, as well as {@link Micrometer#instrumentedPool(PoolBuilder, String, MeterRegistry)}
 * for a solution that covers both.
 *
 * @author Simon Baslé
 */
public final class PoolGaugesBinder implements MeterBinder {

	private final InstrumentedPool.PoolMetrics poolMetrics;
	private final String                       poolName;

	/**
	 * Create a {@link PoolGaugesBinder}.
	 *
	 * @param poolMetrics the {@link reactor.pool.InstrumentedPool.PoolMetrics} to turn into gauges
	 * @param poolName the tag value to use on the gauges to differentiate between pools
	 * @see PoolMetersDocumentation
	 */
	public PoolGaugesBinder(InstrumentedPool.PoolMetrics poolMetrics, String poolName) {
		this.poolMetrics = poolMetrics;
		this.poolName = poolName;
	}

	@Override
	public void bindTo(MeterRegistry meterRegistry) {
		Tags nameTag = Tags.of(POOL_NAME.asString(), poolName);
		Gauge.builder(
				PoolMetersDocumentation.ACQUIRED.getName(), poolMetrics,
				InstrumentedPool.PoolMetrics::acquiredSize)
			.tags(nameTag)
			.register(meterRegistry);
		Gauge.builder(
				PoolMetersDocumentation.ALLOCATED.getName(), poolMetrics,
				InstrumentedPool.PoolMetrics::allocatedSize)
			.tags(nameTag)
			.register(meterRegistry);
		Gauge.builder(
				PoolMetersDocumentation.IDLE.getName(), poolMetrics,
				InstrumentedPool.PoolMetrics::idleSize)
			.tags(nameTag)
			.register(meterRegistry);
		Gauge.builder(
				PoolMetersDocumentation.PENDING_ACQUIRE.getName(), poolMetrics,
				InstrumentedPool.PoolMetrics::pendingAcquireSize)
			.tags(nameTag)
			.register(meterRegistry);
	}
}