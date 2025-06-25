/*
 * Copyright (c) 2022-2023 VMware Inc. or its affiliates, All Rights Reserved.
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

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import reactor.core.publisher.Mono;
import reactor.pool.InstrumentedPool;
import reactor.pool.PoolBuilder;
import reactor.pool.PoolMetricsRecorder;

import static org.assertj.core.api.Assertions.assertThat;

class MicrometerTest {

	@Test
	void metersRegisteredForGauge() {
		SimpleMeterRegistry r = new SimpleMeterRegistry();
		InstrumentedPool<String> pool = PoolBuilder.from(Mono.just("testResource")).buildPool();
		InstrumentedPool.PoolMetrics poolMetrics = pool.metrics();

		Micrometer.gaugesOf(poolMetrics, "testGauge", r);

		assertThat(r.getMeters().stream()
			.map(m -> {
				String name = m.getId().getName();
				String tags = m.getId().getTags().isEmpty() ? "" : String.valueOf(m.getId().getTags());
				return name + tags;
			}))
			.containsExactlyInAnyOrder(
				"reactor.pool.resources.acquired[tag(pool.name=testGauge)]",
				"reactor.pool.resources.allocated[tag(pool.name=testGauge)]",
				"reactor.pool.resources.idle[tag(pool.name=testGauge)]",
				"reactor.pool.resources.pendingAcquire[tag(pool.name=testGauge)]"
			);
	}

	@Test
	void metersRegisteredForRecorder() {
		SimpleMeterRegistry r = new SimpleMeterRegistry();

		PoolMetricsRecorder recorder = Micrometer.recorder("testRecorder", r);
		InstrumentedPool<String> pool = PoolBuilder.from(Mono.just("testResource"))
			.metricsRecorder(recorder)
			.buildPool();
		assertThat(pool).isNotNull();

		assertThat(r.getMeters().stream()
			.map(m -> {
				String name = m.getId().getName();
				String tags = m.getId().getTags().isEmpty() ? "" : String.valueOf(m.getId().getTags());
				return name + tags;
			}))
			.containsExactlyInAnyOrder(
				"reactor.pool.allocation[tag(pool.allocation.outcome=success), tag(pool.name=testRecorder)]",
				"reactor.pool.allocation[tag(pool.allocation.outcome=failure), tag(pool.name=testRecorder)]",
				"reactor.pool.destroyed[tag(pool.name=testRecorder)]",
				"reactor.pool.recycled[tag(pool.name=testRecorder)]",
				"reactor.pool.recycled.notable[tag(pool.name=testRecorder), tag(pool.recycling.path=fast)]",
				"reactor.pool.recycled.notable[tag(pool.name=testRecorder), tag(pool.recycling.path=slow)]",
				"reactor.pool.reset[tag(pool.name=testRecorder)]",
				"reactor.pool.resources.summary.lifetime[tag(pool.name=testRecorder)]",
				"reactor.pool.resources.summary.idleness[tag(pool.name=testRecorder)]",
				"reactor.pool.pending[tag(pool.name=testRecorder), tag(pool.pending.outcome=success)]",
				"reactor.pool.pending[tag(pool.name=testRecorder), tag(pool.pending.outcome=failure)]"
			);
	}

	@Test
	void micrometerInstrumentedPoolRegistersGaugeAndRecorderMetrics() {
		SimpleMeterRegistry r = new SimpleMeterRegistry();

		InstrumentedPool<String> pool = Micrometer.instrumentedPool(PoolBuilder.from(Mono.just("testResource")),
			"testMetrics", r);
		assertThat(pool).isNotNull();

		assertThat(r.getMeters().stream()
			.map(m -> {
				String name = m.getId().getName();
				String tags = m.getId().getTags().isEmpty() ? "" : String.valueOf(m.getId().getTags());
				return name + tags;
			}))
			.containsExactlyInAnyOrder(
				"reactor.pool.resources.acquired[tag(pool.name=testMetrics)]",
				"reactor.pool.resources.allocated[tag(pool.name=testMetrics)]",
				"reactor.pool.resources.idle[tag(pool.name=testMetrics)]",
				"reactor.pool.resources.pendingAcquire[tag(pool.name=testMetrics)]",
				"reactor.pool.allocation[tag(pool.allocation.outcome=success), tag(pool.name=testMetrics)]",
				"reactor.pool.allocation[tag(pool.allocation.outcome=failure), tag(pool.name=testMetrics)]",
				"reactor.pool.destroyed[tag(pool.name=testMetrics)]",
				"reactor.pool.recycled[tag(pool.name=testMetrics)]",
				"reactor.pool.recycled.notable[tag(pool.name=testMetrics), tag(pool.recycling.path=fast)]",
				"reactor.pool.recycled.notable[tag(pool.name=testMetrics), tag(pool.recycling.path=slow)]",
				"reactor.pool.reset[tag(pool.name=testMetrics)]",
				"reactor.pool.resources.summary.lifetime[tag(pool.name=testMetrics)]",
				"reactor.pool.resources.summary.idleness[tag(pool.name=testMetrics)]",
				"reactor.pool.pending[tag(pool.name=testMetrics), tag(pool.pending.outcome=success)]",
				"reactor.pool.pending[tag(pool.name=testMetrics), tag(pool.pending.outcome=failure)]"
			);
	}
}