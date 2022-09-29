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
				"testGauge.resources.acquired",
				"testGauge.resources.allocated",
				"testGauge.resources.idle",
				"testGauge.resources.pendingAcquire"
			);
	}

	@Test
	void metersRegisteredForRecorder() {
		SimpleMeterRegistry r = new SimpleMeterRegistry();

		PoolMetricsRecorder recorder = Micrometer.recorder("testRecorder", r);
		InstrumentedPool<String> pool = PoolBuilder.from(Mono.just("testResource"))
			.metricsRecorder(recorder)
			.buildPool();

		assertThat(r.getMeters().stream()
			.map(m -> {
				String name = m.getId().getName();
				String tags = m.getId().getTags().isEmpty() ? "" : String.valueOf(m.getId().getTags());
				return name + tags;
			}))
			.containsExactlyInAnyOrder(
				"testRecorder.allocation[tag(pool.allocation.outcome=success)]",
				"testRecorder.allocation[tag(pool.allocation.outcome=failure)]",
				"testRecorder.destroyed",
				"testRecorder.recycled",
				"testRecorder.recycled.notable[tag(pool.recycling.path=fast)]",
				"testRecorder.recycled.notable[tag(pool.recycling.path=slow)]",
				"testRecorder.reset",
				"testRecorder.resources.summary.lifetime",
				"testRecorder.resources.summary.idleness"
			);
	}

	@Test
	void micrometerInstrumentedPoolRegistersGaugeAndRecorderMetrics() {
		SimpleMeterRegistry r = new SimpleMeterRegistry();

		InstrumentedPool<String> pool = Micrometer.instrumentedPool(PoolBuilder.from(Mono.just("testResource")),
			"testMetrics", r);

		assertThat(r.getMeters().stream()
			.map(m -> {
				String name = m.getId().getName();
				String tags = m.getId().getTags().isEmpty() ? "" : String.valueOf(m.getId().getTags());
				return name + tags;
			}))
			.containsExactlyInAnyOrder(
				"testMetrics.resources.acquired",
				"testMetrics.resources.allocated",
				"testMetrics.resources.idle",
				"testMetrics.resources.pendingAcquire",
				"testMetrics.allocation[tag(pool.allocation.outcome=success)]",
				"testMetrics.allocation[tag(pool.allocation.outcome=failure)]",
				"testMetrics.destroyed",
				"testMetrics.recycled",
				"testMetrics.recycled.notable[tag(pool.recycling.path=fast)]",
				"testMetrics.recycled.notable[tag(pool.recycling.path=slow)]",
				"testMetrics.reset",
				"testMetrics.resources.summary.lifetime",
				"testMetrics.resources.summary.idleness"
			);
	}
}