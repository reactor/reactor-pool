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

import java.util.concurrent.TimeUnit;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;

import reactor.pool.PoolMetricsRecorder;

final class MicrometerMetricsRecorder implements PoolMetricsRecorder {

	private final String        metricsPrefix;
	private final MeterRegistry meterRegistry;

	private final Timer               allocationFailureTimer;
	private final Timer               allocationSuccessTimer;
	private final DistributionSummary destroyedMeter;
	private final Counter             recycledCounter;
	private final Counter             recycledNotableFastPathCounter;
	private final Counter             recycledNotableSlowPathCounter;
	private final DistributionSummary resetMeter;
	private final DistributionSummary resourceSummaryIdleness;
	private final DistributionSummary resourceSummaryLifetime;

	MicrometerMetricsRecorder(String metricsPrefix, MeterRegistry registry) {
		this.metricsPrefix = metricsPrefix;
		this.meterRegistry = registry;

		allocationSuccessTimer = this.meterRegistry.timer(
			DocumentedPoolMeters.ALLOCATION.getName(this.metricsPrefix),
			Tags.of(DocumentedPoolMeters.AllocationTags.OUTCOME_SUCCESS));
		allocationFailureTimer = this.meterRegistry.timer(
			DocumentedPoolMeters.ALLOCATION.getName(this.metricsPrefix),
			Tags.of(DocumentedPoolMeters.AllocationTags.OUTCOME_FAILURE));

		resetMeter = this.meterRegistry.summary(
			DocumentedPoolMeters.RESET.getName(this.metricsPrefix));
		destroyedMeter = this.meterRegistry.summary(
			DocumentedPoolMeters.DESTROYED.getName(this.metricsPrefix));

		recycledCounter = this.meterRegistry.counter(
			DocumentedPoolMeters.RECYCLED.getName(this.metricsPrefix));

		recycledNotableFastPathCounter = this.meterRegistry.counter(
			DocumentedPoolMeters.RECYCLED_NOTABLE.getName(this.metricsPrefix),
			Tags.of(DocumentedPoolMeters.RecycledNotableTags.PATH_FAST));
		recycledNotableSlowPathCounter = this.meterRegistry.counter(
			DocumentedPoolMeters.RECYCLED_NOTABLE.getName(this.metricsPrefix),
			Tags.of(DocumentedPoolMeters.RecycledNotableTags.PATH_SLOW));

		resourceSummaryLifetime = this.meterRegistry.summary(
			DocumentedPoolMeters.SUMMARY_LIFETIME.getName(this.metricsPrefix));
		resourceSummaryIdleness = this.meterRegistry.summary(
			DocumentedPoolMeters.SUMMARY_IDLENESS.getName(this.metricsPrefix));
	}

	@Override
	public void recordAllocationSuccessAndLatency(long latencyMs) {
		allocationSuccessTimer.record(latencyMs, TimeUnit.MILLISECONDS);
	}

	@Override
	public void recordAllocationFailureAndLatency(long latencyMs) {
		allocationFailureTimer.record(latencyMs, TimeUnit.MILLISECONDS);
	}

	@Override
	public void recordResetLatency(long latencyMs) {
		resetMeter.record(latencyMs);
	}

	@Override
	public void recordDestroyLatency(long latencyMs) {
		destroyedMeter.record(latencyMs);
	}

	@Override
	public void recordRecycled() {
		recycledCounter.increment();
	}

	@Override
	public void recordLifetimeDuration(long millisecondsSinceAllocation) {
		resourceSummaryLifetime.record(millisecondsSinceAllocation);
	}

	@Override
	public void recordIdleTime(long millisecondsIdle) {
		resourceSummaryIdleness.record(millisecondsIdle);
	}

	@Override
	public void recordSlowPath() {
		recycledNotableSlowPathCounter.increment();
	}

	@Override
	public void recordFastPath() {
		recycledNotableFastPathCounter.increment();
	}
}
