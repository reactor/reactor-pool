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

import java.util.concurrent.TimeUnit;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;

import reactor.pool.PoolMetricsRecorder;

import static reactor.pool.introspection.micrometer.PoolMetersDocumentation.*;
import static reactor.pool.introspection.micrometer.PoolMetersDocumentation.AllocationTags.OUTCOME_FAILURE;
import static reactor.pool.introspection.micrometer.PoolMetersDocumentation.AllocationTags.OUTCOME_SUCCESS;
import static reactor.pool.introspection.micrometer.PoolMetersDocumentation.CommonTags.POOL_NAME;
import static reactor.pool.introspection.micrometer.PoolMetersDocumentation.RecycledNotableTags.PATH_FAST;
import static reactor.pool.introspection.micrometer.PoolMetersDocumentation.RecycledNotableTags.PATH_SLOW;

final class MicrometerMetricsRecorder implements PoolMetricsRecorder {

	private final String        poolName;
	private final MeterRegistry meterRegistry;

	private final Timer   allocationFailureTimer;
	private final Timer   allocationSuccessTimer;
	private final Timer   destroyedMeter;
	private final Counter recycledCounter;
	private final Counter recycledNotableFastPathCounter;
	private final Counter recycledNotableSlowPathCounter;
	private final Timer   resetMeter;
	private final Timer   resourceSummaryIdleness;
	private final Timer   resourceSummaryLifetime;
	private final Timer   pendingSuccessTimer;
	private final Timer   pendingFailureTimer;

	MicrometerMetricsRecorder(String poolName, MeterRegistry registry) {
		this.poolName = poolName;
		this.meterRegistry = registry;

		final Tags nameTag = Tags.of(POOL_NAME.asString(), this.poolName);

		allocationSuccessTimer = this.meterRegistry.timer(ALLOCATION.getName(),
			nameTag.and(OUTCOME_SUCCESS));
		allocationFailureTimer = this.meterRegistry.timer(ALLOCATION.getName(),
			nameTag.and(OUTCOME_FAILURE));

		resetMeter = this.meterRegistry.timer(RESET.getName(), nameTag);
		destroyedMeter = this.meterRegistry.timer(DESTROYED.getName(), nameTag);

		recycledCounter = this.meterRegistry.counter(RECYCLED.getName(), nameTag);

		recycledNotableFastPathCounter = this.meterRegistry.counter(RECYCLED_NOTABLE.getName(),
			nameTag.and(PATH_FAST));
		recycledNotableSlowPathCounter = this.meterRegistry.counter(RECYCLED_NOTABLE.getName(),
			nameTag.and(PATH_SLOW));

		resourceSummaryLifetime = this.meterRegistry.timer(SUMMARY_LIFETIME.getName(), nameTag);
		resourceSummaryIdleness = this.meterRegistry.timer(SUMMARY_IDLENESS.getName(), nameTag);

		pendingSuccessTimer = this.meterRegistry.timer(PENDING.getName(),
				nameTag.and(PendingTags.OUTCOME_SUCCESS));
		pendingFailureTimer = this.meterRegistry.timer(PENDING.getName(),
				nameTag.and(PendingTags.OUTCOME_FAILURE));
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
		resetMeter.record(latencyMs, TimeUnit.MILLISECONDS);
	}

	@Override
	public void recordDestroyLatency(long latencyMs) {
		destroyedMeter.record(latencyMs, TimeUnit.MILLISECONDS);
	}

	@Override
	public void recordRecycled() {
		recycledCounter.increment();
	}

	@Override
	public void recordLifetimeDuration(long millisecondsSinceAllocation) {
		resourceSummaryLifetime.record(millisecondsSinceAllocation, TimeUnit.MILLISECONDS);
	}

	@Override
	public void recordIdleTime(long millisecondsIdle) {
		resourceSummaryIdleness.record(millisecondsIdle, TimeUnit.MILLISECONDS);
	}

	@Override
	public void recordSlowPath() {
		recycledNotableSlowPathCounter.increment();
	}

	@Override
	public void recordFastPath() {
		recycledNotableFastPathCounter.increment();
	}

	@Override
	public void recordPendingSuccessAndLatency(long latencyMs) {
		pendingSuccessTimer.record(latencyMs, TimeUnit.MILLISECONDS);
	}

	@Override
	public void recordPendingFailureAndLatency(long latencyMs) {
		pendingFailureTimer.record(latencyMs, TimeUnit.MILLISECONDS);
	}
}
