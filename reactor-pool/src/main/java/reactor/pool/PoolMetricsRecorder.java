/*
 * Copyright (c) 2019-2023 VMware Inc. or its affiliates, All Rights Reserved.
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

/**
 * An interface representing ways for {@link Pool} to collect instrumentation data.
 * Some methods are pool-implementation specific.
 * <p>
 * Note this doesn't include the concepts of measuring timings, which should be the
 * responsibility of a {@link java.time.Clock}.
 *
 * @author Simon Baslé
 * @author Violeta Georgieva
 */
public interface PoolMetricsRecorder {

	/**
	 * Record a latency for successful allocation. Implies incrementing an allocation success counter as well.
	 * @param latencyMs the latency in milliseconds
	 */
	void recordAllocationSuccessAndLatency(long latencyMs);

	/**
	 * Record a latency for failed allocation. Implies incrementing an allocation failure counter as well.
	 * @param latencyMs the latency in milliseconds
	 */
	void recordAllocationFailureAndLatency(long latencyMs);

	/**
	 * Record a latency for resetting a resource to a reusable state. Implies incrementing a counter as well.
	 * @param latencyMs the latency in milliseconds
	 */
	void recordResetLatency(long latencyMs);

	/**
	 * Record a latency for destroying a resource. Implies incrementing a counter as well.
	 * @param latencyMs the latency in milliseconds
	 */
	void recordDestroyLatency(long latencyMs);

	/**
	 * Record the fact that a resource was recycled, ie it was reset and tested for reuse.
	 */
	void recordRecycled();

	/**
	 * Record the number of milliseconds a pooled object has been live (ie time between allocation and destruction).
	 * @param millisecondsSinceAllocation the number of milliseconds since the object was allocated, at the time it is destroyed
	 */
	void recordLifetimeDuration(long millisecondsSinceAllocation);

	/**
	 * Record the number of milliseconds an object had been idle when it gets pulled from the pool and passed to a borrower.
	 * @param millisecondsIdle the number of milliseconds an object that was just acquired had previously been idle
	 */
	void recordIdleTime(long millisecondsIdle);

	/**
	 * Record the fact that a {@link Pool} has a slow path of recycling and just used it.
	 */
	void recordSlowPath();

	/**
	 * Record the fact that a {@link Pool} has a fast path of recycling and just used it.
	 */
	void recordFastPath();

	/**
	 * Record a latency for successful pending acquire operation.
	 * A successful pending acquire operation is such that triggers an allocation operation.
	 * Implies incrementing a pending acquire success counter as well.
	 * @param latencyMs the latency in milliseconds
	 * @since 1.0.4
	 */
	default void recordPendingSuccessAndLatency(long latencyMs) {
		// noop
	}

	/**
	 * Record a latency for failed pending acquire.
	 * A failed pending acquire operation is such that finishes with {@link PoolAcquireTimeoutException}.
	 * Implies incrementing a pending acquire failure counter as well.
	 * @param latencyMs the latency in milliseconds
	 * @since 1.0.4
	 */
	default void recordPendingFailureAndLatency(long latencyMs) {
		// noop
	}
}
