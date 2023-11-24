/*
 * Copyright (c) 2019-2024 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.time.Duration;

/**
 * An {@link InstrumentedPool} is a {@link Pool} that exposes a few additional methods
 * around metrics.
 *
 * @author Simon Baslé
 */
public interface InstrumentedPool<POOLABLE> extends Pool<POOLABLE> {

	/**
	 * @return a {@link PoolMetrics} object to be used to get live gauges about the {@link Pool}
	 */
	PoolMetrics metrics();

	/**
	 * Estimates if the pool can currently either reuse or create some resources
	 * @return true if the pool can currently either reuse or create some resources, false if no idles resources are
	 *         currently available and no more resources can be currently created.
	 */
	default boolean hasAvailableResources() {
		PoolMetrics pm = metrics();
		return (pm.idleSize() + config().allocationStrategy().estimatePermitCount()) - pm.pendingAcquireSize() >= 0;
	}

	/**
	 * An object that can be used to get live information about a {@link Pool}, suitable
	 * for gauge metrics.
	 * <p>
	 * getXxx methods are configuration accessors, ie values that won't change over time,
	 * whereas other methods can be used as gauges to introspect the current state of the
	 * pool.
	 */
	interface PoolMetrics {

		/**
		 * Measure the current number of resources that have been successfully
		 * {@link Pool#acquire() acquired} and are in active use, outside of the
		 * control of the pool until they're released back to it. This number is
		 * only incremented after the resource has been successfully allocated and
		 * is about to be handed off to the subscriber of {@link Pool#acquire()}.
		 *
		 * @return the number of acquired resources
		 */
		int acquiredSize();

		/**
		 * Measure the current number of allocated resources in the {@link Pool}, acquired
		 * or idle.
		 *
		 * @return the total number of allocated resources managed by the {@link Pool}
		 */
		int allocatedSize();

		/**
		 * Measure the current number of idle resources in the {@link Pool}.
		 * <p>
		 * Note that some resources might be lazily evicted when they're next considered
		 * for an incoming {@link Pool#acquire()} call. Such resources would still count
		 * towards this method.
		 *
		 * @return the number of idle resources
		 */
		int idleSize();

		/**
		 * Measure the current number of "pending" {@link Pool#acquire() acquire Monos} in
		 * the {@link Pool}.
		 * <p>
		 * An acquire is in the pending state when it is attempted at a point when no idle
		 * resource is available in the pool, and no new resource can be created.
		 *
		 * @return the number of pending acquire
		 */
		int pendingAcquireSize();

		/**
		 * Measure the duration in seconds since the pool was last interacted with in a meaningful way.
		 * This is a best effort indicator of pool inactivity, provided the pool counters
		 * ({@link #acquiredSize()}, {@link #idleSize()}, {@link #pendingAcquireSize()} and {@link #allocatedSize()})
		 * are also at zero.
		 * <p>
		 * The lower the duration, the greater the chances that an interaction could be occurring in parallel to this call.
		 * This is why the duration is truncated to the second.
		 * A pool implementation that cannot yet support this measurement MAY choose to return {@literal -1} seconds instead.
		 * <p>
		 * Interactions include background eviction, disposal of the pool, explicit pool warmup, resource acquisition
		 * and release (in the default implementation, any interaction triggering the drain loop)...
		 *
		 * @return a number of seconds indicative of the time elapsed since last pool interaction
		 * @see #isInactiveForMoreThan(Duration)
		 */
		default long secondsSinceLastInteraction() {
			return -1L;
		}

		/**
		 * A convenience way to check the pool is inactive, in the sense that {@link #acquiredSize()},
		 * {@link #idleSize()}, {@link #pendingAcquireSize()} and {@link #allocatedSize()} are all at zero
		 * and that the last recorded interaction with the pool ({@link #secondsSinceLastInteraction()})
		 * was more than or exactly {@code duration} ago.
		 *
		 * @return true if the pool can be considered inactive (see above), false otherwise
		 * @see #secondsSinceLastInteraction()
		 */
		default boolean isInactiveForMoreThan(Duration duration) {
			return acquiredSize() == 0 && idleSize() == 0 && pendingAcquireSize() == 0 && allocatedSize() == 0
					&& !Duration.ofSeconds(secondsSinceLastInteraction()).minus(duration).isNegative();
		}

		/**
		 * Get the maximum number of live resources this {@link Pool} will allow.
		 * <p>
		 * A {@link Pool} might be unbounded, in which case this method returns {@link Integer#MAX_VALUE}.
		 *
		 * @return the maximum number of live resources that can be allocated by this {@link Pool}
		 */
		int getMaxAllocatedSize();

		/**
		 * Get the maximum number of {@link Pool#acquire()} this {@link Pool} can queue in
		 * a pending state when no available resource is immediately handy (and the {@link Pool}
		 * cannot allocate more resources).
		 * <p>
		 * A {@link Pool} pending queue might be unbounded, in which case this method returns
		 * {@link Integer#MAX_VALUE}.
		 *
		 * @return the maximum number of pending acquire that can be enqueued by this {@link Pool}
		 */
		int getMaxPendingAcquireSize();
	}
}
