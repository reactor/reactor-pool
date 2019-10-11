/*
 * Copyright (c) 2018-Present Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.pool;

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
		 * {@link Pool#acquire() acquired} and are in active use.
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
