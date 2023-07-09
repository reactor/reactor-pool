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
 * A strategy guiding the {@link Pool} on whether or not it is possible to invoke the resource allocator.
 * <p>
 * See {@link PoolBuilder#sizeBetween(int, int)} and {@link PoolBuilder#sizeUnbounded()} for pre-made strategies.
 *
 * @author Simon Baslé
 */
public interface AllocationStrategy {

	/**
	 * Best-effort peek at the state of the strategy which indicates roughly how many more resources can currently be
	 * allocated. Should be paired with {@link #getPermits(int)} for an atomic permission.
	 *
	 * @return an ESTIMATED count of how many more resources can currently be allocated
	 */
	int estimatePermitCount();

	/**
	 * Try to get the permission to allocate a {@code desired} positive number of new resources. Returns the permissible
	 * number of resources which MUST be created (otherwise the internal live counter of the strategy might be off).
	 * This permissible number might be zero, and it can also be a greater number than {@code desired}, which could for
	 * example denote a minimum warmed-up size for the pool to maintain (see below).
	 * Once a resource is discarded from the pool, it must update the strategy using {@link #returnPermits(int)}
	 * (which can happen in batches or with value {@literal 1}).
	 * <p>
	 * For the warming up case, the typical pattern would be to call this method with a {@code desired} of zero.
	 *
	 * @param desired the desired number of new resources
	 * @return the actual number of new resources that MUST be created, can be 0 and can be more than {@code desired}
	 */
	int getPermits(int desired);

	/**
	 * @return a best estimate of the number of permits currently granted, between 0 and {@link Integer#MAX_VALUE}
	 */
	int permitGranted();

	/**
	 * Return the minimum number of permits this strategy tries to maintain granted
	 * (reflecting a minimal size for the pool), or {@code 0} for scale-to-zero.
	 *
	 * @return the minimum number of permits this strategy tries to maintain, or {@code 0}
	 */
	int permitMinimum();

	/**
	 * @return the maximum number of permits this strategy can grant in total, or {@link Integer#MAX_VALUE} for unbounded
	 */
	int permitMaximum();

	/**
	 * Update the strategy to indicate that N resources were discarded from the {@link Pool}, potentially leaving space
	 * for N new ones to be allocated. Users MUST ensure that this method isn't called with a value greater than the
	 * number of held permits it has.
	 * <p>
	 * Some strategy MIGHT throw an {@link IllegalArgumentException} if it can be determined the number of returned permits
	 * is not consistent with the strategy's limits and delivered permits.
	 */
	void returnPermits(int returned);

	/**
	 * Return the concurrency level used when the allocator is subscribed to during the warmup phase, if any.
	 * <p>
	 * The number of resources created concurrently will not exceed the value returned by {@code warmupParallelism()}.
	 * If the concurrency level is set to 1, pre-allocation of resources will be performed sequentially by subscribing to the allocator
	 * one at a time. The process waits for a resource to be created before subscribing again to the allocator.
	 * This sequence continues until all pre-allocated resources have been successfully created.
	 * <p>
	 * Defaults to 1
	 *
	 * @return The concurrency level used when the allocator is subscribed to during the warmup phase, must be positive,
	 *                          {@code 1} by default
	 * @since 1.0.1
	 */
	default int warmupParallelism() {
		return PoolBuilder.DEFAULT_WARMUP_PARALLELISM;
	}
}
