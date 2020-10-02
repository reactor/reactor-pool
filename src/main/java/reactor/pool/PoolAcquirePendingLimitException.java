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
 * A {@link RuntimeException} that rejects a {@link Pool#acquire()} operation due to too
 * many similar operations being in a pending state waiting for resources to be released.
 * The configured maximum pending size for the {@link Pool} can be obtained by calling
 * {@link #getAcquirePendingLimit()}.
 *
 * @author Simon Baslé
 */
public class PoolAcquirePendingLimitException extends RuntimeException {

	private final int maxPending;

	public PoolAcquirePendingLimitException(int maxPending) {
		super("Pending acquire queue has reached its maximum size of " + maxPending);
		this.maxPending = maxPending;
	}

	public PoolAcquirePendingLimitException(int maxPending, String message) {
		super(message);
		this.maxPending = maxPending;
	}

	/**
	 * @return the configured maximum pending size for the {@link Pool}
	 */
	public int getAcquirePendingLimit() {
		return this.maxPending;
	}
}
