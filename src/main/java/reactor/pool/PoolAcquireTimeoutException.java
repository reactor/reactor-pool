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

import java.time.Duration;
import java.util.concurrent.TimeoutException;

/**
 * A specialized {@link TimeoutException} that denotes that a {@link Pool#acquire(Duration)}
 * has timed out. Said {@link Duration} can be obtained via {@link #getAcquireTimeout()}.
 *
 * @author Simon Baslé
 */
public class PoolAcquireTimeoutException extends TimeoutException {

	private final Duration acquireTimeout;

	public PoolAcquireTimeoutException(Duration acquireTimeout) {
		super("Pool#acquire(Duration) has been pending for more than the configured timeout of " + acquireTimeout.toMillis() + "ms");
		this.acquireTimeout = acquireTimeout;
	}

	/**
	 * @return the configured acquire timeout that was just overshot
	 */
	public Duration getAcquireTimeout() {
		return acquireTimeout;
	}
}
