/*
 * Copyright (c) 2023 VMware Inc. or its affiliates, All Rights Reserved.
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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class InstrumentedPoolPoolMetricsTest {

	@Test
	void testIsInactiveForMoreThan() {
		InstrumentedPool.PoolMetrics metrics = new TestPool(2);

		assertThat(metrics.isInactiveForMoreThan(Duration.ofMillis(1999))).isTrue();
		assertThat(metrics.isInactiveForMoreThan(Duration.ofSeconds(2))).isTrue();
		
		assertThat(metrics.isInactiveForMoreThan(Duration.ofMillis(2001))).isFalse();
		assertThat(metrics.isInactiveForMoreThan(Duration.ofSeconds(3))).isFalse();
	}

	static class TestPool implements InstrumentedPool.PoolMetrics {

		final long inactiveSeconds;

		TestPool(long inactiveSeconds) {
			this.inactiveSeconds = inactiveSeconds;
		}

		@Override
		public int acquiredSize() {
			return 0;
		}

		@Override
		public int allocatedSize() {
			return 0;
		}

		@Override
		public int idleSize() {
			return 0;
		}

		@Override
		public int pendingAcquireSize() {
			return 0;
		}

		@Override
		public long secondsSinceLastInteraction() {
			return inactiveSeconds;
		}

		@Override
		public int getMaxAllocatedSize() {
			return 0;
		}

		@Override
		public int getMaxPendingAcquireSize() {
			return 0;
		}
	}
}
