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

import java.time.Clock;
import java.time.Instant;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import reactor.core.publisher.Mono;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class AbstractPoolRefTest {

	private static final class NaiveRef<T> extends AbstractPool.AbstractPooledRef<T> {

		NaiveRef(T poolable, PoolMetricsRecorder metricsRecorder, Clock clock) {
			super(poolable, metricsRecorder, clock);
		}

		NaiveRef(AbstractPool.AbstractPooledRef<T> oldRef) {
			super(oldRef);
		}

		@Override
		public Mono<Void> release() {
			return Mono.empty();
		}

		@Override
		public Mono<Void> invalidate() {
			return Mono.empty();
		}
	}

	protected TestUtils.VirtualClock clock;
	protected TestUtils.InMemoryPoolMetrics poolMetrics;

	@BeforeEach
	void init() {
		//we need to differentiate with 0 sometimes
		clock = new TestUtils.VirtualClock(Instant.ofEpochMilli(1));
		poolMetrics = new TestUtils.InMemoryPoolMetrics(clock);
	}

	@Test
	void initialTimestamps() {
		NaiveRef<String> ref = new NaiveRef<>("foo", poolMetrics, clock);

		assertThat(ref.allocationTimestamp()).as("allocation timestamp").isOne();
		assertThat(ref.releaseTimestamp()).as("release timestamp").isOne();
	}

	@Test
	void releaseTimestampWhenAcquired() {
		NaiveRef<String> ref = new NaiveRef<>("foo", poolMetrics, clock);

		clock.setTimeTo(10);
		ref.markAcquired();
		clock.setTimeTo(15);

		assertThat(ref.allocationTimestamp()).as("allocation timestamp").isOne();
		assertThat(ref.releaseTimestamp()).as("release timestamp").isZero();
		assertThat(ref.idleTime()).as("idle time").isZero();
	}

	@Test
	void releaseTimestampWhenNeverAcquired() {
		NaiveRef<String> ref = new NaiveRef<>("foo", poolMetrics, clock);
		clock.setTimeTo(15);

		assertThat(ref.allocationTimestamp()).as("allocation timestamp").isOne();
		assertThat(ref.releaseTimestamp()).as("release timestamp").isOne();
		assertThat(ref.idleTime()).as("idleTime vs lifeTime").isSameAs(ref.lifeTime());
	}

	@Test
	void releaseTimeTracking() {
		NaiveRef<String> ref = new NaiveRef<>("foo", poolMetrics, clock);
		ref.markAcquired();
		clock.setTimeTo(10);
		ref.markReleased();
		clock.setTimeTo(15);

		assertThat(ref.allocationTimestamp()).as("allocation timestamp").isOne();
		assertThat(ref.releaseTimestamp()).as("release timestamp").isEqualTo(10);
		assertThat(ref.idleTime()).as("idle time").isEqualTo(5); //clock now at 15, released at 10
	}

}