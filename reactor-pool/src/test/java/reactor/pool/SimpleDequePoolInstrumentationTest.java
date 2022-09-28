/*
 * Copyright (c) 2021-2022 VMware Inc. or its affiliates, All Rights Reserved.
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

import reactor.core.publisher.Mono;
import reactor.scheduler.clock.SchedulerClock;
import reactor.test.scheduler.VirtualTimeScheduler;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Simon Baslé
 */
class SimpleDequePoolInstrumentationTest {

	@Test
	void backgroundEvictionDoesntMarkInteractionWhenNothingToEvict() {
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();
		SimpleDequePool<String> pool = new SimpleDequePool<>(
				PoolBuilder.from(Mono.just("example"))
				           .evictInBackground(Duration.ofSeconds(10), vts)
				           .clock(SchedulerClock.of(vts))
				           .buildConfig(), true);

		assertThat(pool.lastInteractionTimestamp).as("initial timestamp").isZero();
		assertThat(pool.secondsSinceLastInteraction())
				.as("initial secondsSinceLastInteraction")
				.isZero();

		vts.advanceTimeBy(Duration.ofSeconds(5));

		assertThat(pool.lastInteractionTimestamp)
				.as("pre-eviction timestamp hasn't moved")
				.isEqualTo(0L);
		assertThat(pool.secondsSinceLastInteraction())
				.as("pre-eviction secondsSinceLastInteraction")
				.isEqualTo(5L);

		vts.advanceTimeBy(Duration.ofSeconds(5));

		assertThat(pool.lastInteractionTimestamp)
				.as("post-eviction timestamp hasn't moved")
				.isEqualTo(0L);
		assertThat(pool.secondsSinceLastInteraction())
				.as("post-eviction secondsSinceLastInteraction")
				.isEqualTo(10L);
	}

	@Test
	void backgroundEvictionMarksInteraction() {
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();
		SimpleDequePool<String> pool = new SimpleDequePool<>(
				PoolBuilder.from(Mono.just("example"))
				           .evictionPredicate((s, metadata) -> metadata.idleTime() > 1000)
				           .evictInBackground(Duration.ofSeconds(10), vts)
				           .clock(SchedulerClock.of(vts))
				           .buildConfig(), true);

		assertThat(pool.lastInteractionTimestamp).as("initial timestamp").isZero();
		assertThat(pool.secondsSinceLastInteraction())
				.as("initial secondsSinceLastInteraction")
				.isZero();

		PooledRef<String> pooledRef = pool.acquire().block();
		assertThat(pooledRef).isNotNull();

		pooledRef.release().block();

		vts.advanceTimeBy(Duration.ofSeconds(5));

		assertThat(pool.lastInteractionTimestamp)
				.as("pre-eviction timestamp hasn't moved")
				.isEqualTo(0L);
		assertThat(pool.secondsSinceLastInteraction())
				.as("pre-eviction secondsSinceLastInteraction")
				.isEqualTo(5L);

		vts.advanceTimeBy(Duration.ofSeconds(5));

		assertThat(pool.lastInteractionTimestamp)
				.as("post-eviction timestamp")
				.isEqualTo(10_000L);
		assertThat(pool.secondsSinceLastInteraction())
				.as("post-eviction secondsSinceLastInteraction")
				.isZero();
	}

	@Test
	void warmupLazilyMarksInteraction() {
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();
		SimpleDequePool<String> pool = new SimpleDequePool<>(
				PoolBuilder.from(Mono.just("example"))
				           .sizeBetween(5, 10)
				           .clock(SchedulerClock.of(vts))
				           .buildConfig(), true);

		assertThat(pool.lastInteractionTimestamp).as("initial timestamp").isZero();
		assertThat(pool.secondsSinceLastInteraction())
				.as("initial secondsSinceLastInteraction")
				.isZero();

		vts.advanceTimeBy(Duration.ofSeconds(5));
		Mono<Integer> warmupMono = pool.warmup();

		assertThat(pool.lastInteractionTimestamp)
				.as("warmupMono timestamp hasn't moved")
				.isEqualTo(0L);
		assertThat(pool.secondsSinceLastInteraction())
				.as("warmupMono secondsSinceLastInteraction")
				.isEqualTo(5L);

		vts.advanceTimeBy(Duration.ofSeconds(5));
		warmupMono.block();

		assertThat(pool.lastInteractionTimestamp)
				.as("subscribed warmupMono timestamp")
				.isEqualTo(10_000L);
		assertThat(pool.secondsSinceLastInteraction())
				.as("subscribed warmupMono secondsSinceLastInteraction")
				.isZero();
	}

	@Test
	void acquireLazilyMarksInteraction() {
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();
		SimpleDequePool<String> pool = new SimpleDequePool<>(
				PoolBuilder.from(Mono.just("example"))
				           .clock(SchedulerClock.of(vts))
				           .buildConfig(), true);

		assertThat(pool.lastInteractionTimestamp).as("initial timestamp").isZero();
		assertThat(pool.secondsSinceLastInteraction())
				.as("initial secondsSinceLastInteraction")
				.isZero();

		vts.advanceTimeBy(Duration.ofSeconds(5));
		Mono<PooledRef<String>> acquireMono = pool.acquire();

		assertThat(pool.lastInteractionTimestamp)
				.as("acquireMono timestamp hasn't moved")
				.isEqualTo(0L);
		assertThat(pool.secondsSinceLastInteraction())
				.as("acquireMono secondsSinceLastInteraction")
				.isEqualTo(5L);

		vts.advanceTimeBy(Duration.ofSeconds(5));
		acquireMono.block();

		assertThat(pool.lastInteractionTimestamp)
				.as("subscribed acquireMono timestamp")
				.isEqualTo(10_000L);
		assertThat(pool.secondsSinceLastInteraction())
				.as("subscribed acquireMono secondsSinceLastInteraction")
				.isZero();
	}

	@Test
	void releaseLazilyMarksInteraction() {
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();
		SimpleDequePool<String> pool = new SimpleDequePool<>(
				PoolBuilder.from(Mono.just("example"))
				           .clock(SchedulerClock.of(vts))
				           .buildConfig(), true);

		PooledRef<String> ref = pool.acquire().block();
		assert ref != null;

		assertThat(pool.lastInteractionTimestamp).as("initial timestamp").isZero();
		assertThat(pool.secondsSinceLastInteraction())
				.as("initial secondsSinceLastInteraction")
				.isZero();

		vts.advanceTimeBy(Duration.ofSeconds(5));
		Mono<Void> releaseMono = ref.release();

		assertThat(pool.lastInteractionTimestamp)
				.as("releaseMono timestamp hasn't moved")
				.isEqualTo(0L);
		assertThat(pool.secondsSinceLastInteraction())
				.as("releaseMono secondsSinceLastInteraction")
				.isEqualTo(5L);

		vts.advanceTimeBy(Duration.ofSeconds(5));
		releaseMono.block();

		assertThat(pool.lastInteractionTimestamp)
				.as("subscribed releaseMono timestamp")
				.isEqualTo(10_000L);
		assertThat(pool.secondsSinceLastInteraction())
				.as("subscribed releaseMono secondsSinceLastInteraction")
				.isZero();
	}

	@Test
	void invalidateLazilyMarksInteraction() {
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();
		SimpleDequePool<String> pool = new SimpleDequePool<>(
				PoolBuilder.from(Mono.just("example"))
				           .clock(SchedulerClock.of(vts))
				           .buildConfig(), true);

		PooledRef<String> ref = pool.acquire().block();
		assert ref != null;

		assertThat(pool.lastInteractionTimestamp).as("initial timestamp").isZero();
		assertThat(pool.secondsSinceLastInteraction())
				.as("initial secondsSinceLastInteraction")
				.isZero();

		vts.advanceTimeBy(Duration.ofSeconds(5));
		Mono<Void> invalidateMono = ref.invalidate();

		assertThat(pool.lastInteractionTimestamp)
				.as("invalidateMono timestamp hasn't moved")
				.isEqualTo(0L);
		assertThat(pool.secondsSinceLastInteraction())
				.as("invalidateMono secondsSinceLastInteraction")
				.isEqualTo(5L);

		vts.advanceTimeBy(Duration.ofSeconds(5));
		invalidateMono.block();

		assertThat(pool.lastInteractionTimestamp)
				.as("subscribed invalidateMono timestamp")
				.isEqualTo(10_000L);
		assertThat(pool.secondsSinceLastInteraction())
				.as("subscribed invalidateMono secondsSinceLastInteraction")
				.isZero();
	}

	@Test
	void disposeLaterLazilyMarksInteraction() {
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();
		SimpleDequePool<String> pool = new SimpleDequePool<>(
				PoolBuilder.from(Mono.just("example"))
				           .clock(SchedulerClock.of(vts))
				           .buildConfig(), true);

		assertThat(pool.lastInteractionTimestamp).as("initial timestamp").isZero();
		assertThat(pool.secondsSinceLastInteraction())
				.as("initial secondsSinceLastInteraction")
				.isZero();

		vts.advanceTimeBy(Duration.ofSeconds(5));
		Mono<Void> disposePoolMono = pool.disposeLater();

		assertThat(pool.lastInteractionTimestamp)
				.as("disposePoolMono timestamp hasn't moved")
				.isEqualTo(0L);
		assertThat(pool.secondsSinceLastInteraction())
				.as("disposePoolMono secondsSinceLastInteraction")
				.isEqualTo(5L);

		vts.advanceTimeBy(Duration.ofSeconds(5));
		disposePoolMono.block();

		assertThat(pool.lastInteractionTimestamp)
				.as("subscribed disposePoolMono timestamp")
				.isEqualTo(10_000L);
		assertThat(pool.secondsSinceLastInteraction())
				.as("subscribed disposePoolMono secondsSinceLastInteraction")
				.isZero();
	}

	@Test
	void secondsSinceLastInteractionTruncates() {
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();
		SimpleDequePool<String> pool = new SimpleDequePool<>(
				PoolBuilder.from(Mono.just("example"))
				           .clock(SchedulerClock.of(vts))
				           .buildConfig(), true);

		vts.advanceTimeBy(Duration.ofMinutes(3).plusMillis(543));

		assertThat(pool.secondsSinceLastInteraction())
				.as("duration reported truncated to seconds")
				.isEqualTo(3 * 60L);
	}

	@Test
	void timestampRecordedWithMsPrecision() {
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();
		SimpleDequePool<String> pool = new SimpleDequePool<>(
				PoolBuilder.from(Mono.just("example"))
				           .clock(SchedulerClock.of(vts))
				           .buildConfig(), true);

		vts.advanceTimeBy(Duration.ofMinutes(3).plusMillis(543));
		pool.recordInteractionTimestamp();

		assertThat(pool.lastInteractionTimestamp)
				.as("timestamp recorded with ms precision")
				.isEqualTo(3 * 60 * 1000 + 543);
	}

	@Test
	void isInactiveForMoreThanShortCircuitsOnPositiveCounters() {
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();
		SimpleDequePool<String> pool = new SimpleDequePool<>(
				PoolBuilder.from(Mono.just("example"))
				           .clock(SchedulerClock.of(vts))
				           .buildConfig(), true);

		pool.acquire().block();

		vts.advanceTimeBy(Duration.ofMinutes(3).plusMillis(543));

		assertThat(pool.acquiredSize() + pool.idleSize() + pool.pendingAcquireSize() + pool.allocatedSize())
				.as("smoke test counters > 0")
				.isPositive();

		assertThat(pool.isInactiveForMoreThan(Duration.ofSeconds(1)))
				.as("isInactiveForMoreThan(1s)")
				.isFalse();
	}

	@Test
	void isInactiveForMoreThanDoesntTruncateParameter() {
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();
		SimpleDequePool<String> pool = new SimpleDequePool<>(
				PoolBuilder.from(Mono.just("example"))
				           .clock(SchedulerClock.of(vts))
				           .buildConfig(), true);

		vts.advanceTimeBy(Duration.ofMinutes(3).plusMillis(543));
		pool.recordInteractionTimestamp();

		assertThat(pool.acquiredSize() + pool.idleSize() + pool.pendingAcquireSize() + pool.allocatedSize())
				.as("smoke test counters == 0")
				.isZero();

		//if 123ms was truncated to seconds to, we'd get inactive(0s) == duration(0s)
		assertThat(pool.isInactiveForMoreThan(Duration.ofMillis(123)))
				.as("isInactiveForMoreThan(123ms)")
				.isTrue();
	}

	@Test
	void testIsInactiveForMoreThan() {
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();
		SimpleDequePool<String> pool = new SimpleDequePool<>(
				PoolBuilder.from(Mono.just("example"))
				           .clock(SchedulerClock.of(vts))
				           .buildConfig(), true);

		pool.recordInteractionTimestamp();

		vts.advanceTimeBy(Duration.ofMinutes(3).plusMillis(543));

		assertThat(pool.acquiredSize() + pool.idleSize() + pool.pendingAcquireSize() + pool.allocatedSize())
				.as("smoke test counters == 0")
				.isZero();

		assertThat(pool.isInactiveForMoreThan(Duration.ofSeconds(5)))
				.as("isInactiveForMoreThan(5s)")
				.isTrue();
	}

}
