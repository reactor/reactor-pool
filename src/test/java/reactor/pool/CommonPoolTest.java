/*
 * Copyright (c) 2018-Present Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.pool;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.FormatterClosedException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.pool.TestUtils.PoolableTest;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.test.util.TestLogger;
import reactor.util.Loggers;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

/**
 * @author Simon Baslé
 */
public class CommonPoolTest {

	static final <T> Function<PoolBuilder<T>, AbstractPool<T>> affinityPoolFifo() {
		return new Function<PoolBuilder<T>, AbstractPool<T>>() {
			@Override
			public AbstractPool<T> apply(PoolBuilder<T> builder) {
				return (AbstractPool<T>) builder.threadAffinity(true)
				                                .build();
			}

			@Override
			public String toString() {
				return "affinityPool FIFO";
			}
		};
	}

	static final <T> Function<PoolBuilder<T>, AbstractPool<T>> simplePoolFifo() {
		return new Function<PoolBuilder<T>, AbstractPool<T>>() {
			@Override
			public AbstractPool<T> apply(PoolBuilder<T> builder) {
				return (AbstractPool<T>) builder.threadAffinity(false)
				                                .build();
			}

			@Override
			public String toString() {
				return "simplePool FIFO";
			}
		};
	}

	static <T> List<Function<PoolBuilder<T>, AbstractPool<T>>> allPools() {
		return Arrays.asList(simplePoolFifo(), affinityPoolFifo());
	}

	static <T> List<Function<PoolBuilder<T>, AbstractPool<T>>> fifoPools() {
		return Arrays.asList(simplePoolFifo(), affinityPoolFifo());
	}

	@ParameterizedTest
	@MethodSource("fifoPools")
	void smokeTestFifo(Function<PoolBuilder<PoolableTest>, Pool<PoolableTest>> configAdjuster) throws InterruptedException {
		AtomicInteger newCount = new AtomicInteger();

		PoolBuilder<PoolableTest> builder = PoolBuilder
				.from(Mono.defer(() -> Mono.just(new PoolableTest(newCount.incrementAndGet()))))
				.sizeMax(3)
				.releaseHandler(pt -> Mono.fromRunnable(pt::clean))
				.evictionPredicate(slot -> !slot.poolable().isHealthy());

		Pool<PoolableTest> pool = configAdjuster.apply(builder);
		assertThat(pool.growIdle(2).block()).as("initial size 2").isEqualTo(2);


		List<PooledRef<PoolableTest>> acquired1 = new ArrayList<>();
		pool.acquire().subscribe(acquired1::add);
		pool.acquire().subscribe(acquired1::add);
		pool.acquire().subscribe(acquired1::add);
		List<PooledRef<PoolableTest>> acquired2 = new ArrayList<>();
		pool.acquire().subscribe(acquired2::add);
		pool.acquire().subscribe(acquired2::add);
		pool.acquire().subscribe(acquired2::add);
		List<PooledRef<PoolableTest>> acquired3 = new ArrayList<>();
		pool.acquire().subscribe(acquired3::add);
		pool.acquire().subscribe(acquired3::add);
		pool.acquire().subscribe(acquired3::add);

		assertThat(acquired1).hasSize(3);
		assertThat(acquired2).isEmpty();
		assertThat(acquired3).isEmpty();

		Thread.sleep(1000);
		for (PooledRef<PoolableTest> slot : acquired1) {
			slot.release().block();
		}
		assertThat(acquired2).hasSize(3);
		assertThat(acquired3).isEmpty();

		Thread.sleep(1000);
		for (PooledRef<PoolableTest> slot : acquired2) {
			slot.release().block();
		}
		assertThat(acquired3).hasSize(3);

		assertThat(acquired1)
				.as("acquired1/2 all used up")
				.hasSameElementsAs(acquired2)
				.allSatisfy(slot -> assertThat(slot.poolable().usedUp).isEqualTo(2));

		assertThat(acquired3)
				.as("acquired3 all new")
				.allSatisfy(slot -> assertThat(slot.poolable().usedUp).isZero());
	}

	@ParameterizedTest
	@MethodSource("fifoPools")
	void smokeTestInScopeFifo(Function<PoolBuilder<PoolableTest>, Pool<PoolableTest>> configAdjuster) throws InterruptedException {
		AtomicInteger newCount = new AtomicInteger();

		PoolBuilder<PoolableTest> builder = PoolBuilder
				.from(Mono.defer(() -> Mono.just(new PoolableTest(newCount.incrementAndGet()))))
				.sizeMax(3)
				.releaseHandler(pt -> Mono.fromRunnable(pt::clean))
				.evictionPredicate(slot -> !slot.poolable().isHealthy());
		Pool<PoolableTest> pool = configAdjuster.apply(builder);
		assertThat(pool.growIdle(2).block()).as("initial size 2").isEqualTo(2);

		TestPublisher<Integer> trigger1 = TestPublisher.create();
		TestPublisher<Integer> trigger2 = TestPublisher.create();
		TestPublisher<Integer> trigger3 = TestPublisher.create();

		List<PoolableTest> acquired1 = new ArrayList<>();

		Mono.when(
				pool.acquireInScope(mono -> mono.doOnNext(acquired1::add).delayUntil(__ -> trigger1)),
				pool.acquireInScope(mono -> mono.doOnNext(acquired1::add).delayUntil(__ -> trigger1)),
				pool.acquireInScope(mono -> mono.doOnNext(acquired1::add).delayUntil(__ -> trigger1))
		).subscribe();

		List<PoolableTest> acquired2 = new ArrayList<>();
		Mono.when(
				pool.acquireInScope(mono -> mono.log().doOnNext(acquired2::add).delayUntil(__ -> trigger2)),
				pool.acquireInScope(mono -> mono.doOnNext(acquired2::add).delayUntil(__ -> trigger2)),
				pool.acquireInScope(mono -> mono.doOnNext(acquired2::add).delayUntil(__ -> trigger2))
		).subscribe();

		List<PoolableTest> acquired3 = new ArrayList<>();
		Mono.when(
				pool.acquireInScope(mono -> mono.doOnNext(acquired3::add).delayUntil(__ -> trigger3)),
				pool.acquireInScope(mono -> mono.doOnNext(acquired3::add).delayUntil(__ -> trigger3)),
				pool.acquireInScope(mono -> mono.doOnNext(acquired3::add).delayUntil(__ -> trigger3))
		).subscribe();

		assertThat(acquired1).as("first batch not pending").hasSize(3);
		assertThat(acquired2).as("second and third pending").hasSameSizeAs(acquired3).isEmpty();

		trigger1.emit(1);

		assertThat(acquired2).as("batch2 after trigger1").hasSize(3);
		assertThat(acquired3).as("batch3 after trigger1").isEmpty();

		trigger2.emit(1);

		assertThat(acquired3).as("batch3 after trigger2").hasSize(3);
		assertThat(newCount).as("allocated total").hasValue(6);

		assertThat(acquired1)
				.as("acquired1/2 all used up")
				.hasSameElementsAs(acquired2)
				.allSatisfy(elem -> assertThat(elem.usedUp).isEqualTo(2));

		assertThat(acquired3)
				.as("acquired3 all new (released once)")
				.allSatisfy(elem -> assertThat(elem.usedUp).isZero());
	}

	@ParameterizedTest
	@MethodSource("fifoPools")
	void smokeTestAsyncFifo(Function<PoolBuilder<PoolableTest>, Pool<PoolableTest>> configAdjuster) throws InterruptedException {
		AtomicInteger newCount = new AtomicInteger();

		PoolBuilder<PoolableTest> builder = PoolBuilder
				.from(Mono.defer(() -> Mono.just(new PoolableTest(newCount.incrementAndGet())))
				          .subscribeOn(Schedulers.newParallel("poolable test allocator")))
				.sizeMax(3)
				.releaseHandler(pt -> Mono.fromRunnable(pt::clean))
				.evictionPredicate(slot -> !slot.poolable().isHealthy());

		Pool<PoolableTest> pool = configAdjuster.apply(builder);
		assertThat(pool.growIdle(2).block()).as("initial size 2").isEqualTo(2);

		List<PooledRef<PoolableTest>> acquired1 = new ArrayList<>();
		CountDownLatch latch1 = new CountDownLatch(3);
		pool.acquire().subscribe(acquired1::add, Throwable::printStackTrace, latch1::countDown);
		pool.acquire().subscribe(acquired1::add, Throwable::printStackTrace, latch1::countDown);
		pool.acquire().subscribe(acquired1::add, Throwable::printStackTrace, latch1::countDown);

		List<PooledRef<PoolableTest>> acquired2 = new ArrayList<>();
		pool.acquire().subscribe(acquired2::add);
		pool.acquire().subscribe(acquired2::add);
		pool.acquire().subscribe(acquired2::add);

		List<PooledRef<PoolableTest>> acquired3 = new ArrayList<>();
		CountDownLatch latch3 = new CountDownLatch(3);
		pool.acquire().subscribe(acquired3::add, Throwable::printStackTrace, latch3::countDown);
		pool.acquire().subscribe(acquired3::add, Throwable::printStackTrace, latch3::countDown);
		pool.acquire().subscribe(acquired3::add, Throwable::printStackTrace, latch3::countDown);

		if (!latch1.await(1, TimeUnit.SECONDS)) { //wait for creation of max elements
			fail("not enough elements created initially, missing " + latch1.getCount());
		}
		assertThat(acquired1).hasSize(3);
		assertThat(acquired2).isEmpty();
		assertThat(acquired3).isEmpty();

		Thread.sleep(1000);
		for (PooledRef<PoolableTest> slot : acquired1) {
			slot.release().block();
		}
		assertThat(acquired2).hasSize(3);
		assertThat(acquired3).isEmpty();

		Thread.sleep(1000);
		for (PooledRef<PoolableTest> slot : acquired2) {
			slot.release().block();
		}

		if (latch3.await(5, TimeUnit.SECONDS)) { //wait for the re-creation of max elements

			assertThat(acquired3).hasSize(3);

			assertThat(acquired1)
					.as("acquired1/2 all used up")
					.hasSameElementsAs(acquired2)
					.allSatisfy(slot -> assertThat(slot.poolable().usedUp).isEqualTo(2));

			assertThat(acquired3)
					.as("acquired3 all new")
					.allSatisfy(slot -> assertThat(slot.poolable().usedUp).isZero());
		}
		else {
			fail("not enough new elements generated, missing " + latch3.getCount());
		}
	}

	@ParameterizedTest
	@MethodSource("allPools")
	void returnedReleasedIfBorrowerCancelled(Function<PoolBuilder<PoolableTest>, Pool<PoolableTest>> configAdjuster) {
		AtomicInteger releasedCount = new AtomicInteger();

		PoolBuilder<PoolableTest> builder = PoolBuilder
				.from(Mono.fromCallable(PoolableTest::new))
				.sizeMax(1)
				.releaseHandler(poolableTest -> Mono.fromRunnable(() -> {
					poolableTest.clean();
					releasedCount.incrementAndGet();
				}))
				.evictionPredicate(slot -> !slot.poolable().isHealthy());

		Pool<PoolableTest> pool = configAdjuster.apply(builder);
		assertThat(pool.growIdle(1).block()).as("initial size 1").isEqualTo(1);

		//acquire the only element
		PooledRef<PoolableTest> slot = pool.acquire().block();
		assertThat(slot).isNotNull();

		pool.acquire().subscribe().dispose();

		assertThat(releasedCount).as("before returning").hasValue(0);

		//release the element, which should forward to the cancelled second acquire, itself also cleaning
		slot.release().block();

		assertThat(releasedCount).as("after returning").hasValue(2);
	}


	@ParameterizedTest
	@MethodSource("allPools")
	void returnedReleasedIfBorrowerInScopeCancelled(Function<PoolBuilder<PoolableTest>, Pool<PoolableTest>> configAdjuster) {
		AtomicInteger releasedCount = new AtomicInteger();
		PoolBuilder<PoolableTest> builder =
				PoolBuilder.from(Mono.fromCallable(PoolableTest::new))
				           .sizeMax(1)
				           .releaseHandler(poolableTest -> Mono.fromRunnable(() -> {
					           poolableTest.clean();
					           releasedCount.incrementAndGet();
				           }))
				           .evictionPredicate(slot -> !slot.poolable().isHealthy());
		Pool<PoolableTest> pool = configAdjuster.apply(builder);
		assertThat(pool.growIdle(1).block()).as("initial size 1").isEqualTo(1);

		//acquire the only element
		PooledRef<PoolableTest> slot = pool.acquire().block();
		assertThat(slot).isNotNull();

		pool.acquireInScope(mono -> mono).subscribe().dispose();

		assertThat(releasedCount).as("before returning").hasValue(0);

		//release the element, which should forward to the cancelled second acquire, itself also cleaning
		slot.release().block();

		assertThat(releasedCount).as("after returning").hasValue(2);
	}


	@ParameterizedTest
	@MethodSource("allPools")
	void allocatedReleasedIfBorrowerCancelled(Function<PoolBuilder<PoolableTest>, Pool<PoolableTest>> configAdjuster) {
		Scheduler scheduler = Schedulers.newParallel("poolable test allocator");
		AtomicInteger newCount = new AtomicInteger();
		AtomicInteger releasedCount = new AtomicInteger();

		PoolBuilder<PoolableTest> builder = PoolBuilder
				.from(Mono.defer(() -> Mono.delay(Duration.ofMillis(50)).thenReturn(new PoolableTest(newCount.incrementAndGet())))
				          .subscribeOn(scheduler))
				.sizeMax(1)
				.releaseHandler(poolableTest -> Mono.fromRunnable(() -> {
					poolableTest.clean();
					releasedCount.incrementAndGet();
				}))
				.evictionPredicate(slot -> !slot.poolable().isHealthy());

		Pool<PoolableTest> pool = configAdjuster.apply(builder);

		//acquire the only element and immediately dispose
		pool.acquire().subscribe().dispose();

		//release due to cancel is async, give it a bit of time
		await()
				.atMost(100, TimeUnit.MILLISECONDS)
				.with().pollInterval(10, TimeUnit.MILLISECONDS)
				.untilAsserted(
						() -> assertThat(releasedCount).as("released").hasValue(1));

		assertThat(newCount).as("created").hasValue(1);
	}


	@ParameterizedTest
	@MethodSource("allPools")
	void allocatedReleasedIfBorrowerInScopeCancelled(Function<PoolBuilder<PoolableTest>, Pool<PoolableTest>> configAdjuster) {
		Scheduler scheduler = Schedulers.newParallel("poolable test allocator");
		AtomicInteger newCount = new AtomicInteger();
		AtomicInteger releasedCount = new AtomicInteger();

		PoolBuilder<PoolableTest> builder =
				PoolBuilder.from(Mono.defer(() -> Mono.delay(Duration.ofMillis(50)).thenReturn(new PoolableTest(newCount.incrementAndGet())))
				                     .subscribeOn(scheduler))
				           .sizeMax(1)
				           .releaseHandler(poolableTest -> Mono.fromRunnable(() -> {
					           poolableTest.clean();
					           releasedCount.incrementAndGet();
				           }))
				           .evictionPredicate(slot -> !slot.poolable().isHealthy());
		Pool<PoolableTest> pool = configAdjuster.apply(builder);

		//acquire the only element and immediately dispose
		pool.acquireInScope(mono -> mono).subscribe().dispose();

		//release due to cancel is async, give it a bit of time
		await()
				.atMost(100, TimeUnit.MILLISECONDS)
				.with().pollInterval(10, TimeUnit.MILLISECONDS)
				.untilAsserted(
						() -> assertThat(releasedCount).as("released").hasValue(1));

		assertThat(newCount).as("created").hasValue(1);
	}

	@ParameterizedTest
	@MethodSource("allPools")
	void cleanerFunctionError(Function<PoolBuilder<PoolableTest>, Pool<PoolableTest>> configAdjuster) {
		PoolBuilder<PoolableTest> builder = PoolBuilder
				.from(Mono.fromCallable(PoolableTest::new))
				.sizeMax(1)
				.releaseHandler(poolableTest -> Mono.fromRunnable(() -> {
					poolableTest.clean();
					throw new IllegalStateException("boom");
				}))
				.evictionPredicate(slot -> !slot.poolable().isHealthy());

		Pool<PoolableTest> pool = configAdjuster.apply(builder);

		PooledRef<PoolableTest> slot = pool.acquire().block();

		assertThat(slot).isNotNull();

		StepVerifier.create(slot.release())
		            .verifyErrorMessage("boom");
	}

	@ParameterizedTest
	@MethodSource("allPools")
	void cleanerFunctionErrorDiscards(Function<PoolBuilder<PoolableTest>, Pool<PoolableTest>> configAdjuster) {
		PoolBuilder<PoolableTest> builder = PoolBuilder
				.from(Mono.fromCallable(PoolableTest::new))
				.sizeMax(1)
				.releaseHandler(poolableTest -> Mono.fromRunnable(() -> {
					poolableTest.clean();
					throw new IllegalStateException("boom");
				}))
				.evictionPredicate(slot -> !slot.poolable().isHealthy());

		Pool<PoolableTest> pool = configAdjuster.apply(builder);

		PooledRef<PoolableTest> slot = pool.acquire().block();

		assertThat(slot).isNotNull();

		StepVerifier.create(slot.release())
		            .verifyErrorMessage("boom");

		assertThat(slot.poolable().discarded).as("discarded despite cleaner error").isEqualTo(1);
	}


	@ParameterizedTest
	@MethodSource("allPools")
	void disposingPoolDisposesElements(Function<PoolBuilder<PoolableTest>, AbstractPool<PoolableTest>> configAdjuster) {
		AtomicInteger cleanerCount = new AtomicInteger();

		PoolBuilder<PoolableTest> builder = PoolBuilder
				.from(Mono.fromCallable(PoolableTest::new))
				.sizeMax(3)
				.releaseHandler(p -> Mono.fromRunnable(cleanerCount::incrementAndGet))
				.evictionPredicate(slot -> !slot.poolable().isHealthy());

		AbstractPool<PoolableTest> pool = configAdjuster.apply(builder);

		PoolableTest pt1 = new PoolableTest(1);
		PoolableTest pt2 = new PoolableTest(2);
		PoolableTest pt3 = new PoolableTest(3);

		pool.elementOffer(pt1);
		pool.elementOffer(pt2);
		pool.elementOffer(pt3);

		pool.dispose();

		assertThat(pool.idleSize()).as("idleSize").isZero();
		assertThat(cleanerCount).as("recycled elements").hasValue(0);
		assertThat(pt1.isDisposed()).as("pt1 disposed").isTrue();
		assertThat(pt2.isDisposed()).as("pt2 disposed").isTrue();
		assertThat(pt3.isDisposed()).as("pt3 disposed").isTrue();
	}

	@ParameterizedTest
	@MethodSource("allPools")
	void disposingPoolFailsPendingBorrowers(Function<PoolBuilder<PoolableTest>, AbstractPool<PoolableTest>> configAdjuster) {
		AtomicInteger cleanerCount = new AtomicInteger();
		PoolBuilder<PoolableTest> builder = PoolBuilder
				.from(Mono.fromCallable(PoolableTest::new))
				.sizeMax(3)
				.releaseHandler(p -> Mono.fromRunnable(cleanerCount::incrementAndGet))
				.evictionPredicate(slot -> !slot.poolable().isHealthy());

		AbstractPool<PoolableTest> pool = configAdjuster.apply(builder);
		assertThat(pool.growIdle(3).block()).as("initial size 3").isEqualTo(3);

		PooledRef<PoolableTest> slot1 = pool.acquire().block();
		PooledRef<PoolableTest> slot2 = pool.acquire().block();
		PooledRef<PoolableTest> slot3 = pool.acquire().block();
		assertThat(slot1).as("slot1").isNotNull();
		assertThat(slot2).as("slot2").isNotNull();
		assertThat(slot3).as("slot3").isNotNull();

		PoolableTest acquired1 = slot1.poolable();
		PoolableTest acquired2 = slot2.poolable();
		PoolableTest acquired3 = slot3.poolable();


		AtomicReference<Throwable> borrowerError = new AtomicReference<>();
		Mono<PooledRef<PoolableTest>> pendingBorrower = pool.acquire();
		pendingBorrower.subscribe(v -> fail("unexpected value " + v),
				borrowerError::set);

		pool.dispose();

		assertThat(pool.idleSize()).as("idleSize").isZero();
		assertThat(cleanerCount).as("recycled elements").hasValue(0);
		assertThat(acquired1.isDisposed()).as("acquired1 held").isFalse();
		assertThat(acquired2.isDisposed()).as("acquired2 held").isFalse();
		assertThat(acquired3.isDisposed()).as("acquired3 held").isFalse();
		assertThat(borrowerError.get()).hasMessage("Pool has been shut down");
	}

	@ParameterizedTest
	@MethodSource("allPools")
	void releasingToDisposedPoolDisposesElement(Function<PoolBuilder<PoolableTest>, AbstractPool<PoolableTest>> configAdjuster) {
		AtomicInteger cleanerCount = new AtomicInteger();
		PoolBuilder<PoolableTest> builder = PoolBuilder
				.from(Mono.fromCallable(PoolableTest::new))
				.sizeMax(3)
				.releaseHandler(p -> Mono.fromRunnable(cleanerCount::incrementAndGet))
				.evictionPredicate(slot -> !slot.poolable().isHealthy());
		AbstractPool<PoolableTest> pool = configAdjuster.apply(builder);
		assertThat(pool.growIdle(3).block()).as("initial size 3").isEqualTo(3);

		PooledRef<PoolableTest> slot1 = pool.acquire().block();
		PooledRef<PoolableTest> slot2 = pool.acquire().block();
		PooledRef<PoolableTest> slot3 = pool.acquire().block();

		assertThat(slot1).as("slot1").isNotNull();
		assertThat(slot2).as("slot2").isNotNull();
		assertThat(slot3).as("slot3").isNotNull();

		pool.dispose();

		assertThat(pool.idleSize()).as("idleSize").isZero();

		slot1.release().block();
		slot2.release().block();
		slot3.release().block();

		assertThat(cleanerCount).as("recycled elements").hasValue(0);
		assertThat(slot1.poolable().isDisposed()).as("acquired1 disposed").isTrue();
		assertThat(slot2.poolable().isDisposed()).as("acquired2 disposed").isTrue();
		assertThat(slot3.poolable().isDisposed()).as("acquired3 disposed").isTrue();
	}

	@ParameterizedTest
	@MethodSource("allPools")
	void acquiringFromDisposedPoolFailsBorrower(Function<PoolBuilder<PoolableTest>, AbstractPool<PoolableTest>> configAdjuster) {
		AtomicInteger cleanerCount = new AtomicInteger();
		PoolBuilder<PoolableTest> builder = PoolBuilder
				.from(Mono.fromCallable(PoolableTest::new))
				.sizeMax(3)
				.releaseHandler(p -> Mono.fromRunnable(cleanerCount::incrementAndGet))
				.evictionPredicate(slot -> !slot.poolable().isHealthy());
		AbstractPool<PoolableTest> pool = configAdjuster.apply(builder);

		assertThat(pool.idleSize()).as("idleSize").isZero();

		pool.dispose();

		StepVerifier.create(pool.acquire())
		            .verifyErrorMessage("Pool has been shut down");

		assertThat(cleanerCount).as("recycled elements").hasValue(0);
	}

	@ParameterizedTest
	@MethodSource("allPools")
	void poolIsDisposed(Function<PoolBuilder<PoolableTest>, AbstractPool<PoolableTest>> configAdjuster) {
		PoolBuilder<PoolableTest> builder = PoolBuilder
				.from(Mono.fromCallable(PoolableTest::new))
				.sizeMax(3)
				.evictionPredicate(slot -> !slot.poolable().isHealthy());
		AbstractPool<PoolableTest> pool = configAdjuster.apply(builder);

		assertThat(pool.isDisposed()).as("not yet disposed").isFalse();

		pool.dispose();

		assertThat(pool.isDisposed()).as("disposed").isTrue();
	}

	@ParameterizedTest
	@MethodSource("allPools")
	void disposingPoolClosesCloseable(Function<PoolBuilder<Formatter>, AbstractPool<Formatter>> configAdjuster) {
		Formatter uniqueElement = new Formatter();

		PoolBuilder<Formatter> builder = PoolBuilder
				.from(Mono.just(uniqueElement))
				.sizeMax(1)
				.evictionPredicate(slot -> true);
		AbstractPool<Formatter> pool = configAdjuster.apply(builder);
		assertThat(pool.growIdle(1).block()).as("initial size 1").isEqualTo(1);

		pool.dispose();

		assertThatExceptionOfType(FormatterClosedException.class)
				.isThrownBy(uniqueElement::flush);
	}

	@ParameterizedTest
	@MethodSource("allPools")
	void allocatorErrorInAcquireIsPropagated(Function<PoolBuilder<String>, AbstractPool<String>> configAdjuster) {
		PoolBuilder<String> builder = PoolBuilder
				.from(Mono.<String>error(new IllegalStateException("boom")))
				.sizeMax(1)
				.evictionPredicate(f -> true);
		AbstractPool<String> pool = configAdjuster.apply(builder);

		StepVerifier.create(pool.acquire())
		            .verifyErrorSatisfies(e -> assertThat(e)
				            .isInstanceOf(IllegalStateException.class)
				            .hasMessage("boom"));
	}

	@ParameterizedTest
	@MethodSource("allPools")
	void allocatorErrorInGrowIdleIsThrown(Function<PoolBuilder<Object>, AbstractPool<Object>> configAdjuster) {
		final PoolBuilder<Object> builder = PoolBuilder
				.from(Mono.error(new IllegalStateException("boom")))
				.sizeMax(1)
				.evictionPredicate(f -> true);
		AbstractPool<Object> pool = configAdjuster.apply(builder);

		StepVerifier.create(pool.growIdle(1))
		            .verifyErrorSatisfies(e -> assertThat(e)
				            .isInstanceOf(IllegalStateException.class)
				            .hasMessage("boom"));
	}

	@ParameterizedTest
	@MethodSource("allPools")
	void discardCloseableWhenCloseFailureLogs(Function<PoolBuilder<Closeable>, AbstractPool<Closeable>> configAdjuster) {
		TestLogger testLogger = new TestLogger();
		Loggers.useCustomLoggers(it -> testLogger);
		try {
			Closeable closeable = () -> {
				throw new IOException("boom");
			};

			PoolBuilder<Closeable> builder = PoolBuilder
					.from(Mono.just(closeable))
					.sizeMax(1)
					.evictionPredicate(f -> true);
			AbstractPool<Closeable> pool = configAdjuster.apply(builder);
			assertThat(pool.growIdle(1).block()).as("initial size 1").isEqualTo(1);

			pool.dispose();

			assertThat(testLogger.getOutContent())
					.contains("Failure while discarding a released Poolable that is Closeable, could not close - java.io.IOException: boom");
		}
		finally {
			Loggers.resetLoggerFactory();
		}
	}

	// === METRICS ===

	protected TestUtils.InMemoryPoolMetrics recorder;

	@BeforeEach
	void initRecorder() {
		this.recorder = new TestUtils.InMemoryPoolMetrics();
	}

	@ParameterizedTest
	@MethodSource("allPools")
	@Tag("metrics")
	void recordsAllocationInGrowIdle(Function<PoolBuilder<String>, AbstractPool<String>> configAdjuster) {
		AtomicInteger flipCountdown = new AtomicInteger(5);
		//note the starter method here is irrelevant, only the config is created and passed to createPool
		PoolBuilder<String> builder = PoolBuilder
				.from(Mono.defer(() -> {
					int flip = flipCountdown.getAndDecrement();
					if (flip > 0) {
						return Mono.just("foo" + flip)
						           .delayElement(Duration.ofMillis(100));
					}
					else {
						return Mono.error(new IllegalStateException("boom"));
					}
				}))
				.metricsRecorder(recorder);
		Pool<String> pool = configAdjuster.apply(builder);

		assertThatIllegalStateException()
				.isThrownBy(() -> pool.growIdle(10).block())
				.withMessage("boom");

		assertThat(recorder.getAllocationTotalCount()).isEqualTo(6);
		assertThat(recorder.getAllocationSuccessCount())
				.isEqualTo(5)
				.isEqualTo(recorder.getAllocationSuccessHistogram().getTotalCount());
		assertThat(recorder.getAllocationErrorCount())
				.isEqualTo(1)
				.isEqualTo(recorder.getAllocationErrorHistogram().getTotalCount());

		long minSuccess = recorder.getAllocationSuccessHistogram().getMinValue();
		assertThat(minSuccess).as("minSuccess").isBetween(100L, 150L);
	}

	@ParameterizedTest
	@MethodSource("allPools")
	@Tag("metrics")
	void recordsAllocationInBorrow(Function<PoolBuilder<String>, AbstractPool<String>> configAdjuster) {
		AtomicBoolean flip = new AtomicBoolean();
		//note the starter method here is irrelevant, only the config is created and passed to createPool
		PoolBuilder<String> builder = PoolBuilder
				.from(Mono.defer(() -> {
					if (flip.compareAndSet(false, true)) {
						return Mono.just("foo")
						           .delayElement(Duration.ofMillis(100));
					}
					else {
						flip.compareAndSet(true, false);
						return Mono.error(new IllegalStateException("boom"));
					}
				}))
				.metricsRecorder(recorder);
		Pool<String> pool = configAdjuster.apply(builder);

		pool.acquire().block(); //success
		pool.acquire().map(PooledRef::poolable)
		    .onErrorReturn("error").block(); //error
		pool.acquire().block(); //success
		pool.acquire().map(PooledRef::poolable)
		    .onErrorReturn("error").block(); //error
		pool.acquire().block(); //success
		pool.acquire().map(PooledRef::poolable)
		    .onErrorReturn("error").block(); //error

		assertThat(recorder.getAllocationTotalCount())
				.as("total allocations")
				.isEqualTo(6);
		assertThat(recorder.getAllocationSuccessCount())
				.as("allocation success")
				.isEqualTo(3)
				.isEqualTo(recorder.getAllocationSuccessHistogram().getTotalCount());
		assertThat(recorder.getAllocationErrorCount())
				.as("allocation errors")
				.isEqualTo(3)
				.isEqualTo(recorder.getAllocationErrorHistogram().getTotalCount());

		long minSuccess = recorder.getAllocationSuccessHistogram().getMinValue();
		assertThat(minSuccess)
				.as("allocation success latency")
				.isBetween(100L, 150L);

		long minError = recorder.getAllocationErrorHistogram().getMinValue();
		assertThat(minError)
				.as("allocation error latency")
				.isBetween(0L, 15L);
	}

	@ParameterizedTest
	@MethodSource("allPools")
	@Tag("metrics")
	void recordsResetLatencies(Function<PoolBuilder<String>, AbstractPool<String>> configAdjuster) {
		AtomicBoolean flip = new AtomicBoolean();
		//note the starter method here is irrelevant, only the config is created and passed to createPool
		PoolBuilder<String> builder = PoolBuilder
				.from(Mono.just("foo"))
				.releaseHandler(s -> {
					if (flip.compareAndSet(false,
							true)) {
						return Mono.delay(Duration.ofMillis(
								100))
						           .then();
					}
					else {
						flip.compareAndSet(true,
								false);
						return Mono.empty();
					}
				})
				.metricsRecorder(recorder);
		Pool<String> pool = configAdjuster.apply(builder);

		pool.acquire().flatMap(PooledRef::release).block();
		pool.acquire().flatMap(PooledRef::release).block();

		assertThat(recorder.getResetCount()).as("reset").isEqualTo(2);

		long min = recorder.getResetHistogram().getMinValue();
		assertThat(min).isCloseTo(0L, Offset.offset(50L));

		long max = recorder.getResetHistogram().getMaxValue();
		assertThat(max).isCloseTo(100L, Offset.offset(50L));
	}

	@ParameterizedTest
	@MethodSource("allPools")
	@Tag("metrics")
	void recordsDestroyLatencies(Function<PoolBuilder<String>, AbstractPool<String>> configAdjuster) {
		AtomicBoolean flip = new AtomicBoolean();
		//note the starter method here is irrelevant, only the config is created and passed to createPool
		PoolBuilder<String> builder = PoolBuilder
				.from(Mono.just("foo"))
				.evictionPredicate(t -> true)
				.destroyHandler(s -> {
					if (flip.compareAndSet(false,
							true)) {
						return Mono.delay(Duration.ofMillis(
								500))
						           .then();
					}
					else {
						flip.compareAndSet(true,
								false);
						return Mono.empty();
					}
				})
				.metricsRecorder(recorder);
		Pool<String> pool = configAdjuster.apply(builder);

		pool.acquire().flatMap(PooledRef::release).block();
		pool.acquire().flatMap(PooledRef::release).block();

		//destroy is fire-and-forget so the 500ms one will not have finished
		assertThat(recorder.getDestroyCount()).as("destroy before 500ms").isEqualTo(1);

		await().atLeast(500, TimeUnit.MILLISECONDS)
		       .atMost(600, TimeUnit.MILLISECONDS)
		       .untilAsserted(() -> assertThat(recorder.getDestroyCount()).as("destroy after 500ms").isEqualTo(2));

		long min = recorder.getDestroyHistogram().getMinValue();
		assertThat(min).isCloseTo(0L, Offset.offset(50L));

		long max = recorder.getDestroyHistogram().getMaxValue();
		assertThat(max).isCloseTo(500L, Offset.offset(50L));
	}

	@ParameterizedTest
	@MethodSource("allPools")
	@Tag("metrics")
	void recordsResetVsRecycle(Function<PoolBuilder<String>, AbstractPool<String>> configAdjuster) {
		AtomicReference<String> content = new AtomicReference<>("foo");
		//note the starter method here is irrelevant, only the config is created and passed to createPool
		PoolBuilder<String> builder = PoolBuilder
				.from(Mono.fromCallable(() -> content.getAndSet("bar")))
				.evictionPredicate(ref -> "foo".equals(ref.poolable()))
				.metricsRecorder(recorder);
		Pool<String> pool = configAdjuster.apply(builder);

		pool.acquire().flatMap(PooledRef::release).block();
		pool.acquire().flatMap(PooledRef::release).block();

		assertThat(recorder.getResetCount()).as("reset").isEqualTo(2);
		assertThat(recorder.getDestroyCount()).as("destroy").isEqualTo(1);
		assertThat(recorder.getRecycledCount()).as("recycle").isEqualTo(1);
	}

	@ParameterizedTest
	@MethodSource("allPools")
	@Tag("metrics")
	void recordsLifetime(Function<PoolBuilder<Integer>, AbstractPool<Integer>> configAdjuster) throws InterruptedException {
		AtomicInteger allocCounter = new AtomicInteger();
		AtomicInteger destroyCounter = new AtomicInteger();
		//note the starter method here is irrelevant, only the config is created and passed to createPool
		PoolBuilder<Integer> builder = PoolBuilder
				.from(Mono.fromCallable(allocCounter::incrementAndGet))
				.sizeMax(2)
				.evictionPredicate(ref -> ref.acquireCount() >= 2)
				.destroyHandler(i -> Mono.fromRunnable(destroyCounter::incrementAndGet))
				.metricsRecorder(recorder);
		Pool<Integer> pool = configAdjuster.apply(builder);

		//first round
		PooledRef<Integer> ref1 = pool.acquire().block();
		PooledRef<Integer> ref2 = pool.acquire().block();
		Thread.sleep(250);
		ref1.release().block();
		ref2.release().block();

		//second round
		ref1 = pool.acquire().block();
		ref2 = pool.acquire().block();
		Thread.sleep(300);
		ref1.release().block();
		ref2.release().block();

		//extra acquire to show 3 allocations
		pool.acquire().block().release().block();

		assertThat(allocCounter).as("allocations").hasValue(3);
		assertThat(destroyCounter).as("destructions").hasValue(2);

		assertThat(recorder.getLifetimeHistogram().getMinNonZeroValue())
				.isCloseTo(550L, Offset.offset(30L));
	}

	@ParameterizedTest
	@MethodSource("allPools")
	@Tag("metrics")
	void recordsIdleTimeFromGrowIdle(Function<PoolBuilder<Integer>, AbstractPool<Integer>> configAdjuster) throws InterruptedException {
		AtomicInteger allocCounter = new AtomicInteger();
		//note the starter method here is irrelevant, only the config is created and passed to createPool
		PoolBuilder<Integer> builder = PoolBuilder
				.from(Mono.fromCallable(allocCounter::incrementAndGet))
				.sizeMax(2)
				.metricsRecorder(recorder);
		Pool<Integer> pool = configAdjuster.apply(builder);

		assertThat(pool.growIdle(2).block()).as("initial size 2").isEqualTo(2);

		//wait 125ms and 250ms before first acquire respectively
		Thread.sleep(125);
		pool.acquire().block();
		Thread.sleep(125);
		pool.acquire().block();

		assertThat(allocCounter).as("allocations").hasValue(2);

		recorder.getIdleTimeHistogram().outputPercentileDistribution(System.out, 1d);
		assertThat(recorder.getIdleTimeHistogram().getMinNonZeroValue())
				.as("min idle time")
				.isCloseTo(125L, Offset.offset(25L));
		assertThat(recorder.getIdleTimeHistogram().getMaxValue())
				.as("max idle time")
				.isCloseTo(250L, Offset.offset(25L));
	}

	@ParameterizedTest
	@MethodSource("allPools")
	@Tag("metrics")
	void recordsIdleTimeBetweenAcquires(Function<PoolBuilder<Integer>, AbstractPool<Integer>> configAdjuster) throws InterruptedException {
		AtomicInteger allocCounter = new AtomicInteger();
		//note the starter method here is irrelevant, only the config is created and passed to createPool
		PoolBuilder<Integer> builder = PoolBuilder
				.from(Mono.fromCallable(allocCounter::incrementAndGet))
				.sizeMax(2)
				.metricsRecorder(recorder);
		Pool<Integer> pool = configAdjuster.apply(builder);

		assertThat(pool.growIdle(2).block()).as("initial size 2").isEqualTo(2);

		//both idle for 125ms
		Thread.sleep(125);

		//first round
		PooledRef<Integer> ref1 = pool.acquire().block();
		PooledRef<Integer> ref2 = pool.acquire().block();

		ref1.release().block();
		//ref1 idle for 100ms more than ref2
		Thread.sleep(100);
		ref2.release().block();
		//ref2 idle for 40ms
		Thread.sleep(200);

		ref1 = pool.acquire().block();
		ref2 = pool.acquire().block();
		//not idle after that

		assertThat(allocCounter).as("allocations").hasValue(2);

		recorder.getIdleTimeHistogram().outputPercentileDistribution(System.out, 1d);
		assertThat(recorder.getIdleTimeHistogram().getMinNonZeroValue())
				.as("min idle time")
				.isCloseTo(125L, Offset.offset(20L));

		assertThat(recorder.getIdleTimeHistogram().getMaxValue())
				.as("max idle time")
				.isCloseTo(300L, Offset.offset(40L));
	}
}
