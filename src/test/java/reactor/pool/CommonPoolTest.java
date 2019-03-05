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

import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.pool.TestUtils.PoolableTest;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.test.util.RaceTestUtils;
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
				                                .lifo(false)
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
				                                .lifo(false)
				                                .build();
			}

			@Override
			public String toString() {
				return "simplePool FIFO";
			}
		};
	}

	static final <T> Function<PoolBuilder<T>, AbstractPool<T>> affinityPoolLifo() {
		return new Function<PoolBuilder<T>, AbstractPool<T>>() {
			@Override
			public AbstractPool<T> apply(PoolBuilder<T> builder) {
				return (AbstractPool<T>) builder.threadAffinity(true)
				                                .lifo(true)
				                                .build();
			}

			@Override
			public String toString() {
				return "affinityPool LIFO";
			}
		};
	}

	static final <T> Function<PoolBuilder<T>, AbstractPool<T>> simplePoolLifo() {
		return new Function<PoolBuilder<T>, AbstractPool<T>>() {
			@Override
			public AbstractPool<T> apply(PoolBuilder<T> builder) {
				return (AbstractPool<T>) builder.threadAffinity(false)
				                                .lifo(true)
				                                .build();
			}

			@Override
			public String toString() {
				return "simplePool LIFO";
			}
		};
	}

	static <T> List<Function<PoolBuilder<T>, AbstractPool<T>>> allPools() {
		return Arrays.asList(simplePoolFifo(), simplePoolLifo(), affinityPoolFifo(), affinityPoolLifo());
	}

	static <T> List<Function<PoolBuilder<T>, AbstractPool<T>>> fifoPools() {
		return Arrays.asList(simplePoolFifo(), affinityPoolFifo());
	}

	static <T> List<Function<PoolBuilder<T>, AbstractPool<T>>> lifoPools() {
		return Arrays.asList(simplePoolLifo(), affinityPoolLifo());
	}

	@ParameterizedTest
	@MethodSource("fifoPools")
	void smokeTestFifo(Function<PoolBuilder<PoolableTest>, Pool<PoolableTest>> configAdjuster) throws InterruptedException {
		AtomicInteger newCount = new AtomicInteger();

		PoolBuilder<PoolableTest> builder = PoolBuilder
				.from(Mono.defer(() -> Mono.just(new PoolableTest(newCount.incrementAndGet()))))
				.initialSize(2)
				.sizeMax(3)
				.releaseHandler(pt -> Mono.fromRunnable(pt::clean))
				.evictionPredicate(slot -> !slot.poolable().isHealthy());

		Pool<PoolableTest> pool = configAdjuster.apply(builder);

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
				.initialSize(2)
				.sizeMax(3)
				.releaseHandler(pt -> Mono.fromRunnable(pt::clean))
				.evictionPredicate(slot -> !slot.poolable().isHealthy());
		Pool<PoolableTest> pool = configAdjuster.apply(builder);

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
				.initialSize(2)
				.sizeMax(3)
				.releaseHandler(pt -> Mono.fromRunnable(pt::clean))
				.evictionPredicate(slot -> !slot.poolable().isHealthy());

		Pool<PoolableTest> pool = configAdjuster.apply(builder);

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
	@MethodSource("lifoPools")
	void simpleLifo(Function<PoolBuilder<PoolableTest>, AbstractPool<PoolableTest>> configAdjuster)
			throws InterruptedException {
		CountDownLatch latch = new CountDownLatch(2);
		AtomicInteger verif = new AtomicInteger();
		AtomicInteger newCount = new AtomicInteger();

		PoolBuilder<PoolableTest> builder =
				PoolBuilder.from(Mono.defer(() ->
						Mono.just(new PoolableTest(newCount.incrementAndGet()))))
				           .sizeMax(1)
				           .releaseHandler(pt -> Mono.fromRunnable(pt::clean));

		AbstractPool<PoolableTest> pool = configAdjuster.apply(builder);

		PooledRef<PoolableTest> ref = pool.acquire().block();

		pool.acquire()
		    .doOnNext(v -> {
			    verif.compareAndSet(1, 2);
			    System.out.println("first in got " + v);
		    })
		    .flatMap(PooledRef::release)
		    .subscribe(v -> {}, e -> latch.countDown(), latch::countDown);

		pool.acquire()
		    .doOnNext(v -> {
			    verif.compareAndSet(0, 1);
			    System.out.println("second in got " + v);
		    })
		    .flatMap(PooledRef::release)
		    .subscribe(v -> {}, e -> latch.countDown(), latch::countDown);

		ref.release().block();
		latch.await(1, TimeUnit.SECONDS);

		assertThat(verif).as("second in, first out").hasValue(2);
		assertThat(newCount).as("created one").hasValue(1);
	}

	@ParameterizedTest
	@MethodSource("lifoPools")
	void smokeTestLifo(Function<PoolBuilder<PoolableTest>, AbstractPool<PoolableTest>> configAdjuster) throws InterruptedException {
		AtomicInteger newCount = new AtomicInteger();
		PoolBuilder<PoolableTest> builder = PoolBuilder
				.from(Mono.defer(() -> Mono.just(new PoolableTest(newCount.incrementAndGet()))))
				.initialSize(2)
				.sizeMax(3)
				.releaseHandler(pt -> Mono.fromRunnable(pt::clean))
				.evictionPredicate(slot -> !slot.poolable().isHealthy());
		AbstractPool<PoolableTest> pool = configAdjuster.apply(builder);

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
		assertThat(acquired3).hasSize(3);
		assertThat(acquired2).isEmpty();

		Thread.sleep(1000);
		for (PooledRef<PoolableTest> slot : acquired3) {
			slot.release().block();
		}
		assertThat(acquired2).hasSize(3);

		assertThat(acquired1)
				.as("acquired1/3 all used up")
				.hasSameElementsAs(acquired3)
				.allSatisfy(slot -> assertThat(slot.poolable().usedUp).isEqualTo(2));

		assertThat(acquired2)
				.as("acquired2 all new")
				.allSatisfy(slot -> assertThat(slot.poolable().usedUp).isZero());
	}

	@ParameterizedTest
	@MethodSource("lifoPools")
	void smokeTestInScopeLifo(Function<PoolBuilder<PoolableTest>, AbstractPool<PoolableTest>> configAdjuster) {
		AtomicInteger newCount = new AtomicInteger();
		PoolBuilder<PoolableTest> builder =
				PoolBuilder.from(Mono.defer(() -> Mono.just(new PoolableTest(newCount.incrementAndGet()))))
				           .initialSize(2)
				           .sizeMax(3)
				           .releaseHandler(pt -> Mono.fromRunnable(pt::clean))
				           .evictionPredicate(slot -> !slot.poolable().isHealthy());
		AbstractPool<PoolableTest> pool = configAdjuster.apply(builder);

		TestPublisher<Integer> trigger1 = TestPublisher.create();
		TestPublisher<Integer> trigger2 = TestPublisher.create();
		TestPublisher<Integer> cleanupTrigger = TestPublisher.create();

		List<PoolableTest> acquired1 = new ArrayList<>();

		Mono.when(
				pool.acquireInScope(mono -> mono.doOnNext(acquired1::add).delayUntil(__ -> trigger1)),
				pool.acquireInScope(mono -> mono.doOnNext(acquired1::add).delayUntil(__ -> trigger1)),
				pool.acquireInScope(mono -> mono.doOnNext(acquired1::add).delayUntil(__ -> trigger1))
		).subscribe();

		List<PoolableTest> acquired2 = new ArrayList<>();
		Mono.when(
				pool.acquireInScope(mono -> mono.doOnNext(acquired2::add).delayUntil(__ -> cleanupTrigger)),
				pool.acquireInScope(mono -> mono.doOnNext(acquired2::add).delayUntil(__ -> cleanupTrigger)),
				pool.acquireInScope(mono -> mono.doOnNext(acquired2::add).delayUntil(__ -> cleanupTrigger))
		).subscribe();

		List<PoolableTest> acquired3 = new ArrayList<>();
		Mono.when(
				pool.acquireInScope(mono -> mono.doOnNext(acquired3::add).delayUntil(__ -> trigger2)),
				pool.acquireInScope(mono -> mono.doOnNext(acquired3::add).delayUntil(__ -> trigger2)),
				pool.acquireInScope(mono -> mono.doOnNext(acquired3::add).delayUntil(__ -> trigger2))
		).subscribe();

		assertThat(acquired1).as("first batch not pending").hasSize(3);
		assertThat(acquired2).as("second and third pending").hasSameSizeAs(acquired3).isEmpty();

		trigger1.emit(1);

		assertThat(acquired3).as("batch3 after trigger1").hasSize(3);
		assertThat(acquired2).as("batch2 after trigger1").isEmpty();

		trigger2.emit(1);
		assertThat(acquired2).as("batch2 after trigger2").hasSize(3);

		assertThat(newCount).as("allocated total").hasValue(6);

		cleanupTrigger.emit(1); //release the objects

		assertThat(acquired1)
				.as("acquired1/3 all used up")
				.hasSameElementsAs(acquired3)
				.allSatisfy(elem -> assertThat(elem.usedUp).isEqualTo(2));

		assertThat(acquired2)
				.as("acquired2 all new (released once)")
				.allSatisfy(elem -> assertThat(elem.usedUp).isOne());
	}

	@ParameterizedTest
	@MethodSource("lifoPools")
	void smokeTestAsyncLifo(Function<PoolBuilder<PoolableTest>, AbstractPool<PoolableTest>> configAdjuster) throws InterruptedException {
		AtomicInteger newCount = new AtomicInteger();

		PoolBuilder<PoolableTest> builder = PoolBuilder
				.from(Mono.defer(() -> Mono.just(new PoolableTest(newCount.incrementAndGet())))
				          .subscribeOn(Schedulers.newParallel(
						          "poolable test allocator")))
				.initialSize(2)
				.sizeMax(3)
				.releaseHandler(pt -> Mono.fromRunnable(pt::clean))
				.evictionPredicate(slot -> !slot.poolable().isHealthy());
		AbstractPool<PoolableTest> pool = configAdjuster.apply(builder);


		List<PooledRef<PoolableTest>> acquired1 = new ArrayList<>();
		CountDownLatch latch1 = new CountDownLatch(3);
		pool.acquire().subscribe(acquired1::add, Throwable::printStackTrace, latch1::countDown);
		pool.acquire().subscribe(acquired1::add, Throwable::printStackTrace, latch1::countDown);
		pool.acquire().subscribe(acquired1::add, Throwable::printStackTrace, latch1::countDown);

		List<PooledRef<PoolableTest>> acquired2 = new ArrayList<>();
		CountDownLatch latch2 = new CountDownLatch(3);
		pool.acquire().subscribe(acquired2::add, Throwable::printStackTrace, latch2::countDown);
		pool.acquire().subscribe(acquired2::add, Throwable::printStackTrace, latch2::countDown);
		pool.acquire().subscribe(acquired2::add, Throwable::printStackTrace, latch2::countDown);

		List<PooledRef<PoolableTest>> acquired3 = new ArrayList<>();
		pool.acquire().subscribe(acquired3::add);
		pool.acquire().subscribe(acquired3::add);
		pool.acquire().subscribe(acquired3::add);

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
		assertThat(acquired3).hasSize(3);
		assertThat(acquired2).isEmpty();

		Thread.sleep(1000);
		for (PooledRef<PoolableTest> slot : acquired3) {
			slot.release().block();
		}

		if (latch2.await(2, TimeUnit.SECONDS)) { //wait for the re-creation of max elements

			assertThat(acquired2).hasSize(3);

			assertThat(acquired1)
					.as("acquired1/3 all used up")
					.hasSameElementsAs(acquired3)
					.allSatisfy(slot -> assertThat(slot.poolable().usedUp).isEqualTo(2));

			assertThat(acquired2)
					.as("acquired2 all new")
					.allSatisfy(slot -> assertThat(slot.poolable().usedUp).isZero());
		}
		else {
			fail("not enough new elements generated, missing " + latch2.getCount());
		}
	}

	@ParameterizedTest
	@MethodSource("allPools")
	void returnedReleasedIfBorrowerCancelled(Function<PoolBuilder<PoolableTest>, Pool<PoolableTest>> configAdjuster) {
		AtomicInteger releasedCount = new AtomicInteger();

		PoolBuilder<PoolableTest> builder = PoolBuilder
				.from(Mono.fromCallable(PoolableTest::new))
				.initialSize(1)
				.sizeMax(1)
				.releaseHandler(poolableTest -> Mono.fromRunnable(() -> {
					poolableTest.clean();
					releasedCount.incrementAndGet();
				}))
				.evictionPredicate(slot -> !slot.poolable().isHealthy());

		Pool<PoolableTest> pool = configAdjuster.apply(builder);

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
				           .initialSize(1)
				           .sizeMax(1)
				           .releaseHandler(poolableTest -> Mono.fromRunnable(() -> {
					           poolableTest.clean();
					           releasedCount.incrementAndGet();
				           }))
				           .evictionPredicate(slot -> !slot.poolable().isHealthy());
		Pool<PoolableTest> pool = configAdjuster.apply(builder);

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
				           .initialSize(0)
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
	void pendingLimitSync(Function<PoolBuilder<Integer>, AbstractPool<Integer>> configAdjuster) {
		AtomicInteger allocatorCount = new AtomicInteger();
		Disposable.Composite composite = Disposables.composite();

		try {
			PoolBuilder<Integer> builder = PoolBuilder.from(Mono.fromCallable(allocatorCount::incrementAndGet))
			                                          .sizeMax(1)
			                                          .initialSize(1)
			                                          .maxPendingAcquire(1);
			AbstractPool<Integer> pool = configAdjuster.apply(builder);
			PooledRef<Integer> hold = pool.acquire().block();

			AtomicReference<Throwable> error = new AtomicReference<>();
			AtomicInteger errorCount = new AtomicInteger();
			AtomicInteger otherTerminationCount = new AtomicInteger();

			for (int i = 0; i < 2; i++) {
				composite.add(
						pool.acquire()
						    .doFinally(fin -> {
							    if (SignalType.ON_ERROR == fin) errorCount.incrementAndGet();
							    else otherTerminationCount.incrementAndGet();
						    })
						    .doOnError(error::set)
						    .subscribe()
				);
			}

			assertThat(AbstractPool.PENDING_COUNT.get(pool)).as("pending counter limited to 1").isEqualTo(1);

			assertThat(errorCount).as("immediate error of extraneous pending").hasValue(1);
			assertThat(otherTerminationCount).as("no other immediate termination").hasValue(0);
			assertThat(error.get()).as("extraneous pending error")
			                       .isInstanceOf(IllegalStateException.class)
			                       .hasMessage("Pending acquire queue has reached its maximum size of 1");

			hold.release().block();

			assertThat(errorCount).as("error count stable after release").hasValue(1);
			assertThat(otherTerminationCount).as("pending succeeds after release").hasValue(1);
		}
		finally {
			composite.dispose();
		}
	}

	@ParameterizedTest
	@MethodSource("allPools")
	void pendingLimitAsync(Function<PoolBuilder<Integer>, AbstractPool<Integer>> configAdjuster) {
		AtomicInteger allocatorCount = new AtomicInteger();
		final Disposable.Composite composite = Disposables.composite();

		try {
			PoolBuilder<Integer> builder = PoolBuilder.from(Mono.fromCallable(allocatorCount::incrementAndGet))
			                                          .sizeMax(1)
			                                          .initialSize(1)
			                                          .maxPendingAcquire(1);
			AbstractPool<Integer> pool = configAdjuster.apply(builder);
			PooledRef<Integer> hold = pool.acquire().block();

			AtomicReference<Throwable> error = new AtomicReference<>();
			AtomicInteger errorCount = new AtomicInteger();
			AtomicInteger otherTerminationCount = new AtomicInteger();

			final Runnable runnable = () -> composite.add(
					pool.acquire()
					    .doFinally(fin -> {
					    	if (SignalType.ON_ERROR == fin) errorCount.incrementAndGet();
					    	else otherTerminationCount.incrementAndGet();
					    })
					    .doOnError(error::set)
					    .subscribe()
			);
			RaceTestUtils.race(runnable, runnable);

			assertThat(AbstractPool.PENDING_COUNT.get(pool)).as("pending counter limited to 1").isEqualTo(1);

			assertThat(errorCount).as("immediate error of extraneous pending").hasValue(1);
			assertThat(otherTerminationCount).as("no other immediate termination").hasValue(0);
			assertThat(error.get()).as("extraneous pending error")
			                       .isInstanceOf(IllegalStateException.class)
			                       .hasMessage("Pending acquire queue has reached its maximum size of 1");

			hold.release().block();

			assertThat(errorCount).as("error count stable after release").hasValue(1);
			assertThat(otherTerminationCount).as("pending succeeds after release").hasValue(1);
		}
		finally {
			composite.dispose();
		}
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
				.initialSize(3)
				.releaseHandler(p -> Mono.fromRunnable(cleanerCount::incrementAndGet))
				.evictionPredicate(slot -> !slot.poolable().isHealthy());

		AbstractPool<PoolableTest> pool = configAdjuster.apply(builder);

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
				.initialSize(3)
				.releaseHandler(p -> Mono.fromRunnable(cleanerCount::incrementAndGet))
				.evictionPredicate(slot -> !slot.poolable().isHealthy());
		AbstractPool<PoolableTest> pool = configAdjuster.apply(builder);

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
				.initialSize(1)
				.evictionPredicate(slot -> true);
		AbstractPool<Formatter> pool = configAdjuster.apply(builder);

		pool.dispose();

		assertThatExceptionOfType(FormatterClosedException.class)
				.isThrownBy(uniqueElement::flush);
	}

	@ParameterizedTest
	@MethodSource("allPools")
	void allocatorErrorOutsideConstructorIsPropagated(Function<PoolBuilder<String>, AbstractPool<String>> configAdjuster) {
		PoolBuilder<String> builder = PoolBuilder
				.from(Mono.<String>error(new IllegalStateException("boom")))
				.sizeMax(1)
				.initialSize(0)
				.evictionPredicate(f -> true);
		AbstractPool<String> pool = configAdjuster.apply(builder);

		assertThatExceptionOfType(IllegalStateException.class)
				.isThrownBy(pool.acquire()::block)
				.withMessage("boom");
	}

	@ParameterizedTest
	@MethodSource("allPools")
	void allocatorErrorInConstructorIsThrown(Function<PoolBuilder<Object>, AbstractPool<Object>> configAdjuster) {
		final PoolBuilder<Object> builder = PoolBuilder
				.from(Mono.error(new IllegalStateException("boom")))
				.initialSize(1)
				.sizeMax(1)
				.evictionPredicate(f -> true);

		assertThatExceptionOfType(IllegalStateException.class)
				.isThrownBy(() -> configAdjuster.apply(builder))
				.withMessage("boom");
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
					.initialSize(1)
					.sizeMax(1)
					.evictionPredicate(f -> true);
			AbstractPool<Closeable> pool = configAdjuster.apply(builder);

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
	void recordsAllocationInConstructor(Function<PoolBuilder<String>, AbstractPool<String>> configAdjuster) {
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
				.initialSize(10)
				.metricsRecorder(recorder);

		assertThatIllegalStateException()
				.isThrownBy(() -> configAdjuster.apply(builder));

		assertThat(recorder.getAllocationTotalCount()).isEqualTo(2);
		assertThat(recorder.getAllocationSuccessCount())
				.isOne()
				.isEqualTo(recorder.getAllocationSuccessHistogram().getTotalCount());
		assertThat(recorder.getAllocationErrorCount())
				.isOne()
				.isEqualTo(recorder.getAllocationErrorHistogram().getTotalCount());

		long minSuccess = recorder.getAllocationSuccessHistogram().getMinValue();
		assertThat(minSuccess).isBetween(100L, 150L);
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
	void recordsIdleTimeFromConstructor(Function<PoolBuilder<Integer>, AbstractPool<Integer>> configAdjuster) throws InterruptedException {
		AtomicInteger allocCounter = new AtomicInteger();
		//note the starter method here is irrelevant, only the config is created and passed to createPool
		PoolBuilder<Integer> builder = PoolBuilder
				.from(Mono.fromCallable(allocCounter::incrementAndGet))
				.sizeMax(2)
				.initialSize(2)
				.metricsRecorder(recorder);
		Pool<Integer> pool = configAdjuster.apply(builder);


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
				.initialSize(2)
				.metricsRecorder(recorder);
		Pool<Integer> pool = configAdjuster.apply(builder);

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
