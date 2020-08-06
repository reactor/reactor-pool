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

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.FormatterClosedException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.assertj.core.api.Assertions;
import org.assertj.core.data.Offset;
import org.awaitility.Awaitility;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.pool.InstrumentedPool.PoolMetrics;
import reactor.pool.TestUtils.PoolableTest;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.test.scheduler.VirtualTimeScheduler;
import reactor.test.util.RaceTestUtils;
import reactor.test.util.TestLogger;
import reactor.util.Loggers;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

/**
 * @author Simon Baslé
 */
public class CommonPoolTest {

	static final <T> Function<PoolBuilder<T, ?>, AbstractPool<T>> lruFifo() {
		return new Function<PoolBuilder<T, ?>, AbstractPool<T>>() {
			@Override
			public AbstractPool<T> apply(PoolBuilder<T, ?> builder) {
				return (AbstractPool<T>) builder.buildPool();
			}

			@Override
			public String toString() {
				return "LRU & FIFO";
			}
		};
	}

	static final <T> Function<PoolBuilder<T, ?>, AbstractPool<T>> lruLifo() {
		return new Function<PoolBuilder<T, ?>, AbstractPool<T>>() {
			@SuppressWarnings("deprecation")
			@Override
			public AbstractPool<T> apply(PoolBuilder<T, ?> builder) {
				return (AbstractPool<T>) builder.lifo();
			}

			@Override
			public String toString() {
				return "LRU & LIFO";
			}
		};
	}

	static final <T> Function<PoolBuilder<T, ?>, AbstractPool<T>> mruFifo() {
		return new Function<PoolBuilder<T, ?>, AbstractPool<T>>() {
			@Override
			public AbstractPool<T> apply(PoolBuilder<T, ?> builder) {
				return (AbstractPool<T>) builder.idleResourceReuseMruOrder().buildPool();
			}

			@Override
			public String toString() {
				return "MRU & FIFO";
			}
		};
	}

	static final <T> Function<PoolBuilder<T, ?>, AbstractPool<T>> mruLifo() {
		return new Function<PoolBuilder<T, ?>, AbstractPool<T>>() {
			@SuppressWarnings("deprecation")
			@Override
			public AbstractPool<T> apply(PoolBuilder<T, ?> builder) {
				return (AbstractPool<T>) builder.idleResourceReuseMruOrder().lifo();
			}

			@Override
			public String toString() {
				return "MRU & LIFO";
			}
		};
	}

	static <T> List<Function<PoolBuilder<T, ?>, AbstractPool<T>>> allPools() {
		return Arrays.asList(lruFifo(), lruLifo(), mruFifo(), mruLifo());
	}

	static <T> List<Function<PoolBuilder<T, ?>, AbstractPool<T>>> fifoPools() {
		return Arrays.asList(lruFifo(), mruFifo());
	}

	static <T> List<Function<PoolBuilder<T, ?>, AbstractPool<T>>> lifoPools() {
		return Arrays.asList(lruLifo(), mruLifo());
	}

	static <T> List<Function<PoolBuilder<T, ?>, AbstractPool<T>>> mruPools() {
		return Arrays.asList(mruFifo(), mruLifo());
	}

	static <T> List<Function<PoolBuilder<T, ?>, AbstractPool<T>>> lruPools() {
		return Arrays.asList(lruFifo(), lruLifo());
	}

	@ParameterizedTest
	@MethodSource("fifoPools")
	void smokeTestFifo(Function<PoolBuilder<PoolableTest, ?>, Pool<PoolableTest>> configAdjuster) throws InterruptedException {
		AtomicInteger newCount = new AtomicInteger();

		PoolBuilder<PoolableTest, ?> builder = PoolBuilder
				//default maxUse is 5, but this test relies on it being 2
				.from(Mono.defer(() -> Mono.just(new PoolableTest(newCount.incrementAndGet(), 2))))
				.sizeBetween(2, 3)
				.releaseHandler(pt -> Mono.fromRunnable(pt::clean))
				.evictionPredicate((poolable, metadata) -> !poolable.isHealthy());

		Pool<PoolableTest> pool = configAdjuster.apply(builder);
		pool.warmup().block();

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

		List<PoolableTest> poolables1 = acquired1.stream().map(PooledRef::poolable).collect(Collectors.toList());
		List<PoolableTest> poolables2 = acquired2.stream().map(PooledRef::poolable).collect(Collectors.toList());
		assertThat(poolables1)
				.as("poolable1 and 2 all used up")
				.hasSameElementsAs(poolables2)
				.allMatch(poolable -> poolable.usedUp == 2);

		assertThat(acquired3)
				.as("acquired3 all new")
				.allSatisfy(slot -> assertThat(slot.poolable().usedUp).isZero());
	}

	@ParameterizedTest
	@MethodSource("fifoPools")
	void smokeTestInScopeFifo(Function<PoolBuilder<PoolableTest, ?>, Pool<PoolableTest>> configAdjuster) {
		AtomicInteger newCount = new AtomicInteger();

		PoolBuilder<PoolableTest, ?> builder = PoolBuilder
				//default maxUse is 5, but this test relies on it being 2
				.from(Mono.defer(() -> Mono.just(new PoolableTest(newCount.incrementAndGet(), 2))))
				.sizeBetween(2, 3)
				.releaseHandler(pt -> Mono.fromRunnable(pt::clean))
				.evictionPredicate((poolable, metadata) -> !poolable.isHealthy());
		Pool<PoolableTest> pool = configAdjuster.apply(builder);
		pool.warmup().block();

		TestPublisher<Integer> trigger1 = TestPublisher.create();
		TestPublisher<Integer> trigger2 = TestPublisher.create();
		TestPublisher<Integer> trigger3 = TestPublisher.create();

		List<PoolableTest> acquired1 = new ArrayList<>();

		Mono.when(
				pool.withPoolable(poolable -> Mono.just(poolable).doOnNext(acquired1::add).delayUntil(__ -> trigger1)),
				pool.withPoolable(poolable -> Mono.just(poolable).doOnNext(acquired1::add).delayUntil(__ -> trigger1)),
				pool.withPoolable(poolable -> Mono.just(poolable).doOnNext(acquired1::add).delayUntil(__ -> trigger1))
		).subscribe();

		List<PoolableTest> acquired2 = new ArrayList<>();
		Mono.when(
				pool.withPoolable(poolable -> Mono.just(poolable).doOnNext(acquired2::add).delayUntil(__ -> trigger2)),
				pool.withPoolable(poolable -> Mono.just(poolable).doOnNext(acquired2::add).delayUntil(__ -> trigger2)),
				pool.withPoolable(poolable -> Mono.just(poolable).doOnNext(acquired2::add).delayUntil(__ -> trigger2))
		).subscribe();

		List<PoolableTest> acquired3 = new ArrayList<>();
		Mono.when(
				pool.withPoolable(poolable -> Mono.just(poolable).doOnNext(acquired3::add).delayUntil(__ -> trigger3)),
				pool.withPoolable(poolable -> Mono.just(poolable).doOnNext(acquired3::add).delayUntil(__ -> trigger3)),
				pool.withPoolable(poolable -> Mono.just(poolable).doOnNext(acquired3::add).delayUntil(__ -> trigger3))
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
	void smokeTestAsyncFifo(Function<PoolBuilder<PoolableTest, ?>, Pool<PoolableTest>> configAdjuster) throws InterruptedException {
		AtomicInteger newCount = new AtomicInteger();

		PoolBuilder<PoolableTest, ?> builder = PoolBuilder
				//default maxUse is 5, but this test relies on it being 2
				.from(Mono.defer(() -> Mono.just(new PoolableTest(newCount.incrementAndGet(), 2)))
				          .subscribeOn(Schedulers.newParallel("poolable test allocator")))
				.sizeBetween(2, 3)
				.releaseHandler(pt -> Mono.fromRunnable(pt::clean))
				.evictionPredicate((poolable, metadata) -> !poolable.isHealthy());

		Pool<PoolableTest> pool = configAdjuster.apply(builder);
		pool.warmup().block();

		List<PooledRef<PoolableTest>> acquired1 = new CopyOnWriteArrayList<>();
		CountDownLatch latch1 = new CountDownLatch(3);
		pool.acquire().subscribe(acquired1::add, Throwable::printStackTrace, latch1::countDown);
		pool.acquire().subscribe(acquired1::add, Throwable::printStackTrace, latch1::countDown);
		pool.acquire().subscribe(acquired1::add, Throwable::printStackTrace, latch1::countDown);

		List<PooledRef<PoolableTest>> acquired2 = new CopyOnWriteArrayList<>();
		pool.acquire().subscribe(acquired2::add);
		pool.acquire().subscribe(acquired2::add);
		pool.acquire().subscribe(acquired2::add);

		List<PooledRef<PoolableTest>> acquired3 = new CopyOnWriteArrayList<>();
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

			List<PoolableTest> poolables1 = acquired1.stream().map(PooledRef::poolable).collect(Collectors.toList());
			List<PoolableTest> poolables2 = acquired2.stream().map(PooledRef::poolable).collect(Collectors.toList());

			assertThat(poolables1)
					.as("poolables 1 and 2 all used up")
					.hasSameElementsAs(poolables2)
					.allSatisfy(poolable -> assertThat(poolable.usedUp).isEqualTo(2));

			assertThat(acquired3)
					.as("acquired3 all new")
					.allSatisfy(slot -> {
						assertThat(slot).as("acquire3 slot").isNotNull();
						assertThat(slot.poolable()).as("acquire3 poolable").isNotNull();
						assertThat(slot.poolable().usedUp).as("acquire3 usedUp").isZero();
					});
		}
		else {
			fail("not enough new elements generated, missing " + latch3.getCount());
		}
	}

	@ParameterizedTest
	@MethodSource("lifoPools")
	void simpleLifo(Function<PoolBuilder<PoolableTest, ?>, AbstractPool<PoolableTest>> configAdjuster)
			throws InterruptedException {
		CountDownLatch latch = new CountDownLatch(2);
		AtomicInteger verif = new AtomicInteger();
		AtomicInteger newCount = new AtomicInteger();

		PoolBuilder<PoolableTest, ?> builder =
				PoolBuilder.from(Mono.defer(() ->
						Mono.just(new PoolableTest(newCount.incrementAndGet()))))
				           .sizeBetween(0, 1)
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
	void smokeTestLifo(Function<PoolBuilder<PoolableTest, ?>, AbstractPool<PoolableTest>> configAdjuster) throws InterruptedException {
		AtomicInteger newCount = new AtomicInteger();
		PoolBuilder<PoolableTest, ?> builder = PoolBuilder
				//default maxUse is 5, but this test relies on it being 2
				.from(Mono.defer(() -> Mono.just(new PoolableTest(newCount.incrementAndGet(), 2))))
				.sizeBetween(2, 3)
				.releaseHandler(pt -> Mono.fromRunnable(pt::clean))
				.evictionPredicate((value, metadata) -> !value.isHealthy());
		AbstractPool<PoolableTest> pool = configAdjuster.apply(builder);
		pool.warmup().block();

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

		List<PoolableTest> poolables1 = acquired1.stream().map(PooledRef::poolable).collect(Collectors.toList());
		List<PoolableTest> poolables3 = acquired3.stream().map(PooledRef::poolable).collect(Collectors.toList());

		assertThat(poolables1)
				.as("poolable 1 and 3 all used up")
				.hasSameElementsAs(poolables3)
				.allSatisfy(poolable -> assertThat(poolable.usedUp).isEqualTo(2));

		assertThat(acquired2)
				.as("acquired2 all new")
				.allSatisfy(slot -> assertThat(slot.poolable().usedUp).isZero());
	}

	@ParameterizedTest
	@MethodSource("lifoPools")
	void smokeTestInScopeLifo(Function<PoolBuilder<PoolableTest, ?>, AbstractPool<PoolableTest>> configAdjuster) {
		AtomicInteger newCount = new AtomicInteger();
		PoolBuilder<PoolableTest, ?> builder =
				//default maxUse is 5, but this test relies on it being 2
				PoolBuilder.from(Mono.defer(() -> Mono.just(new PoolableTest(newCount.incrementAndGet(), 2))))
				           .sizeBetween(2, 3)
				           .releaseHandler(pt -> Mono.fromRunnable(pt::clean))
				           .evictionPredicate((value, metadata) -> !value.isHealthy());
		AbstractPool<PoolableTest> pool = configAdjuster.apply(builder);
		pool.warmup().block();

		TestPublisher<Integer> trigger1 = TestPublisher.create();
		TestPublisher<Integer> trigger2 = TestPublisher.create();
		TestPublisher<Integer> cleanupTrigger = TestPublisher.create();

		List<PoolableTest> acquired1 = new ArrayList<>();

		Mono.when(
				pool.withPoolable(poolable -> Mono.just(poolable).doOnNext(acquired1::add).delayUntil(__ -> trigger1)),
				pool.withPoolable(poolable -> Mono.just(poolable).doOnNext(acquired1::add).delayUntil(__ -> trigger1)),
				pool.withPoolable(poolable -> Mono.just(poolable).doOnNext(acquired1::add).delayUntil(__ -> trigger1))
		).subscribe();

		List<PoolableTest> acquired2 = new ArrayList<>();
		Mono.when(
				pool.withPoolable(poolable -> Mono.just(poolable).doOnNext(acquired2::add).delayUntil(__ -> cleanupTrigger)),
				pool.withPoolable(poolable -> Mono.just(poolable).doOnNext(acquired2::add).delayUntil(__ -> cleanupTrigger)),
				pool.withPoolable(poolable -> Mono.just(poolable).doOnNext(acquired2::add).delayUntil(__ -> cleanupTrigger))
		).subscribe();

		List<PoolableTest> acquired3 = new ArrayList<>();
		Mono.when(
				pool.withPoolable(poolable -> Mono.just(poolable).doOnNext(acquired3::add).delayUntil(__ -> trigger2)),
				pool.withPoolable(poolable -> Mono.just(poolable).doOnNext(acquired3::add).delayUntil(__ -> trigger2)),
				pool.withPoolable(poolable -> Mono.just(poolable).doOnNext(acquired3::add).delayUntil(__ -> trigger2))
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
	void smokeTestAsyncLifo(Function<PoolBuilder<PoolableTest, ?>, AbstractPool<PoolableTest>> configAdjuster) throws InterruptedException {
		AtomicInteger newCount = new AtomicInteger();

		PoolBuilder<PoolableTest, ?> builder = PoolBuilder
				//default maxUse is 5, but this test relies on it being 2
				.from(Mono.defer(() -> Mono.just(new PoolableTest(newCount.incrementAndGet(), 2)))
				          .subscribeOn(Schedulers.newParallel(
						          "poolable test allocator")))
				.sizeBetween(2, 3)
				.releaseHandler(pt -> Mono.fromRunnable(pt::clean))
				.evictionPredicate((value, metadata) -> !value.isHealthy());
		AbstractPool<PoolableTest> pool = configAdjuster.apply(builder);
		pool.warmup().block();


		List<PooledRef<PoolableTest>> acquired1 = new CopyOnWriteArrayList<>();
		CountDownLatch latch1 = new CountDownLatch(3);
		pool.acquire().subscribe(acquired1::add, Throwable::printStackTrace, latch1::countDown);
		pool.acquire().subscribe(acquired1::add, Throwable::printStackTrace, latch1::countDown);
		pool.acquire().subscribe(acquired1::add, Throwable::printStackTrace, latch1::countDown);

		List<PooledRef<PoolableTest>> acquired2 = new CopyOnWriteArrayList<>();
		CountDownLatch latch2 = new CountDownLatch(3);
		pool.acquire().subscribe(acquired2::add, Throwable::printStackTrace, latch2::countDown);
		pool.acquire().subscribe(acquired2::add, Throwable::printStackTrace, latch2::countDown);
		pool.acquire().subscribe(acquired2::add, Throwable::printStackTrace, latch2::countDown);

		List<PooledRef<PoolableTest>> acquired3 = new CopyOnWriteArrayList<>();
		pool.acquire().subscribe(acquired3::add);
		pool.acquire().subscribe(acquired3::add);
		pool.acquire().subscribe(acquired3::add);

		if (!latch1.await(15, TimeUnit.SECONDS)) { //wait for creation of max elements
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

		if (latch2.await(15, TimeUnit.SECONDS)) { //wait for the re-creation of max elements

			assertThat(acquired2).as("acquired2 size").hasSize(3);

			List<PoolableTest> poolables1 = acquired1.stream().map(PooledRef::poolable).collect(Collectors.toList());
			List<PoolableTest> poolables3 = acquired3.stream().map(PooledRef::poolable).collect(Collectors.toList());

			assertThat(poolables1)
					.as("poolables 1 and 3 all used up")
					.hasSameElementsAs(poolables3)
					.allSatisfy(poolable -> assertThat(poolable.usedUp).isEqualTo(2));

			assertThat(acquired2)
					.as("acquired2 all new")
					.allSatisfy(slot -> {
						assertThat(slot).as("acquire2 slot").isNotNull();
						assertThat(slot.poolable()).as("acquire2 poolable").isNotNull();
						assertThat(slot.poolable().usedUp).as("acquire2 usedUp").isZero();
					});
		}
		else {
			fail("not enough new elements generated, missing " + latch2.getCount());
		}
	}

	@ParameterizedTest
	@MethodSource("mruPools")
	void smokeTestMruIdle(Function<PoolBuilder<PoolableTest, ?>, AbstractPool<PoolableTest>> configAdjuster) {
		AtomicInteger newCount = new AtomicInteger();
		PoolBuilder<PoolableTest, ?> builder =
				PoolBuilder.from(Mono.fromSupplier(() -> new PoolableTest(newCount.incrementAndGet())))
				           .sizeBetween(0, 3)
				           .releaseHandler(pt -> Mono.fromRunnable(pt::clean))
				           .evictionPredicate((value, metadata) -> !value.isHealthy());
		AbstractPool<PoolableTest> pool = configAdjuster.apply(builder);

		PooledRef<PoolableTest> ref1 = pool.acquire().block();
		PooledRef<PoolableTest> ref2 = pool.acquire().block();
		PooledRef<PoolableTest> ref3 = pool.acquire().block();

		ref2.release().block();
		ref1.release().block();
		ref3.release().block();

		assertThat(pool.idleSize()).as("pool fully idle").isEqualTo(3);

		pool.acquire()
		    .as(StepVerifier::create)
		    .assertNext(ref -> assertThat(ref.poolable().id).as("MRU first call returns ref3").isEqualTo(3))
		    .verifyComplete();

		pool.acquire()
		    .as(StepVerifier::create)
		    .assertNext(ref -> assertThat(ref.poolable().id).as("MRU second call returns ref1").isEqualTo(1))
		    .verifyComplete();

		assertThat(pool.idleSize()).as("idleSize after 2 MRU calls").isOne();
	}

	@ParameterizedTest
	@MethodSource("allPools")
	void firstAcquireCausesWarmupWithMinSize(Function<PoolBuilder<PoolableTest, ?>, Pool<PoolableTest>> configAdjuster)
			throws InterruptedException {
		AtomicInteger allocationCount = new AtomicInteger();

		PoolBuilder<PoolableTest, ?> builder = PoolBuilder
				.from(Mono.fromCallable(() -> {
					int id = allocationCount.incrementAndGet();
					return new PoolableTest(id);
				}))
				.sizeBetween(4, 8)
				.releaseHandler(p -> Mono.fromRunnable(p::clean));

		final Pool<PoolableTest> pool = configAdjuster.apply(builder);
		//notice no explicit warmup in this one
		assertThat(pool).isInstanceOf(InstrumentedPool.class);
		final PoolMetrics metrics = ((InstrumentedPool) pool).metrics();

		assertThat(metrics.allocatedSize()).as("constructor allocated").isEqualTo(0);

		final PooledRef<PoolableTest> firstAcquire = pool.acquire()
		                                                 .block();

		assertThat(metrics.allocatedSize()).as("first acquire allocated").isEqualTo(4);
		assertThat(metrics.idleSize()).as("warmup idle").isEqualTo(3);

		final PooledRef<PoolableTest> secondAcquire = pool.acquire()
		                                                  .block();

		assertThat(metrics.allocatedSize()).as("second acquire allocated").isEqualTo(4);
		assertThat(metrics.idleSize()).as("second acquire idle").isEqualTo(2);

		//give time for an unexpected warmup to take place so that the last assertion is valid
		Thread.sleep(100);
		assertThat(allocationCount).as("total allocations").hasValue(4);
	}

	@ParameterizedTest
	@MethodSource("allPools")
	void destroyBelowMinSizeThreshold(Function<PoolBuilder<PoolableTest, ?>, Pool<PoolableTest>> configAdjuster)
			throws InterruptedException {
		AtomicInteger allocationCount = new AtomicInteger();

		PoolBuilder<PoolableTest, ?> builder = PoolBuilder
				.from(Mono.fromCallable(() -> {
					int id = allocationCount.incrementAndGet();
					return new PoolableTest(id);
				}))
				.sizeBetween(3, 4)
				.releaseHandler(p -> Mono.fromRunnable(p::clean));

		final Pool<PoolableTest> pool = configAdjuster.apply(builder);
		assertThat(pool).isInstanceOf(InstrumentedPool.class);
		final PoolMetrics metrics = ((InstrumentedPool) pool).metrics();

		final PooledRef<PoolableTest> ref1 = pool.acquire().block();
		final PooledRef<PoolableTest> ref2 = pool.acquire().block();
		final PooledRef<PoolableTest> ref3 = pool.acquire().block();
		final PooledRef<PoolableTest> ref4 = pool.acquire().block();

		assertThat(metrics.allocatedSize()).as("initial allocated").isEqualTo(4);
		assertThat(metrics.idleSize()).as("initial idle").isEqualTo(0);

		ref1.invalidate().block();
		ref2.invalidate().block();
		ref3.invalidate().block();

		assertThat(metrics.allocatedSize()).as("3 releases: allocated").isEqualTo(1);
		assertThat(metrics.idleSize()).as("3 releases: idle").isEqualTo(0);

		PooledRef<PoolableTest> ref5 = pool.acquire()
		                                   .block();

		assertThat(metrics.allocatedSize()).as("second acquire allocated").isEqualTo(3); //1 borrowed, 1 acquiring, 1 extra matches min of 3
		assertThat(metrics.idleSize()).as("second acquire idle").isEqualTo(1); //only 1 extra created this time

		//give time for an unexpected warmup to take place so that the last assertion is valid
		Thread.sleep(100);
		assertThat(allocationCount).as("total allocations").hasValue(6);
	}

	@ParameterizedTest
	@MethodSource("allPools")
	void returnedNotReleasedIfBorrowerCancelledEarly(Function<PoolBuilder<PoolableTest, ?>, Pool<PoolableTest>> configAdjuster) {
		AtomicInteger releasedCount = new AtomicInteger();

		PoolBuilder<PoolableTest, ?> builder = PoolBuilder
				.from(Mono.fromCallable(PoolableTest::new))
				.sizeBetween(1, 1)
				.releaseHandler(poolableTest -> Mono.fromRunnable(() -> {
					poolableTest.clean();
					releasedCount.incrementAndGet();
				}))
				.evictionPredicate((poolable, metadata) -> !poolable.isHealthy());

		Pool<PoolableTest> pool = configAdjuster.apply(builder);
		pool.warmup().block();

		//acquire the only element and hold on to it
		PooledRef<PoolableTest> slot = pool.acquire().block();
		assertThat(slot).isNotNull();

		pool.acquire().subscribe().dispose();

		assertThat(releasedCount).as("before returning").hasValue(0);

		//release the element, which should only mark it once, as the pending acquire should not be visible anymore
		slot.release().block();

		assertThat(releasedCount).as("after returning").hasValue(1);
	}

	@ParameterizedTest
	@MethodSource("allPools")
	void fixedPoolReplenishOnDestroy(Function<PoolBuilder<PoolableTest, ?>, Pool<PoolableTest>> configAdjuster) throws Exception{
		AtomicInteger releasedCount = new AtomicInteger();
		AtomicInteger destroyedCount = new AtomicInteger();

		PoolBuilder<PoolableTest, ?> builder = PoolBuilder
				.from(Mono.fromCallable(PoolableTest::new))
				.sizeBetween(1, 1)
				.destroyHandler(poolableTest -> Mono.fromRunnable(() -> {
					poolableTest.clean();
					releasedCount.incrementAndGet();
				}))
				.releaseHandler(poolableTest -> Mono.fromRunnable(() -> {
					poolableTest.clean();
					destroyedCount.incrementAndGet();
				}))
				.evictionPredicate((poolable, metadata) -> !poolable.isHealthy());

		Pool<PoolableTest> pool = configAdjuster.apply(builder);
		pool.warmup().block();

		//acquire the only element
		PooledRef<PoolableTest> slot = pool.acquire().block();
		AtomicReference<PooledRef<PoolableTest>> acquired = new AtomicReference<>();
		CountDownLatch latch = new CountDownLatch(1);
		pool.acquire().subscribe(p -> {
			acquired.set(p);
			latch.countDown();
		});
		assertThat(slot).isNotNull();

		slot.invalidate().block();

		assertThat(releasedCount).as("before returning").hasValue(1);
		assertThat(destroyedCount).as("before returning").hasValue(0);

		//release the element, which should forward to the cancelled second acquire, itself also cleaning
		latch.await(5, TimeUnit.SECONDS);
		assertThat(acquired.get()).isNotNull();

		acquired.get().release().block();

		assertThat(destroyedCount).as("after returning").hasValue(1);
		assertThat(releasedCount).as("after returning").hasValue(1);
	}


	@ParameterizedTest
	@MethodSource("allPools")
	void returnedNotReleasedIfBorrowerInScopeCancelledEarly(Function<PoolBuilder<PoolableTest, ?>, Pool<PoolableTest>> configAdjuster) {
		AtomicInteger releasedCount = new AtomicInteger();
		PoolBuilder<PoolableTest, ?> builder =
				PoolBuilder.from(Mono.fromCallable(PoolableTest::new))
				           .sizeBetween(1, 1)
				           .releaseHandler(poolableTest -> Mono.fromRunnable(() -> {
					           poolableTest.clean();
					           releasedCount.incrementAndGet();
				           }))
				           .evictionPredicate((poolable, metadata) -> !poolable.isHealthy());
		Pool<PoolableTest> pool = configAdjuster.apply(builder);
		pool.warmup().block();

		//acquire the only element and hold on to it
		PooledRef<PoolableTest> slot = pool.acquire().block();
		assertThat(slot).isNotNull();

		pool.withPoolable(Mono::just).subscribe().dispose();

		assertThat(releasedCount).as("before returning").hasValue(0);

		//release the element after the second acquire has been cancelled, so it should never "see" it
		slot.release().block();

		assertThat(releasedCount).as("after returning").hasValue(1);
	}


	@ParameterizedTest
	@MethodSource("allPools")
	void allocatedReleasedIfBorrowerCancelled(Function<PoolBuilder<PoolableTest, ?>, Pool<PoolableTest>> configAdjuster) {
		Scheduler scheduler = Schedulers.newParallel("poolable test allocator");
		AtomicInteger newCount = new AtomicInteger();
		AtomicInteger releasedCount = new AtomicInteger();

		PoolBuilder<PoolableTest, ?> builder = PoolBuilder
				.from(Mono.defer(() -> Mono.delay(Duration.ofMillis(50)).thenReturn(new PoolableTest(newCount.incrementAndGet())))
				          .subscribeOn(scheduler))
				.sizeBetween(0, 1)
				.releaseHandler(poolableTest -> Mono.fromRunnable(() -> {
					poolableTest.clean();
					releasedCount.incrementAndGet();
				}))
				.evictionPredicate((poolable, metadata) -> !poolable.isHealthy());

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
	void allocatedReleasedIfBorrowerInScopeCancelled(Function<PoolBuilder<PoolableTest, ?>, Pool<PoolableTest>> configAdjuster) {
		Scheduler scheduler = Schedulers.newParallel("poolable test allocator");
		AtomicInteger newCount = new AtomicInteger();
		AtomicInteger releasedCount = new AtomicInteger();

		PoolBuilder<PoolableTest, ?> builder =
				PoolBuilder.from(Mono.defer(() -> Mono.delay(Duration.ofMillis(50)).thenReturn(new PoolableTest(newCount.incrementAndGet())))
				                     .subscribeOn(scheduler))
				           .sizeBetween(0, 1)
				           .releaseHandler(poolableTest -> Mono.fromRunnable(() -> {
					           poolableTest.clean();
					           releasedCount.incrementAndGet();
				           }))
				           .evictionPredicate((poolable, metadata) -> !poolable.isHealthy());
		Pool<PoolableTest> pool = configAdjuster.apply(builder);

		//acquire the only element and immediately dispose
		pool.withPoolable(Mono::just).subscribe().dispose();

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
	void pendingLimitSync(Function<PoolBuilder<Integer, ?>, AbstractPool<Integer>> configAdjuster) {
		AtomicInteger allocatorCount = new AtomicInteger();
		Disposable.Composite composite = Disposables.composite();

		try {
			PoolBuilder<Integer, ?> builder = PoolBuilder.from(Mono.fromCallable(allocatorCount::incrementAndGet))
			                                             .sizeBetween(1, 1)
			                                             .maxPendingAcquire(1);
			AbstractPool<Integer> pool = configAdjuster.apply(builder);
			pool.warmup().block();
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
			                       .isInstanceOf(PoolAcquirePendingLimitException.class)
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
	void pendingLimitAsync(Function<PoolBuilder<Integer, ?>, AbstractPool<Integer>> configAdjuster) {
		AtomicInteger allocatorCount = new AtomicInteger();
		final Disposable.Composite composite = Disposables.composite();

		try {
			PoolBuilder<Integer, ?> builder = PoolBuilder.from(Mono.fromCallable(allocatorCount::incrementAndGet))
			                                             .sizeBetween(1, 1)
			                                             .maxPendingAcquire(1);
			AbstractPool<Integer> pool = configAdjuster.apply(builder);
			pool.warmup().block();
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
			                       .isInstanceOf(PoolAcquirePendingLimitException.class)
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
	void cleanerFunctionError(Function<PoolBuilder<PoolableTest, ?>, Pool<PoolableTest>> configAdjuster) {
		PoolBuilder<PoolableTest, ?> builder = PoolBuilder
				.from(Mono.fromCallable(PoolableTest::new))
				.sizeBetween(0, 1)
				.releaseHandler(poolableTest -> Mono.fromRunnable(() -> {
					poolableTest.clean();
					throw new IllegalStateException("boom");
				}))
				.evictionPredicate((poolable, metadata) -> !poolable.isHealthy());

		Pool<PoolableTest> pool = configAdjuster.apply(builder);

		PooledRef<PoolableTest> slot = pool.acquire().block();

		assertThat(slot).isNotNull();

		StepVerifier.create(slot.release())
		            .verifyErrorMessage("boom");
	}

	@ParameterizedTest
	@MethodSource("allPools")
	void cleanerFunctionErrorDiscards(Function<PoolBuilder<PoolableTest, ?>, Pool<PoolableTest>> configAdjuster) {
		PoolBuilder<PoolableTest, ?> builder = PoolBuilder
				.from(Mono.fromCallable(PoolableTest::new))
				.sizeBetween(0, 1)
				.releaseHandler(poolableTest -> Mono.fromRunnable(() -> {
					poolableTest.clean();
					throw new IllegalStateException("boom");
				}))
				.evictionPredicate((poolable, metadata) -> !poolable.isHealthy());

		Pool<PoolableTest> pool = configAdjuster.apply(builder);

		PooledRef<PoolableTest> slot = pool.acquire().block();

		assertThat(slot).isNotNull();

		StepVerifier.create(slot.release())
		            .verifyErrorMessage("boom");

		assertThat(slot.poolable().discarded).as("discarded despite cleaner error").isEqualTo(1);
	}


	@ParameterizedTest
	@MethodSource("allPools")
	void disposingPoolDisposesElements(Function<PoolBuilder<PoolableTest, ?>, AbstractPool<PoolableTest>> configAdjuster) {
		AtomicInteger cleanerCount = new AtomicInteger();

		PoolBuilder<PoolableTest, ?> builder = PoolBuilder
				.from(Mono.fromCallable(PoolableTest::new))
				.sizeBetween(0, 3)
				.releaseHandler(p -> Mono.fromRunnable(cleanerCount::incrementAndGet))
				.evictionPredicate((poolable, metadata) -> !poolable.isHealthy());

		AbstractPool<PoolableTest> pool = configAdjuster.apply(builder);

		PoolableTest pt1 = new PoolableTest(1);
		PoolableTest pt2 = new PoolableTest(2);
		PoolableTest pt3 = new PoolableTest(3);

		pool.elementOffer(pt1);
		pool.elementOffer(pt2);
		pool.elementOffer(pt3);
		//important: acquire the permits to simulate the proper creation of resources
		assertThat(pool.poolConfig.allocationStrategy().getPermits(3)).as("permits granted").isEqualTo(3);

		//this releases the permits acquired above
		pool.dispose();

		assertThat(pool.idleSize()).as("idleSize").isZero();
		assertThat(cleanerCount).as("recycled elements").hasValue(0);
		assertThat(pt1.isDisposed()).as("pt1 disposed").isTrue();
		assertThat(pt2.isDisposed()).as("pt2 disposed").isTrue();
		assertThat(pt3.isDisposed()).as("pt3 disposed").isTrue();
		assertThat(pool.poolConfig.allocationStrategy().estimatePermitCount()).as("permits available post dispose").isEqualTo(3);
	}

	@ParameterizedTest
	@MethodSource("allPools")
	void disposingPoolFailsPendingBorrowers(Function<PoolBuilder<PoolableTest, ?>, AbstractPool<PoolableTest>> configAdjuster) {
		AtomicInteger cleanerCount = new AtomicInteger();
		PoolBuilder<PoolableTest, ?> builder = PoolBuilder
				.from(Mono.fromCallable(PoolableTest::new))
				.sizeBetween(3, 3)
				.releaseHandler(p -> Mono.fromRunnable(cleanerCount::incrementAndGet))
				.evictionPredicate((poolable, metadata) -> !poolable.isHealthy());

		AbstractPool<PoolableTest> pool = configAdjuster.apply(builder);
		pool.warmup().block();

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
	void releasingToDisposedPoolDisposesElement(Function<PoolBuilder<PoolableTest, ?>, AbstractPool<PoolableTest>> configAdjuster) {
		AtomicInteger cleanerCount = new AtomicInteger();
		PoolBuilder<PoolableTest, ?> builder = PoolBuilder
				.from(Mono.fromCallable(PoolableTest::new))
				.sizeBetween(3, 3)
				.releaseHandler(p -> Mono.fromRunnable(cleanerCount::incrementAndGet))
				.evictionPredicate((poolable, metadata) -> !poolable.isHealthy());
		AbstractPool<PoolableTest> pool = configAdjuster.apply(builder);
		pool.warmup().block();

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
	void acquiringFromDisposedPoolFailsBorrower(Function<PoolBuilder<PoolableTest, ?>, AbstractPool<PoolableTest>> configAdjuster) {
		AtomicInteger cleanerCount = new AtomicInteger();
		PoolBuilder<PoolableTest, ?> builder = PoolBuilder
				.from(Mono.fromCallable(PoolableTest::new))
				.sizeBetween(0, 3)
				.releaseHandler(p -> Mono.fromRunnable(cleanerCount::incrementAndGet))
				.evictionPredicate((poolable, metadata) -> !poolable.isHealthy());
		AbstractPool<PoolableTest> pool = configAdjuster.apply(builder);

		assertThat(pool.idleSize()).as("idleSize").isZero();

		pool.dispose();

		StepVerifier.create(pool.acquire())
		            .verifyErrorMessage("Pool has been shut down");

		assertThat(cleanerCount).as("recycled elements").hasValue(0);
	}

	@ParameterizedTest
	@MethodSource("allPools")
	void poolIsDisposed(Function<PoolBuilder<PoolableTest, ?>, AbstractPool<PoolableTest>> configAdjuster) {
		PoolBuilder<PoolableTest, ?> builder = PoolBuilder
				.from(Mono.fromCallable(PoolableTest::new))
				.sizeBetween(0, 3)
				.evictionPredicate((poolable, metadata) -> !poolable.isHealthy());
		AbstractPool<PoolableTest> pool = configAdjuster.apply(builder);

		assertThat(pool.isDisposed()).as("not yet disposed").isFalse();

		pool.dispose();

		assertThat(pool.isDisposed()).as("disposed").isTrue();
	}

	@ParameterizedTest
	@MethodSource("allPools")
	void disposingPoolClosesCloseable(Function<PoolBuilder<Formatter, ?>, AbstractPool<Formatter>> configAdjuster) {
		Formatter uniqueElement = new Formatter();

		PoolBuilder<Formatter, ?> builder = PoolBuilder
				.from(Mono.just(uniqueElement))
				.sizeBetween(1, 1)
				.evictionPredicate((poolable, metadata) -> true);
		AbstractPool<Formatter> pool = configAdjuster.apply(builder);
		pool.warmup().block();

		pool.dispose();

		assertThatExceptionOfType(FormatterClosedException.class)
				.isThrownBy(uniqueElement::flush);
	}

	@ParameterizedTest
	@MethodSource("allPools")
	void disposeLaterIsLazy(Function<PoolBuilder<Formatter, ?>, AbstractPool<Formatter>> configAdjuster) {
		Formatter uniqueElement = new Formatter();

		PoolBuilder<Formatter, ?> builder = PoolBuilder
				.from(Mono.just(uniqueElement))
				.sizeBetween(1, 1)
				.evictionPredicate((poolable, metadata) -> true);
		AbstractPool<Formatter> pool = configAdjuster.apply(builder);
		pool.warmup().block();

		Mono<Void> disposeMono = pool.disposeLater();
		Mono<Void> disposeMono2 = pool.disposeLater();

		assertThat(disposeMono)
				.isNotSameAs(disposeMono2);

		assertThatCode(uniqueElement::flush)
				.doesNotThrowAnyException();
	}

	@ParameterizedTest
	@MethodSource("allPools")
	void disposeLaterCompletesWhenAllReleased(Function<PoolBuilder<AtomicBoolean, ?>, AbstractPool<AtomicBoolean>> configAdjuster) {
		List<AtomicBoolean> elements = Arrays.asList(new AtomicBoolean(), new AtomicBoolean(), new AtomicBoolean());
		AtomicInteger index = new AtomicInteger(0);

		PoolBuilder<AtomicBoolean, ?> builder = PoolBuilder
				.from(Mono.fromCallable(() -> elements.get(index.getAndIncrement())))
				.sizeBetween(3, 3)
				.evictionPredicate((poolable, metadata) -> true)
				.destroyHandler(ab -> Mono.fromRunnable(() -> ab.set(true)));
		AbstractPool<AtomicBoolean> pool = configAdjuster.apply(builder);
		pool.warmup().block();

		Mono<Void> disposeMono = pool.disposeLater();

		assertThat(elements).as("before disposeLater subscription").noneMatch(AtomicBoolean::get);

		disposeMono.block();

		assertThat(elements).as("after disposeLater done").allMatch(AtomicBoolean::get);
	}

	@ParameterizedTest
	@MethodSource("allPools")
	void disposeLaterReleasedConcurrently(Function<PoolBuilder<Integer, ?>, AbstractPool<Integer>> configAdjuster) {
		AtomicInteger live = new AtomicInteger(0);

		PoolBuilder<Integer, ?> builder = PoolBuilder
				.from(Mono.fromCallable(live::getAndIncrement))
				.sizeBetween(3, 3)
				.evictionPredicate((poolable, metadata) -> true)
				.destroyHandler(ab -> Mono.delay(Duration.ofMillis(500))
				                          .doOnNext(v -> live.decrementAndGet())
				                          .then());
		AbstractPool<Integer> pool = configAdjuster.apply(builder);
		pool.warmup().block();

		Mono<Void> disposeMono = pool.disposeLater();

		assertThat(live).as("before disposeLater subscription").hasValue(3);

		disposeMono.subscribe();

		Awaitility.await().atMost(600, TimeUnit.MILLISECONDS) //serially would add up to 1500ms
		          .untilAtomic(live, CoreMatchers.is(0));
	}

	@ParameterizedTest
	@MethodSource("allPools")
	void allocatorErrorInAcquireDrains_NoMinSize(Function<PoolBuilder<String, ?>, AbstractPool<String>> configAdjuster) {
		AtomicInteger errorThrown = new AtomicInteger();
		PoolBuilder<String, ?> builder = PoolBuilder
				.from(Mono.delay(Duration.ofMillis(50))
						.flatMap(l -> {
					if (errorThrown.compareAndSet(0, 1)) {
						return Mono.<String>error(new IllegalStateException("boom"));
					}
					else {
						return Mono.just("success");
					}
				}))
				.sizeBetween(0, 1)
				.evictionPredicate((poolable, metadata) -> true);
		AbstractPool<String> pool = configAdjuster.apply(builder);

		StepVerifier.create(Flux.just(pool.acquire().onErrorResume(e -> Mono.empty()), pool.acquire())
								.flatMap(Function.identity()))
					.expectNextMatches(pooledRef -> "success".equals(pooledRef.poolable()))
					.expectComplete()
					.verify(Duration.ofSeconds(5));
	}

	@ParameterizedTest
	@MethodSource("allPools")
	@Tag("loops")
	void loopAllocatorErrorInAcquireDrains_NoMinSize(Function<PoolBuilder<String, ?>, AbstractPool<String>> configAdjuster) {
		TestLogger testLogger = new TestLogger();
		Loggers.useCustomLoggers(s -> testLogger);
		try {
			for (int i = 0; i < 100; i++) {
				allocatorErrorInAcquireDrains_NoMinSize(configAdjuster);
			}
			String log = testLogger.getOutContent();
			if (!log.isEmpty()) {
				System.out.println("Log in loop test, removed duplicated lines:");
				Stream.of(log.split("\n"))
				      .distinct()
				      .forEach(System.out::println);
			}
		}
		finally {
			Loggers.resetLoggerFactory();
		}
	}

	@ParameterizedTest
	@MethodSource("allPools")
	void allocatorErrorInAcquireDrains_WithMinSize(Function<PoolBuilder<String, ?>, AbstractPool<String>> configAdjuster) {
		PoolBuilder<String, ?> builder = PoolBuilder
				.from(Mono.delay(Duration.ofMillis(50))
						.flatMap(l -> Mono.<String>error(new IllegalStateException("boom"))))
				.sizeBetween(3, 3)
				.evictionPredicate((poolable, metadata) -> true);
		AbstractPool<String> pool = configAdjuster.apply(builder);

		StepVerifier.create(
				Flux.just(pool.acquire().onErrorResume(e -> Mono.empty()),
						pool.acquire().onErrorResume(e -> Mono.empty()),
						pool.acquire())
				    .flatMap(Function.identity()))
		            .expectErrorSatisfies(e -> assertThat(e).isInstanceOf(IllegalStateException.class)
		                                                    .hasMessage("boom"))
		            .verify(Duration.ofSeconds(5));
	}

	@ParameterizedTest
	@MethodSource("allPools")
	@Tag("loops")
	void loopAllocatorErrorInAcquireDrains_WithMinSize(Function<PoolBuilder<String, ?>, AbstractPool<String>> configAdjuster) {
		TestLogger testLogger = new TestLogger();
		Loggers.useCustomLoggers(s -> testLogger);
		try {
			for (int i = 0; i < 100; i++) {
				allocatorErrorInAcquireDrains_WithMinSize(configAdjuster);
			}
			String log = testLogger.getOutContent();
			if (!log.isEmpty()) {
				System.out.println("Log in loop test, removed duplicated lines:");
				Stream.of(log.split("\n"))
				      .distinct()
				      .forEach(System.out::println);
			}
		}
		finally {
			Loggers.resetLoggerFactory();
		}
	}

	@ParameterizedTest
	@MethodSource("allPools")
	void allocatorErrorInAcquireIsPropagated(Function<PoolBuilder<String, ?>, AbstractPool<String>> configAdjuster) {
		PoolBuilder<String, ?> builder = PoolBuilder
				.from(Mono.<String>error(new IllegalStateException("boom")))
				.sizeBetween(0, 1)
				.evictionPredicate((poolable, metadata) -> true);
		AbstractPool<String> pool = configAdjuster.apply(builder);

		StepVerifier.create(pool.acquire())
		            .verifyErrorSatisfies(e -> assertThat(e).isInstanceOf(IllegalStateException.class)
		                                                    .hasMessage("boom"));
	}

	@ParameterizedTest
	@MethodSource("allPools")
	void allocatorErrorInWarmupIsPropagated(Function<PoolBuilder<Object, ?>, AbstractPool<Object>> configAdjuster) {
		final PoolBuilder<Object, ?> builder = PoolBuilder
				.from(Mono.error(new IllegalStateException("boom")))
				.sizeBetween(1, 1)
				.evictionPredicate((poolable, metadata) -> true);
		AbstractPool<Object> pool = configAdjuster.apply(builder);

		StepVerifier.create(pool.warmup())
		            .verifyErrorSatisfies(e -> assertThat(e).isInstanceOf(IllegalStateException.class)
		                                                    .hasMessage("boom"));
	}

	@ParameterizedTest
	@MethodSource("allPools")
	void discardCloseableWhenCloseFailureLogs(Function<PoolBuilder<Closeable, ?>, AbstractPool<Closeable>> configAdjuster) {
		TestLogger testLogger = new TestLogger();
		Loggers.useCustomLoggers(it -> testLogger);
		try {
			Closeable closeable = () -> {
				throw new IOException("boom");
			};

			PoolBuilder<Closeable, ?> builder = PoolBuilder
					.from(Mono.just(closeable))
					.sizeBetween(1, 1)
					.evictionPredicate((poolable, metadata) -> true);
			AbstractPool<Closeable> pool = configAdjuster.apply(builder);
			pool.warmup().block();

			pool.dispose();

			assertThat(testLogger.getOutContent())
					.contains("Failure while discarding a released Poolable that is Closeable, could not close - java.io.IOException: boom");
		}
		finally {
			Loggers.resetLoggerFactory();
		}
	}

	@ParameterizedTest
	@MethodSource("allPools")
	void pendingTimeoutNotImpactedByLongAllocation(Function<PoolBuilder<String, ?>, AbstractPool<String>> configAdjuster) {
		VirtualTimeScheduler vts1 = VirtualTimeScheduler.getOrSet();

		try {
			PoolBuilder<String, ?> builder = PoolBuilder
					.from(Mono.just("delayed").delaySubscription(Duration.ofMillis(500)))
					.sizeBetween(0, 1);
			AbstractPool<String> pool = configAdjuster.apply(builder);

			StepVerifier.withVirtualTime(
					() -> pool.acquire(Duration.ofMillis(100)),
					() -> vts1,
					1)
			            .expectSubscription()
			            .expectNoEvent(Duration.ofMillis(500))
			            .assertNext(n -> {
			            	Assertions.assertThat(n.poolable()).isEqualTo("delayed");
			            	n.release()
				             .subscribe();
			            })
			            .verifyComplete();


			VirtualTimeScheduler vts2 = VirtualTimeScheduler.getOrSet();

			StepVerifier.withVirtualTime(
					() -> pool.acquire(Duration.ofMillis(100)).map(PooledRef::poolable),
					() -> vts2,
					1)
			            .expectSubscription()
			            .expectNext("delayed")
			            .verifyComplete();
		}
		finally {
			VirtualTimeScheduler.reset();
		}
	}

	@ParameterizedTest
	@MethodSource("allPools")
	void pendingTimeoutImpactedByLongRelease(Function<PoolBuilder<String, ?>, AbstractPool<String>> configAdjuster) {
		PoolBuilder<String, ?> builder = PoolBuilder
				.from(Mono.just("instant"))
				.sizeBetween(0, 1);
		AbstractPool<String> pool = configAdjuster.apply(builder);

		PooledRef<String> uniqueRef = pool.acquire().block();

		StepVerifier.withVirtualTime(() -> pool.acquire(Duration.ofMillis(100)).map(PooledRef::poolable))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(100))
		            .thenAwait(Duration.ofMillis(1))
		            .verifyErrorSatisfies(e -> assertThat(e)
				            .isInstanceOf(TimeoutException.class)
				            .isExactlyInstanceOf(PoolAcquireTimeoutException.class)
				            .hasMessage("Pool#acquire(Duration) has been pending for more than the configured timeout of 100ms"));
	}

	@ParameterizedTest
	@MethodSource("allPools")
	void pendingTimeoutDoesntCauseExtraReleasePostTimeout(Function<PoolBuilder<AtomicInteger, ?>, AbstractPool<AtomicInteger>> configAdjuster) {
		AtomicInteger resource = new AtomicInteger();
		PoolBuilder<AtomicInteger, ?> builder = PoolBuilder
				.from(Mono.just(resource))
				.releaseHandler(atomic -> Mono.fromRunnable(atomic::incrementAndGet))
				.sizeBetween(0, 1);
		AbstractPool<AtomicInteger> pool = configAdjuster.apply(builder);

		PooledRef<AtomicInteger> uniqueRef = pool.acquire().block();
		assert uniqueRef != null;

		StepVerifier.withVirtualTime(() -> pool.acquire(Duration.ofMillis(100)).map(PooledRef::poolable))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(100))
		            .thenAwait(Duration.ofMillis(1))
		            .verifyErrorSatisfies(e -> assertThat(e)
				            .isInstanceOf(TimeoutException.class)
				            .isExactlyInstanceOf(PoolAcquireTimeoutException.class)
				            .hasMessage("Pool#acquire(Duration) has been pending for more than the configured timeout of 100ms"));

		assertThat(resource).as("post timeout but before resource available").hasValue(0);

		uniqueRef.release().block();
		assertThat(resource).as("post timeout and after resource available").hasValue(1);
	}

	@ParameterizedTest
	@MethodSource("allPools")
	void eachBorrowerCanOnlyReleaseOnce(Function<PoolBuilder<AtomicInteger, ?>, AbstractPool<AtomicInteger>> configAdjuster) {
		AtomicInteger resource = new AtomicInteger();
		PoolBuilder<AtomicInteger, ?> builder = PoolBuilder
				.from(Mono.just(resource))
				.releaseHandler(atomic -> Mono.fromRunnable(atomic::incrementAndGet))
				.sizeBetween(0, 1);
		AbstractPool<AtomicInteger> pool = configAdjuster.apply(builder);

		PooledRef<AtomicInteger> acquire1 = pool.acquire().block();

		Mono<Void> releaserMono1 = acquire1.release();
		releaserMono1.block();
		releaserMono1.block();
		Mono<Void> releaserMono2 = acquire1.release();
		releaserMono2.block();
		releaserMono2.block();

		assertThat(resource).as("first acquire multi-release").hasValue(1);

		Mono<Void> releaserMono3 = pool.acquire().block().release();
		releaserMono3.block();
		releaserMono3.block();

		assertThat(resource).as("second acquire multi-release").hasValue(2);
	}

	@ParameterizedTest
	@MethodSource("allPools")
	void eachBorrowerCanOnlyInvalidateOnce(Function<PoolBuilder<AtomicInteger, ?>, AbstractPool<AtomicInteger>> configAdjuster) {
		AtomicInteger resource = new AtomicInteger();
		PoolBuilder<AtomicInteger, ?> builder = PoolBuilder
				.from(Mono.just(resource))
				.destroyHandler(atomic -> Mono.fromRunnable(atomic::incrementAndGet))
				.sizeBetween(0, 1);
		AbstractPool<AtomicInteger> pool = configAdjuster.apply(builder);

		PooledRef<AtomicInteger> acquire1 = pool.acquire().block();

		Mono<Void> invalidateMono1 = acquire1.invalidate();
		invalidateMono1.block();
		invalidateMono1.block();
		final Mono<Void> invalidateMono2 = acquire1.invalidate();
		invalidateMono2.block();
		invalidateMono2.block();

		assertThat(resource).as("first acquire multi-invalidate").hasValue(1);

		final Mono<Void> invalidateMono3 = pool.acquire().block().invalidate();
		invalidateMono3.block();
		invalidateMono3.block();

		assertThat(resource).as("second acquire multi-invalidate").hasValue(2);
	}

	// === METRICS ===

	protected TestUtils.InMemoryPoolMetrics recorder;

	@BeforeEach
	void initRecorder() {
		this.recorder = new TestUtils.InMemoryPoolMetrics(new TestUtils.NanoTimeClock());
	}

	@ParameterizedTest
	@MethodSource("allPools")
	@Tag("metrics")
	void recordsAllocationCountInWarmup(Function<PoolBuilder<String, ?>, AbstractPool<String>> configAdjuster) {
		AtomicBoolean flip = new AtomicBoolean();
		//note the starter method here is irrelevant, only the config is created and passed to createPool
		PoolBuilder<String, ?> builder = PoolBuilder
				.from(Mono.defer(() -> {
					if (flip.compareAndSet(false, true)) {
						return Mono.just("foo");
					}
					else {
						flip.compareAndSet(true, false);
						return Mono.error(new IllegalStateException("boom"));
					}
				}))
				.sizeBetween(10, Integer.MAX_VALUE)
				.metricsRecorder(recorder)
				.clock(recorder.getClock());
		AbstractPool<String> pool = configAdjuster.apply(builder);

		assertThatIllegalStateException()
				.isThrownBy(() -> pool.warmup().block());

		assertThat(recorder.getAllocationTotalCount()).isEqualTo(2);
		assertThat(recorder.getAllocationSuccessCount())
				.isOne()
				.isEqualTo(recorder.getAllocationSuccessHistogram().getTotalCount());
		assertThat(recorder.getAllocationErrorCount())
				.isOne()
				.isEqualTo(recorder.getAllocationErrorHistogram().getTotalCount());
	}

	@ParameterizedTest
	@MethodSource("allPools")
	@Tag("metrics")
	void recordsAllocationCountInBorrow(Function<PoolBuilder<String, ?>, AbstractPool<String>> configAdjuster) {
		AtomicBoolean flip = new AtomicBoolean();
		//note the starter method here is irrelevant, only the config is created and passed to createPool
		PoolBuilder<String, ?> builder = PoolBuilder
				.from(Mono.defer(() -> {
					if (flip.compareAndSet(false, true)) {
						return Mono.just("foo");
					}
					else {
						flip.compareAndSet(true, false);
						return Mono.error(new IllegalStateException("boom"));
					}
				}))
				.metricsRecorder(recorder)
				.clock(recorder.getClock());
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
	}

	@ParameterizedTest
	@MethodSource("allPools")
	@Tag("metrics")
	void recordsAllocationLatenciesInWarmup(Function<PoolBuilder<String, ?>, AbstractPool<String>> configAdjuster) {
		AtomicBoolean flip = new AtomicBoolean();
		//note the starter method here is irrelevant, only the config is created and passed to createPool
		PoolBuilder<String, ?> builder = PoolBuilder
				.from(Mono.defer(() -> {
					if (flip.compareAndSet(false, true)) {
						return Mono.just("foo").delayElement(Duration.ofMillis(100));
					}
					else {
						flip.compareAndSet(true, false);
						return Mono.delay(Duration.ofMillis(200)).then(Mono.error(new IllegalStateException("boom")));
					}
				}))
				.sizeBetween(10, Integer.MAX_VALUE)
				.metricsRecorder(recorder)
				.clock(recorder.getClock());
		AbstractPool<String> pool = configAdjuster.apply(builder);

		assertThatIllegalStateException()
				.isThrownBy(() -> pool.warmup().block());

		assertThat(recorder.getAllocationTotalCount()).isEqualTo(2);

		long minSuccess = recorder.getAllocationSuccessHistogram().getMinValue();
		long minError = recorder.getAllocationErrorHistogram().getMinValue();

		assertThat(minSuccess).as("allocation success latency").isGreaterThanOrEqualTo(100L);
		assertThat(minError).as("allocation error latency").isGreaterThanOrEqualTo(200L);
	}

	@ParameterizedTest
	@MethodSource("allPools")
	@Tag("metrics")
	void recordsAllocationLatenciesInBorrow(Function<PoolBuilder<String, ?>, AbstractPool<String>> configAdjuster) {
		AtomicBoolean flip = new AtomicBoolean();
		//note the starter method here is irrelevant, only the config is created and passed to createPool
		PoolBuilder<String, ?> builder = PoolBuilder
				.from(Mono.defer(() -> {
					if (flip.compareAndSet(false, true)) {
						return Mono.just("foo").delayElement(Duration.ofMillis(100));
					}
					else {
						flip.compareAndSet(true, false);
						return Mono.delay(Duration.ofMillis(200)).then(Mono.error(new IllegalStateException("boom")));
					}
				}))
				.metricsRecorder(recorder)
				.clock(recorder.getClock());
		Pool<String> pool = configAdjuster.apply(builder);

		pool.acquire().block(); //success
		pool.acquire().map(PooledRef::poolable)
		    .onErrorReturn("error").block(); //error

		long minSuccess = recorder.getAllocationSuccessHistogram().getMinValue();
		assertThat(minSuccess)
				.as("allocation success latency")
				.isGreaterThanOrEqualTo(100L);

		long minError = recorder.getAllocationErrorHistogram().getMinValue();
		assertThat(minError)
				.as("allocation error latency")
				.isGreaterThanOrEqualTo(200L);
	}

	@ParameterizedTest
	@MethodSource("allPools")
	@Tag("metrics")
	void recordsResetLatencies(Function<PoolBuilder<String, ?>, AbstractPool<String>> configAdjuster) {
		AtomicBoolean flip = new AtomicBoolean();
		//note the starter method here is irrelevant, only the config is created and passed to createPool
		PoolBuilder<String, ?> builder = PoolBuilder
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
				.metricsRecorder(recorder)
				.clock(recorder.getClock());
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
	void recordsDestroyLatencies(Function<PoolBuilder<String, ?>, AbstractPool<String>> configAdjuster) {
		AtomicBoolean flip = new AtomicBoolean();
		//note the starter method here is irrelevant, only the config is created and passed to createPool
		PoolBuilder<String, ?> builder = PoolBuilder
				.from(Mono.just("foo"))
				.evictionPredicate((poolable, metadata) -> true)
				.destroyHandler(s -> {
					if (flip.compareAndSet(false,
							true)) {
						return Mono.delay(Duration.ofMillis(500))
						           .then();
					}
					else {
						flip.compareAndSet(true,
								false);
						return Mono.empty();
					}
				})
				.metricsRecorder(recorder)
				.clock(recorder.getClock());
		Pool<String> pool = configAdjuster.apply(builder);

		pool.acquire().flatMap(PooledRef::release).block();
		pool.acquire().flatMap(PooledRef::release).block();

		//destroy is fire-and-forget so the 500ms one will not have finished
		assertThat(recorder.getDestroyCount()).as("destroy before 500ms").isEqualTo(1);

		await().pollDelay(500, TimeUnit.MILLISECONDS)
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
	void recordsResetVsRecycle(Function<PoolBuilder<String, ?>, AbstractPool<String>> configAdjuster) {
		AtomicReference<String> content = new AtomicReference<>("foo");
		//note the starter method here is irrelevant, only the config is created and passed to createPool
		PoolBuilder<String, ?> builder = PoolBuilder
				.from(Mono.fromCallable(() -> content.getAndSet("bar")))
				.evictionPredicate((poolable, metadata) -> "foo".equals(poolable))
				.metricsRecorder(recorder)
				.clock(recorder.getClock());
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
	void recordsLifetime(Function<PoolBuilder<Integer, ?>, AbstractPool<Integer>> configAdjuster) throws InterruptedException {
		AtomicInteger allocCounter = new AtomicInteger();
		AtomicInteger destroyCounter = new AtomicInteger();
		//note the starter method here is irrelevant, only the config is created and passed to createPool
		PoolBuilder<Integer, ?> builder = PoolBuilder
				.from(Mono.fromCallable(allocCounter::incrementAndGet))
				.sizeBetween(0, 2)
				.evictionPredicate((poolable, metadata) -> metadata.acquireCount() >= 2)
				.destroyHandler(i -> Mono.fromRunnable(destroyCounter::incrementAndGet))
				.metricsRecorder(recorder)
				.clock(recorder.getClock());
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
	void recordsIdleTimeFromConstructor(Function<PoolBuilder<Integer, ?>, AbstractPool<Integer>> configAdjuster) throws InterruptedException {
		AtomicInteger allocCounter = new AtomicInteger();
		//note the starter method here is irrelevant, only the config is created and passed to createPool
		PoolBuilder<Integer, ?> builder = PoolBuilder
				.from(Mono.fromCallable(allocCounter::incrementAndGet))
				.sizeBetween(2, 2)
				.metricsRecorder(recorder)
				.clock(recorder.getClock());
		Pool<Integer> pool = configAdjuster.apply(builder);
		pool.warmup().block();


		//wait 125ms and 250ms before first acquire respectively
		Thread.sleep(125);
		pool.acquire().block();
		Thread.sleep(125);
		pool.acquire().block();

		assertThat(allocCounter).as("allocations").hasValue(2);

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
	void recordsIdleTimeBetweenAcquires(Function<PoolBuilder<Integer, ?>, AbstractPool<Integer>> configAdjuster) throws InterruptedException {
		AtomicInteger allocCounter = new AtomicInteger();
		//note the starter method here is irrelevant, only the config is created and passed to createPool
		PoolBuilder<Integer, ?> builder = PoolBuilder
				.from(Mono.fromCallable(allocCounter::incrementAndGet))
				.sizeBetween(2, 2)
				.metricsRecorder(recorder)
				.clock(recorder.getClock());
		Pool<Integer> pool = configAdjuster.apply(builder);
		pool.warmup().block();

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

		assertThat(recorder.getIdleTimeHistogram().getMinNonZeroValue())
				.as("min idle time")
				.isCloseTo(125L, Offset.offset(20L));

		assertThat(recorder.getIdleTimeHistogram().getMaxValue())
				.as("max idle time")
				.isCloseTo(300L, Offset.offset(40L));
	}

	@ParameterizedTest
	@MethodSource("allPools")
	@Tag("metrics")
	void acquireTimeout(Function<PoolBuilder<Integer, ?>, AbstractPool<Integer>> configAdjuster) {
		AtomicInteger allocCounter = new AtomicInteger();
		AtomicInteger didReset = new AtomicInteger();
		//note the starter method here is irrelevant, only the config is created and passed to createPool
		PoolBuilder<Integer, ?> builder = PoolBuilder
				.from(Mono.fromCallable(allocCounter::incrementAndGet))
				.releaseHandler(i -> Mono.fromRunnable(didReset::incrementAndGet))
				.sizeBetween(0, 1);
		Pool<Integer> pool = configAdjuster.apply(builder);

		PooledRef<Integer> uniqueElement = Objects.requireNonNull(pool.acquire().block());

		assertThat(uniqueElement.metadata().acquireCount()).isOne();

		assertThatExceptionOfType(RuntimeException.class)
				.isThrownBy(() -> pool.acquire().timeout(Duration.ofMillis(50)).block())
				.withCauseExactlyInstanceOf(TimeoutException.class);

		assertThat(didReset).hasValue(0);

		uniqueElement.release().block();
		assertThat(uniqueElement.metadata().acquireCount()).as("acquireCount post timeout-then-release").isOne();

		assertThat(pool.acquire().block()).isNotNull();
		assertThat(allocCounter).as("final allocation count").hasValue(1);
		assertThat(didReset).hasValue(1);
	}

	@ParameterizedTest
	@MethodSource("allPools")
	@Tag("metrics")
	void instrumentedPoolsMetricsAreSelfViews(Function<PoolBuilder<Integer, ?>, AbstractPool<Integer>> configAdjuster) {
		PoolBuilder<Integer, ?> builder = PoolBuilder.from(Mono.just(1));
		Pool<Integer> pool = configAdjuster.apply(builder);

		assertThat(pool).isInstanceOf(InstrumentedPool.class);

		PoolMetrics metrics = ((InstrumentedPool) pool).metrics();

		assertThat(pool).isSameAs(metrics);
	}

	@ParameterizedTest
	@MethodSource("allPools")
	@Tag("metrics")
	void instrumentAllocatedIdleAcquired(Function<PoolBuilder<Integer, ?>, AbstractPool<Integer>> configAdjuster) {
		PoolBuilder<Integer, ?> builder = PoolBuilder.from(Mono.just(1))
				.sizeBetween(0, 1);
		InstrumentedPool<Integer> pool = configAdjuster.apply(builder);
		PoolMetrics poolMetrics = pool.metrics();

		assertThat(poolMetrics.allocatedSize()).as("allocated at start").isZero();

		PooledRef<Integer> ref = pool.acquire().block();

		assertThat(poolMetrics.allocatedSize()).as("allocated at first acquire").isOne();
		assertThat(poolMetrics.idleSize()).as("idle at first acquire").isZero();
		assertThat(poolMetrics.acquiredSize()).as("acquired size at first acquire").isOne();

		ref.release().block();

		assertThat(poolMetrics.allocatedSize()).as("allocated after release").isOne();
		assertThat(poolMetrics.idleSize()).as("idle after release").isOne();
		assertThat(poolMetrics.acquiredSize()).as("acquired after release").isZero();
	}

	@ParameterizedTest
	@MethodSource("allPools")
	@Tag("metrics")
	void instrumentAllocatedIdleAcquired_1(Function<PoolBuilder<Integer, ?>, AbstractPool<Integer>> configAdjuster)
			throws Exception {
		PoolBuilder<Integer, ?> builder = PoolBuilder.from(Mono.just(1))
				.sizeBetween(0, 1);
		InstrumentedPool<Integer> pool = configAdjuster.apply(builder);
		PoolMetrics poolMetrics = pool.metrics();

		AtomicInteger allocated = new AtomicInteger(poolMetrics.allocatedSize());
		AtomicInteger idle = new AtomicInteger(poolMetrics.idleSize());
		AtomicInteger acquired = new AtomicInteger(poolMetrics.acquiredSize());
		AtomicReference<PooledRef<Integer>> ref = new AtomicReference<>();

		assertThat(allocated.get()).as("allocated at start").isZero();

		CountDownLatch latch1 = new CountDownLatch(1);
		pool.acquire()
				.subscribe(pooledRef -> {
					allocated.set(poolMetrics.allocatedSize());
					idle.set(poolMetrics.idleSize());
					acquired.set(poolMetrics.acquiredSize());
					ref.set(pooledRef);
					latch1.countDown();
				});

		assertThat(latch1.await(30, TimeUnit.SECONDS)).isTrue();
		assertThat(allocated.get()).as("allocated at first acquire").isOne();
		assertThat(idle.get()).as("idle at first acquire").isZero();
		assertThat(acquired.get()).as("acquired size at first acquire").isOne();

		CountDownLatch latch2 = new CountDownLatch(1);
		ref.get()
				.release()
				.subscribe(null,
						null,
						() -> {
							allocated.set(poolMetrics.allocatedSize());
							idle.set(poolMetrics.idleSize());
							acquired.set(poolMetrics.acquiredSize());
							latch2.countDown();
						});

		assertThat(latch2.await(30, TimeUnit.SECONDS)).isTrue();
		assertThat(allocated.get()).as("allocated after release").isOne();
		assertThat(idle.get()).as("idle after release").isOne();
		assertThat(acquired.get()).as("acquired after release").isZero();
	}

	@ParameterizedTest
	@MethodSource("allPools")
	@Tag("metrics")
	void instrumentPendingAcquire(Function<PoolBuilder<Integer, ?>, AbstractPool<Integer>> configAdjuster) {
		PoolBuilder<Integer, ?> builder = PoolBuilder.from(Mono.just(1))
				.sizeBetween(0, 1);
		InstrumentedPool<Integer> pool = configAdjuster.apply(builder);
		PoolMetrics poolMetrics = pool.metrics();

		PooledRef<Integer> ref = pool.acquire().block();

		assertThat(poolMetrics.pendingAcquireSize()).as("first acquire not pending").isZero();


		pool.acquire().subscribe();

		assertThat(poolMetrics.pendingAcquireSize()).as("second acquire put in pending").isOne();

		ref.release().block();

		assertThat(poolMetrics.pendingAcquireSize()).as("second acquire not pending after release").isZero();
	}

	@ParameterizedTest
	@MethodSource("allPools")
	@Tag("metrics")
	void getConfigMaxPendingAcquire(Function<PoolBuilder<Integer, ?>, AbstractPool<Integer>> configAdjuster) {
		PoolBuilder<Integer, ?> builder = PoolBuilder.from(Mono.just(1))
		                                          .maxPendingAcquire(12);
		InstrumentedPool<Integer> pool = configAdjuster.apply(builder);
		PoolMetrics poolMetrics = pool.metrics();

		assertThat(poolMetrics.getMaxPendingAcquireSize()).isEqualTo(12);
	}

	@ParameterizedTest
	@MethodSource("allPools")
	@Tag("metrics")
	void getConfigMaxPendingAcquireUnbounded(Function<PoolBuilder<Integer, ?>, AbstractPool<Integer>> configAdjuster) {
		PoolBuilder<Integer, ?> builder = PoolBuilder.from(Mono.just(1))
		                                          .maxPendingAcquireUnbounded();
		InstrumentedPool<Integer> pool = configAdjuster.apply(builder);
		PoolMetrics poolMetrics = pool.metrics();

		assertThat(poolMetrics.getMaxPendingAcquireSize()).isEqualTo(Integer.MAX_VALUE);
	}

	@ParameterizedTest
	@MethodSource("allPools")
	@Tag("metrics")
	void getConfigMaxSize(Function<PoolBuilder<Integer, ?>, AbstractPool<Integer>> configAdjuster) {
		PoolBuilder<Integer, ?> builder = PoolBuilder.from(Mono.just(1))
		                                          .sizeBetween(0, 22);
		InstrumentedPool<Integer> pool = configAdjuster.apply(builder);
		PoolMetrics poolMetrics = pool.metrics();

		assertThat(poolMetrics.getMaxAllocatedSize()).isEqualTo(22);
	}

	@ParameterizedTest
	@MethodSource("allPools")
	@Tag("metrics")
	void getConfigMaxSizeUnbounded(Function<PoolBuilder<Integer, ?>, AbstractPool<Integer>> configAdjuster) {
		PoolBuilder<Integer, ?> builder = PoolBuilder.from(Mono.just(1))
		                                          .sizeUnbounded();
		InstrumentedPool<Integer> pool = configAdjuster.apply(builder);
		PoolMetrics poolMetrics = pool.metrics();

		assertThat(poolMetrics.getMaxAllocatedSize()).isEqualTo(Integer.MAX_VALUE);
	}

	@ParameterizedTest
	@MethodSource("allPools")
	void invalidateRaceIdleState(Function<PoolBuilder<Integer, ?>, AbstractPool<Integer>> configAdjuster)
			throws Exception {
		AtomicInteger destroyCounter = new AtomicInteger();
		PoolBuilder<Integer, ?> builder = PoolBuilder.from(Mono.just(1))
		                                             .evictionPredicate((obj, meta) -> true)
		                                             .destroyHandler(i -> Mono.fromRunnable(destroyCounter::incrementAndGet))
		                                             .sizeBetween(0, 1)
		                                             .maxPendingAcquire(1);
		InstrumentedPool<Integer> pool = configAdjuster.apply(builder);
		int loops = 4000;

		final CountDownLatch latch = new CountDownLatch(loops * 2);

		for (int i = 0; i < loops; i++) {
			final PooledRef<Integer> pooledRef = pool.acquire().block();
			RaceTestUtils.race(() -> pooledRef.release().subscribe(v -> {}, e -> latch.countDown(),
					latch::countDown),
					() -> pooledRef.invalidate().subscribe(v -> {}, e -> latch.countDown(),
							latch::countDown));
		}

		assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();

		assertThat(destroyCounter).hasValue(4000);
	}

	@ParameterizedTest
	@MethodSource("lruPools")
	void releaseAllOnAcquire(Function<PoolBuilder<Integer, ?>, AbstractPool<Integer>> configAdjuster) {
		AtomicInteger intSource = new AtomicInteger();
		AtomicInteger releasedIndex = new AtomicInteger();
		ConcurrentLinkedQueue<Integer> destroyed = new ConcurrentLinkedQueue<>();

		PoolBuilder<Integer, PoolConfig<Integer>> builder =
				PoolBuilder.from(Mono.fromCallable(intSource::incrementAndGet))
				           .evictionPredicate((obj, meta) -> obj <= releasedIndex.get())
				           .destroyHandler(i -> Mono.fromRunnable(() -> destroyed.offer(i)))
				           .sizeBetween(0, 5)
				           .maxPendingAcquire(100);

		InstrumentedPool<Integer> pool = configAdjuster.apply(builder);

		//acquire THEN release four refs
		List<PooledRef<Integer>> fourRefs = Arrays.asList(
				pool.acquire().block(),
				pool.acquire().block(),
				pool.acquire().block(),
				pool.acquire().block()
		);
		for (PooledRef<Integer> ref : fourRefs) {
			ref.release().block();
		}
		//4 idle
		assertThat(pool.metrics().idleSize()).as("idleSize").isEqualTo(4);
		assertThat(destroyed).as("none destroyed so far").isEmpty();

		//set the release predicate to release <= 3 on acquire
		releasedIndex.set(4);

		assertThat(pool.acquire().block().poolable()).as("allocated post idle").isEqualTo(5);
		assertThat(intSource).as("did generate a new value").hasValue(5);
		assertThat(destroyed).as("single acquire released all idle")
		                     .containsExactly(1, 2, 3, 4);
	}

	@ParameterizedTest
	@MethodSource("allPools")
	void releaseRacingWithPoolClose(Function<PoolBuilder<AtomicInteger, ?>, AbstractPool<AtomicInteger>> configAdjuster)
			throws InterruptedException {
		for (int i = 0; i < 10_000; i++) {
			final int round = i + 1;
			ConcurrentLinkedQueue<AtomicInteger> created = new ConcurrentLinkedQueue<>();
			PoolBuilder<AtomicInteger, PoolConfig<AtomicInteger>> builder =
					PoolBuilder.from(Mono.fromCallable(() -> {
						AtomicInteger resource = new AtomicInteger(round);
						created.add(resource);
						return resource;
					}))
					           .evictionPredicate((obj, meta) -> false)
					           .destroyHandler(ai -> Mono.fromRunnable(() -> ai.set(-1)))
					           .sizeBetween(0, 4);

			InstrumentedPool<AtomicInteger> pool = configAdjuster.apply(builder);

			PooledRef<AtomicInteger> ref = pool.acquire().block();

			final CountDownLatch latch = new CountDownLatch(2);
			//acquire-and-release, vs pool disposal
			RaceTestUtils.race(
					() -> ref.release().doFinally(__ -> latch.countDown()).subscribe(),
					() -> pool.disposeLater().doFinally(__ -> latch.countDown()).subscribe()
			);

			assertThat(latch.await(30, TimeUnit.SECONDS)).as("latch counted down").isTrue();
			assertThat(pool.isDisposed()).as("pool isDisposed").isTrue();
			assertThat(pool.metrics().idleSize()).as("pool has no idle elements").isZero();

//			Thread.sleep(10);
			assertThat(created).allSatisfy(ai -> assertThat(ai).hasValue(-1));
		}
	}

	@ParameterizedTest
	@MethodSource("allPools")
	void poolCloseRacingWithRelease(Function<PoolBuilder<AtomicInteger, ?>, AbstractPool<AtomicInteger>> configAdjuster)
			throws InterruptedException {
		for (int i = 0; i < 10_000; i++) {
			final int round = i + 1;
			ConcurrentLinkedQueue<AtomicInteger> created = new ConcurrentLinkedQueue<>();
			PoolBuilder<AtomicInteger, PoolConfig<AtomicInteger>> builder =
					PoolBuilder.from(Mono.fromCallable(() -> {
						AtomicInteger resource = new AtomicInteger(round);
						created.add(resource);
						return resource;
					}))
					           .evictionPredicate((obj, meta) -> false)
					           .destroyHandler(ai -> Mono.fromRunnable(() -> ai.set(-1)))
					           .sizeBetween(0, 4);

			InstrumentedPool<AtomicInteger> pool = configAdjuster.apply(builder);

			PooledRef<AtomicInteger> ref = pool.acquire().block();

			final CountDownLatch latch = new CountDownLatch(2);
			//acquire-and-release, vs pool disposal
			RaceTestUtils.race(
					() -> pool.disposeLater().doFinally(__ -> latch.countDown()).subscribe(),
					() -> ref.release().doFinally(__ -> latch.countDown()).subscribe()
			);

			assertThat(latch.await(30, TimeUnit.SECONDS)).as("latch counted down").isTrue();
			assertThat(pool.isDisposed()).as("pool isDisposed").isTrue();
			assertThat(pool.metrics().idleSize()).as("pool has no idle elements").isZero();
			assertThat(created).allSatisfy(ai -> assertThat(ai).hasValue(-1));
		}
	}

	@ParameterizedTest
	@MethodSource("allPools")
	void raceShutdownAndAcquireInvalidate(Function<PoolBuilder<AtomicInteger, ?>, AbstractPool<AtomicInteger>> configAdjuster) {
		AtomicInteger ai = new AtomicInteger();
		PoolBuilder<AtomicInteger, PoolConfig<AtomicInteger>> configBuilder = PoolBuilder
				.from(Mono.fromSupplier(() -> {
					ai.incrementAndGet();
					return ai;
				}))
				.evictionIdle(Duration.ZERO)
				.destroyHandler(resource -> Mono.fromRunnable(resource::decrementAndGet))
				.sizeBetween(0, 1);

		AtomicReference<Throwable> errorRef = new AtomicReference<>();
		for (int i = 0; i < 100_000; i++) {
			errorRef.set(null);
			InstrumentedPool<AtomicInteger> pool = configAdjuster.apply(configBuilder);

			if (i % 2 == 0) {
				RaceTestUtils.race(
						() -> pool.disposeLater().subscribe(v -> {}, errorRef::set),
						() -> pool.acquire()
						          .flatMap(PooledRef::invalidate)
						          .onErrorResume(PoolShutdownException.class, e -> Mono.empty())
						          .subscribe(v -> {}, errorRef::set)
				);
			}
			else {
				RaceTestUtils.race(
						() -> pool.acquire()
						          .flatMap(PooledRef::invalidate)
						          .onErrorResume(PoolShutdownException.class, e -> Mono.empty())
						          .subscribe(v -> {}, errorRef::set),
						() -> pool.disposeLater().subscribe(v -> {}, errorRef::set)
				);
			}
			if (errorRef.get() != null) {
				errorRef.get().printStackTrace();
			}
			assertThat(errorRef.get()).as("exception in " + configAdjuster.toString() + " iteration " + i).isNull();
		}
		assertThat(ai).as("creates and destroys stabilizes to 0").hasValue(0);
	}

	@ParameterizedTest
	@MethodSource("allPools")
	void raceShutdownAndPreAcquiredInvalidate(Function<PoolBuilder<AtomicInteger, ?>, AbstractPool<AtomicInteger>> configAdjuster) {
		AtomicInteger ai = new AtomicInteger();
		PoolBuilder<AtomicInteger, PoolConfig<AtomicInteger>> configBuilder = PoolBuilder
				.from(Mono.fromSupplier(() -> {
					ai.incrementAndGet();
					return ai;
				}))
				.evictionIdle(Duration.ZERO)
				.destroyHandler(resource -> Mono.fromRunnable(resource::decrementAndGet))
				.sizeBetween(0, 1);

		AtomicReference<Throwable> errorRef = new AtomicReference<>();
		for (int i = 0; i < 100_000; i++) {
			errorRef.set(null);
			InstrumentedPool<AtomicInteger> pool = configAdjuster.apply(configBuilder);
			PooledRef<AtomicInteger> aiRef = pool.acquire().block();

			if (i % 2 == 0) {
				RaceTestUtils.race(
						() -> pool.disposeLater().subscribe(v -> {}, errorRef::set),
						() -> aiRef.invalidate()
						           .onErrorResume(PoolShutdownException.class, e -> Mono.empty())
						           .subscribe(v -> {}, errorRef::set)
				);
			}
			else {
				RaceTestUtils.race(
						() -> aiRef.invalidate()
						           .onErrorResume(PoolShutdownException.class, e -> Mono.empty())
						           .subscribe(v -> {}, errorRef::set),
						() -> pool.disposeLater().subscribe(v -> {}, errorRef::set)
				);
			}
			if (errorRef.get() != null) {
				errorRef.get().printStackTrace();
			}
			assertThat(errorRef.get()).as("exception in " + configAdjuster.toString() + " iteration " + i).isNull();
		}
		assertThat(ai).as("creates and destroys stabilizes to 0").hasValue(0);
	}

	@ParameterizedTest
	@MethodSource("allPools")
	void raceShutdownAndPreAcquiredReleaseWithEviction(Function<PoolBuilder<AtomicInteger, ?>, AbstractPool<AtomicInteger>> configAdjuster) {
		AtomicInteger ai = new AtomicInteger();
		PoolBuilder<AtomicInteger, PoolConfig<AtomicInteger>> configBuilder = PoolBuilder
				.from(Mono.fromSupplier(() -> {
					ai.incrementAndGet();
					return ai;
				}))
				.evictionIdle(Duration.ZERO)
				.destroyHandler(resource -> Mono.fromRunnable(resource::decrementAndGet))
				.sizeBetween(0, 1);

		AtomicReference<Throwable> errorRef = new AtomicReference<>();
		for (int i = 0; i < 100_000; i++) {
			errorRef.set(null);
			InstrumentedPool<AtomicInteger> pool = configAdjuster.apply(configBuilder);
			PooledRef<AtomicInteger> aiRef = pool.acquire().block();

			if (i % 2 == 0) {
				RaceTestUtils.race(
						() -> pool.disposeLater().subscribe(v -> {}, errorRef::set),
						() -> aiRef.release()
						           .onErrorResume(PoolShutdownException.class, e -> Mono.empty())
						           .subscribe(v -> {}, errorRef::set)
				);
			}
			else {
				RaceTestUtils.race(
						() -> aiRef.release()
						           .onErrorResume(PoolShutdownException.class, e -> Mono.empty())
						           .subscribe(v -> {}, errorRef::set),
						() -> pool.disposeLater().subscribe(v -> {}, errorRef::set)
				);
			}
			if (errorRef.get() != null) {
				errorRef.get().printStackTrace();
			}
			assertThat(errorRef.get()).as("exception in " + configAdjuster.toString() + " iteration " + i).isNull();
		}
		assertThat(ai).as("creates and destroys stabilizes to 0").hasValue(0);
	}

}
