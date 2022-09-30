/*
 * Copyright (c) 2020-2022 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.pool.TestUtils.ParameterizedTestWithName;
import reactor.pool.TestUtils.PoolableTest;
import reactor.test.util.RaceTestUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static reactor.pool.PoolBuilder.from;

/**
 * @author Simon Baslé
 */
@SuppressWarnings("deprecation")
class PendingAcquireLifoBehaviorTest {

	private Disposable.Composite disposeList;

	@BeforeEach
	void initComposite() {
		disposeList = Disposables.composite();
	}

	@AfterEach
	void cleanup() {
		disposeList.dispose();
	}

	<T extends Disposable> T autoDispose(T toDispose) {
		disposeList.add(toDispose);
		return toDispose;
	}

	static final PoolBuilder<PoolableTest, PoolConfig<PoolableTest>> poolableTestBuilder(int minSize, int maxSize, Mono<PoolableTest> allocator, Scheduler deliveryScheduler) {
		return from(allocator)
				.sizeBetween(minSize, maxSize)
				.releaseHandler(pt -> Mono.fromRunnable(pt::clean))
				.evictionPredicate((value, metadata) -> !value.isHealthy())
				.acquisitionScheduler(deliveryScheduler);
	}
	//======

	@Test
	@Tag("loops")
	void consistentThreadDeliveringWhenNoElementsAndFullAndRaceDrain_loop() throws InterruptedException {
		for (int i = 0; i < 10_000; i++) {
			try {
				consistentThreadDeliveringWhenNoElementsAndFullAndRaceDrain(i);
			}
			finally {
				cleanup();
				initComposite();
			}
		}
	}

	@Test
	void consistentThreadDeliveringWhenNoElementsAndFullAndRaceDrain() throws InterruptedException {
		consistentThreadDeliveringWhenNoElementsAndFullAndRaceDrain(0);
	}

	void consistentThreadDeliveringWhenNoElementsAndFullAndRaceDrain(int i) throws InterruptedException {
		Scheduler allocatorScheduler = autoDispose(Schedulers.newParallel("poolable test allocator"));
		Scheduler deliveryScheduler = autoDispose(Schedulers.newSingle("delivery"));
		Scheduler acquire1Scheduler = autoDispose(Schedulers.newSingle("acquire1"));
		Scheduler racerScheduler = autoDispose(Schedulers.fromExecutorService(Executors.newFixedThreadPool(2, r -> new Thread(r, "racer"))));

		AtomicReference<String> threadName = new AtomicReference<>();
		AtomicInteger newCount = new AtomicInteger();

		PoolBuilder<PoolableTest, PoolConfig<PoolableTest>> testBuilder = poolableTestBuilder(1, 1,
				Mono.fromCallable(() -> new PoolableTest(newCount.getAndIncrement()))
					.subscribeOn(allocatorScheduler),
				deliveryScheduler);
		InstrumentedPool<PoolableTest> pool = testBuilder.lifo();

		//the pool is started with one elements, and has capacity for 1.
		//we actually first acquire that element so that next acquire will wait for a release
		PooledRef<PoolableTest> uniqueSlot = pool.acquire().block();
		assertThat(uniqueSlot).isNotNull();

		//we prepare two more acquires
		Mono<PooledRef<PoolableTest>> firstBorrower = pool.acquire();
		Mono<PooledRef<PoolableTest>> secondBorrower = pool.acquire();

		CountDownLatch latch = new CountDownLatch(1);

		//we'll enqueue a first acquire from a first thread
		//in parallel, we'll race a second acquire AND release the unique element (each on their dedicated threads)
		//we expect the release might sometimes win, which would mean acquire 1 would get served. mostly we want to verify delivery thread though
		acquire1Scheduler.schedule(() -> firstBorrower.subscribe(v -> threadName.compareAndSet(null, Thread.currentThread().getName())
				, e -> latch.countDown(), latch::countDown));
		RaceTestUtils.race(racerScheduler,
				() -> secondBorrower.subscribe(v -> threadName.compareAndSet(null, Thread.currentThread().getName())
				, e -> latch.countDown(), latch::countDown),
				uniqueSlot.release()::block);

		latch.await(1, TimeUnit.SECONDS);

		//we expect that, consistently, the poolable is delivered on a `delivery` thread
		assertThat(threadName.get()).as("round #" + i).startsWith("delivery-");

		//we expect that only 1 element was created
		assertThat(newCount).as("elements created in round " + i).hasValue(1);
	}

	@Test
	@Tag("loops")
	void consistentThreadDeliveringWhenNoElementsAndFullAndRaceDrainWithPoolable_loop() throws InterruptedException {
		for (int i = 0; i < 10_000; i++) {
			try {
				consistentThreadDeliveringWhenNoElementsAndFullAndRaceDrainWithPoolable(i);
			}
			finally {
				cleanup();
				initComposite();
			}
		}
	}

	@Test
	void consistentThreadDeliveringWhenNoElementsAndFullAndRaceDrainWithPoolable() throws InterruptedException {
		consistentThreadDeliveringWhenNoElementsAndFullAndRaceDrainWithPoolable(0);
	}

	void consistentThreadDeliveringWhenNoElementsAndFullAndRaceDrainWithPoolable(int i) throws InterruptedException {
		Scheduler allocatorScheduler = autoDispose(Schedulers.newParallel("poolable test allocator"));
		Scheduler deliveryScheduler = autoDispose(Schedulers.newSingle("delivery"));
		Scheduler acquire1Scheduler = autoDispose(Schedulers.newSingle("acquire1"));
		Scheduler racerScheduler = autoDispose(Schedulers.fromExecutorService(
				Executors.newFixedThreadPool(2, (r -> new Thread(r,"racer")))));

		AtomicReference<String> threadName = new AtomicReference<>();
		AtomicInteger newCount = new AtomicInteger();

		PoolBuilder<PoolableTest, PoolConfig<PoolableTest>> testBuilder = poolableTestBuilder(1, 1,
				Mono.fromCallable(() -> new PoolableTest(newCount.getAndIncrement()))
					.subscribeOn(allocatorScheduler),
				deliveryScheduler);
		InstrumentedPool<PoolableTest> pool = testBuilder.lifo();

		//the pool is started with one elements, and has capacity for 1.
		//we actually first acquire that element so that next acquire will wait for a release
		PooledRef<PoolableTest> uniqueSlot = pool.acquire().block();
		assertThat(uniqueSlot).isNotNull();

		//we prepare next acquire
		Mono<PoolableTest> firstBorrower = Mono.fromDirect(pool.withPoolable(Mono::just));
		Mono<PoolableTest> otherBorrower = Mono.fromDirect(pool.withPoolable(Mono::just));

		CountDownLatch latch = new CountDownLatch(3);

		//we actually perform the acquire from its dedicated thread, capturing the thread on which the element will actually get delivered
		acquire1Scheduler.schedule(() -> firstBorrower.subscribe(v -> threadName.set(Thread.currentThread().getName())
				, e -> latch.countDown(), latch::countDown));

		//in parallel, we'll race a second acquire AND release the unique element (each on their dedicated threads)
		//since LIFO we expect that if the release loses, it will server acquire1
		RaceTestUtils.race(racerScheduler,
				() -> otherBorrower.subscribe(v -> threadName.set(Thread.currentThread().getName())
						, e -> latch.countDown(), latch::countDown),
				() -> {
					uniqueSlot.release().block();
					latch.countDown();
				});
		latch.await(1, TimeUnit.SECONDS);

		//we expect that, consistently, the poolable is delivered on a `delivery` thread
		assertThat(threadName.get()).as("round #" + i).startsWith("delivery-");

		//2 elements MIGHT be created if the first acquire wins (since we're in auto-release mode)
		assertThat(newCount.get()).as("1 or 2 elements created in round " + i).isIn(1, 2);
	}

	@Test
	void stillacquiredAfterPoolDisposedMaintainsCount() {
		AtomicInteger cleanerCount = new AtomicInteger();
		InstrumentedPool<PoolableTest> pool =
				from(Mono.fromCallable(PoolableTest::new))
						.sizeBetween(3, 3)
						.releaseHandler(p -> Mono.fromRunnable(cleanerCount::incrementAndGet))
						.evictionPredicate((value, metadata) -> !value.isHealthy())
						.lifo();

		PooledRef<PoolableTest> acquired1 = pool.acquire().block();
		PooledRef<PoolableTest> acquired2 = pool.acquire().block();
		PooledRef<PoolableTest> acquired3 = pool.acquire().block();

		assertThat(acquired1).as("acquired1").isNotNull();
		assertThat(acquired2).as("acquired2").isNotNull();
		assertThat(acquired3).as("acquired3").isNotNull();

		pool.dispose();

		assertThat(pool.metrics().acquiredSize()).as("before releases").isEqualTo(3);

		acquired1.release().block();
		acquired2.release().block();
		acquired3.release().block();

		assertThat(pool.metrics().acquiredSize()).as("after releases").isEqualTo(0);
	}

	//see https://github.com/reactor/reactor-pool/issues/65
	@SuppressWarnings("FutureReturnValueIgnored")
	@Tag("loops")
	@ParameterizedTestWithName
	@CsvSource({"4, 1", "4, 100000", "10, 1", "10, 100000"})
	void concurrentAcquireCorrectlyAccountsAll(int parallelism, int loops) throws InterruptedException {
		final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(parallelism);
		autoDispose(executorService::shutdownNow);

		for (int l = 0; l < loops; l++) {
			PoolBuilder<String, PoolConfig<String>> builder = from(Mono.just("foo"))
					.sizeBetween(0, 100);
			InstrumentedPool<String> lifoPool = autoDispose(builder.lifo());
			CountDownLatch latch = new CountDownLatch(parallelism);

			for (int i = 0; i < parallelism; i++) {
				executorService.submit(() -> {
					lifoPool.acquire()
							.block();
					latch.countDown();
				});
			}
			boolean awaited = latch.await(1, TimeUnit.SECONDS);
			assertThat(awaited).as("all concurrent acquire served in loop #" + l).isTrue();
		}
	}
}