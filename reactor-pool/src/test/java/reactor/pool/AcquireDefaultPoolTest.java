/*
 * Copyright (c) 2018-2025 VMware Inc. or its affiliates, All Rights Reserved.
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
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.assertj.core.data.Offset;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.provider.CsvSource;
import org.reactivestreams.Subscription;

import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.pool.TestUtils.PoolableTest;
import reactor.test.util.RaceTestUtils;
import reactor.util.function.Tuple2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static reactor.pool.PoolBuilder.from;

/**
 * This test class uses both modes of acquire on the {@link PoolBuilder#buildPool()} default
 * pool.
 *
 * @author Simon Baslé
 */
//TODO merge with CommonPoolTest ?
//TODO ensure correct cleanup of executors and schedulers in all tests
class AcquireDefaultPoolTest {

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

	//==utils for package-private config==
	static final PoolBuilder<PoolableTest, PoolConfig<PoolableTest>> poolableTestBuilder(int minSize, int maxSize, Mono<PoolableTest> allocator) {
		return from(allocator)
				.sizeBetween(minSize, maxSize)
				.releaseHandler(pt -> Mono.fromRunnable(pt::clean))
				.evictionPredicate((value, metadata) -> !value.isHealthy());
	}

	static final PoolBuilder<PoolableTest, PoolConfig<PoolableTest>> poolableTestBuilder(int minSize, int maxSize, Mono<PoolableTest> allocator, Scheduler deliveryScheduler) {
		return from(allocator)
				.sizeBetween(minSize, maxSize)
				.releaseHandler(pt -> Mono.fromRunnable(pt::clean))
				.evictionPredicate((value, metadata) -> !value.isHealthy())
				.acquisitionScheduler(deliveryScheduler);
	}

	static final PoolBuilder<PoolableTest, PoolConfig<PoolableTest>> poolableTestBuilder(int minSize, int maxSize, Mono<PoolableTest> allocator,
			Consumer<? super PoolableTest> additionalCleaner) {
		return from(allocator)
				.sizeBetween(minSize, maxSize)
				.releaseHandler(poolableTest -> Mono.fromRunnable(() -> {
					poolableTest.clean();
					additionalCleaner.accept(poolableTest);
				}))
				.evictionPredicate((value, metadata) -> !value.isHealthy());
	}
	//======

	@Test
	void demonstrateAcquireInScopePipeline() throws InterruptedException {
		AtomicInteger counter = new AtomicInteger();
		AtomicReference<@Nullable String> releaseRef = new AtomicReference<>();

		InstrumentedPool<String> pool =
				from(Mono.just("Hello Reactive World"))
						.sizeBetween(0, 1)
						.releaseHandler(s -> Mono.fromRunnable(()-> releaseRef.set(s)))
						.buildPool();

		Flux<String> words = pool.withPoolable(poolable -> Mono.just(poolable)
				//simulate deriving a value from the resource (ie. query from DB connection)
				.map(resource -> resource.split(" "))
				//then further process the derived value to produce multiple values (ie. rows from a query)
				.flatMapIterable(Arrays::asList)
				//and all that with latency
				.delayElements(Duration.ofMillis(500)));

		words.subscribe(v -> counter.incrementAndGet());
		assertThat(counter).hasValue(0);

		Thread.sleep(1100);
		//we're in the middle of processing the "rows"
		assertThat(counter).as("before all emitted").hasValue(2);
		assertThat(releaseRef).as("still acquiring").hasValue(null);

		Thread.sleep(500);
		//we've finished processing, let's check resource has been automatically released
		assertThat(counter).as("after all emitted").hasValue(3);
		assertThat(pool.metrics().allocatedSize()).as("allocation permits").isEqualTo(pool.metrics().getMaxAllocatedSize());
		assertThat(pool.metrics().idleSize()).as("available").isOne();
		assertThat(releaseRef).as("released").hasValue("Hello Reactive World");
	}

	@Nested
	static class AcquireTest {

		@Test
		@Tag("loops")
		void allocatedReleasedOrAbortedIfCancelRequestRace_loop() throws InterruptedException {
			AtomicInteger newCount = new AtomicInteger();
			AtomicInteger releasedCount = new AtomicInteger();
			for (int i = 0; i < 100; i++) {
				allocatedReleasedOrAbortedIfCancelRequestRace(i, newCount, releasedCount, i % 2 == 0);
			}
			System.out.println("Total release of " + releasedCount.get() + " for " + newCount.get() + " created over 100 rounds");
		}

		@Test
		void allocatedReleasedOrAbortedIfCancelRequestRace() throws InterruptedException {
			allocatedReleasedOrAbortedIfCancelRequestRace(0, new AtomicInteger(), new AtomicInteger(), true);
			allocatedReleasedOrAbortedIfCancelRequestRace(1, new AtomicInteger(), new AtomicInteger(), false);

		}

		@SuppressWarnings("FutureReturnValueIgnored")
		void allocatedReleasedOrAbortedIfCancelRequestRace(int round, AtomicInteger newCount, AtomicInteger releasedCount, boolean cancelFirst) throws InterruptedException {
			Scheduler scheduler = Schedulers.newParallel("poolable test allocator");
			final ExecutorService executorService = Executors.newFixedThreadPool(2);

			try {

				InstrumentedPool<PoolableTest> pool = poolableTestBuilder(0, 1,
						Mono.defer(() -> Mono.delay(Duration.ofMillis(50)).thenReturn(new PoolableTest(newCount.incrementAndGet())))
							.subscribeOn(scheduler),
						pt -> releasedCount.incrementAndGet())
.buildPool();

				//acquire the only element and capture the subscription, don't request just yet
				CountDownLatch latch = new CountDownLatch(1);
				final BaseSubscriber<PooledRef<PoolableTest>> baseSubscriber = new BaseSubscriber<PooledRef<PoolableTest>>() {
					@Override
					protected void hookOnSubscribe(Subscription subscription) {
						//don't request
						latch.countDown();
					}
				};
				pool.acquire().subscribe(baseSubscriber);
				latch.await();

				if (cancelFirst) {
					executorService.submit(baseSubscriber::cancel);
					executorService.submit(baseSubscriber::requestUnbounded);
				}
				else {
					executorService.submit(baseSubscriber::requestUnbounded);
					executorService.submit(baseSubscriber::cancel);
				}

				//release due to cancel is async, give it ample time
				await().atMost(200, TimeUnit.MILLISECONDS).with().pollInterval(10, TimeUnit.MILLISECONDS)
					   .untilAsserted(() -> assertThat(releasedCount)
							   .as("released vs created in round " + round + (cancelFirst? " (cancel first)" : " (request first)"))
							   .hasValue(newCount.get()));
			}
			finally {
				scheduler.dispose();
				executorService.shutdownNow();
			}
		}

		@Test
		void defaultThreadDeliveringWhenHasElements() throws InterruptedException {
			AtomicReference<String> threadName = new AtomicReference<>();
			Scheduler acquireScheduler = Schedulers.newSingle("acquire");
			InstrumentedPool<PoolableTest> pool = poolableTestBuilder(1, 1,
					Mono.fromCallable(PoolableTest::new)
						.subscribeOn(Schedulers.newParallel("poolable test allocator")))
.buildPool();
			pool.warmup().block();

			//the pool is started and warmed up with one available element
			//we prepare to acquire it
			Mono<PooledRef<PoolableTest>> borrower = pool.acquire();
			CountDownLatch latch = new CountDownLatch(1);

			//we actually request the acquire from a separate thread and see from which thread the element was delivered
			acquireScheduler.schedule(() -> borrower.subscribe(v -> threadName.set(Thread.currentThread().getName()), e -> latch.countDown(), latch::countDown));
			latch.await(1, TimeUnit.SECONDS);

			assertThat(threadName.get())
					.startsWith("acquire-");
		}

		@Test
		void defaultThreadDeliveringWhenNoElementsButNotFull() throws InterruptedException {
			AtomicReference<String> threadName = new AtomicReference<>();
			Scheduler acquireScheduler = Schedulers.newSingle("acquire");
			InstrumentedPool<PoolableTest> pool = poolableTestBuilder(0, 1,
					Mono.fromCallable(PoolableTest::new)
						.subscribeOn(Schedulers.newParallel("poolable test allocator")))
.buildPool();

			//the pool is started with no elements, and has capacity for 1
			//we prepare to acquire, which would allocate the element
			Mono<PooledRef<PoolableTest>> borrower = pool.acquire();
			CountDownLatch latch = new CountDownLatch(1);

			//we actually request the acquire from a separate thread, but the allocation also happens in a dedicated thread
			//we look at which thread the element was delivered from
			acquireScheduler.schedule(() -> borrower.subscribe(v -> threadName.set(Thread.currentThread().getName()), e -> latch.countDown(), latch::countDown));
			latch.await(1, TimeUnit.SECONDS);

			assertThat(threadName.get())
					.startsWith("poolable test allocator-");
		}

		@Test
		void defaultThreadDeliveringWhenNoElementsAndFull() throws InterruptedException {
			AtomicReference<String> threadName = new AtomicReference<>();
			Scheduler acquireScheduler = Schedulers.newSingle("acquire");
			Scheduler releaseScheduler = Schedulers.fromExecutorService(
					Executors.newSingleThreadScheduledExecutor((r -> new Thread(r,"release"))));
			InstrumentedPool<PoolableTest> pool = poolableTestBuilder(1, 1,
					Mono.fromCallable(PoolableTest::new)
						.subscribeOn(Schedulers.newParallel("poolable test allocator")))
.buildPool();

			//the pool is started with one elements, and has capacity for 1.
			//we actually first acquire that element so that next acquire will wait for a release
			PooledRef<PoolableTest> uniqueSlot = pool.acquire().block();
			assertThat(uniqueSlot).isNotNull();

			//we prepare next acquire
			Mono<PooledRef<PoolableTest>> borrower = pool.acquire();
			CountDownLatch latch = new CountDownLatch(1);

			//we actually perform the acquire from its dedicated thread, capturing the thread on which the element will actually get delivered
			acquireScheduler.schedule(() -> borrower.subscribe(v -> threadName.set(Thread.currentThread().getName()),
					e -> latch.countDown(), latch::countDown));
			//after a short while, we release the acquired unique element from a third thread
			releaseScheduler.schedule(uniqueSlot.release()::block, 500, TimeUnit.MILLISECONDS);
			latch.await(1, TimeUnit.SECONDS);

			assertThat(threadName.get())
					.isEqualTo("release");
		}

		@Test
		@Tag("loops")
		void defaultThreadDeliveringWhenNoElementsAndFullAndRaceDrain_loop() throws InterruptedException {
			AtomicInteger releaserWins = new AtomicInteger();
			AtomicInteger borrowerWins = new AtomicInteger();

			for (int i = 0; i < 100; i++) {
				defaultThreadDeliveringWhenNoElementsAndFullAndRaceDrain(i, releaserWins, borrowerWins);
			}
			//look at the stats and show them in case of assertion error. We expect all deliveries to be on either of the racer threads.
			//we expect a subset of the deliveries to happen on the second borrower's thread
			String stats = "releaser won " + releaserWins.get() + ", borrower won " + borrowerWins.get();
			assertThat(releaserWins.get()).as("releaser should win some. " + stats).isPositive();
			assertThat(releaserWins.get() + borrowerWins.get()).as(stats).isEqualTo(100);
		}

		@Test
		void defaultThreadDeliveringWhenNoElementsAndFullAndRaceDrain() throws InterruptedException {
			AtomicInteger releaserWins = new AtomicInteger();
			AtomicInteger borrowerWins = new AtomicInteger();

			defaultThreadDeliveringWhenNoElementsAndFullAndRaceDrain(0, releaserWins, borrowerWins);

			assertThat(releaserWins.get() + borrowerWins.get()).isEqualTo(1);
		}

		void defaultThreadDeliveringWhenNoElementsAndFullAndRaceDrain(int round, AtomicInteger releaserWins, AtomicInteger borrowerWins) throws InterruptedException {
			AtomicReference<String> threadName = new AtomicReference<>();
			AtomicInteger newCount = new AtomicInteger();
			Scheduler acquire1Scheduler = Schedulers.newSingle("acquire1");
			Scheduler racerReleaseScheduler = Schedulers.fromExecutorService(
					Executors.newSingleThreadScheduledExecutor((r -> new Thread(r,"racerRelease"))));
			Scheduler racerAcquireScheduler = Schedulers.fromExecutorService(
					Executors.newSingleThreadScheduledExecutor((r -> new Thread(r,"racerAcquire"))));

			InstrumentedPool<PoolableTest> pool = poolableTestBuilder(1, 1,
					Mono.fromCallable(() -> new PoolableTest(newCount.getAndIncrement()))
						.subscribeOn(Schedulers.newParallel("poolable test allocator")))
.buildPool();

			//the pool is started with one elements, and has capacity for 1.
			//we actually first acquire that element so that next acquire will wait for a release
			PooledRef<PoolableTest> uniqueSlot = pool.acquire().block();
			assertThat(uniqueSlot).isNotNull();

			//we prepare next acquire
			Mono<PooledRef<PoolableTest>> borrower = pool.acquire();
			CountDownLatch latch = new CountDownLatch(1);

			//we actually perform the acquire from its dedicated thread, capturing the thread on which the element will actually get delivered
			acquire1Scheduler.schedule(() -> borrower.subscribe(v -> threadName.set(Thread.currentThread().getName())
					, e -> latch.countDown(), latch::countDown));

			//in parallel, we'll both attempt concurrent acquire AND release the unique element (each on their dedicated threads)
			racerAcquireScheduler.schedule(pool.acquire()::block, 100, TimeUnit.MILLISECONDS);
			racerReleaseScheduler.schedule(uniqueSlot.release()::block, 100, TimeUnit.MILLISECONDS);
			latch.await(1, TimeUnit.SECONDS);

			assertThat(newCount).as("created 1 poolable in round " + round).hasValue(1);

			//we expect that sometimes the race will let the second borrower thread drain, which would mean first borrower
			//will get the element delivered from racerAcquire thread. Yet the rest of the time it would get drained by racerRelease.
			if (threadName.get().startsWith("racerRelease")) releaserWins.incrementAndGet();
			else if (threadName.get().startsWith("racerAcquire")) borrowerWins.incrementAndGet();
			else System.out.println(threadName.get());
		}

		@Test
		void consistentThreadDeliveringWhenHasElements() throws InterruptedException {
			Scheduler deliveryScheduler = Schedulers.newSingle("delivery");
			AtomicReference<String> threadName = new AtomicReference<>();
			Scheduler acquireScheduler = Schedulers.newSingle("acquire");
			InstrumentedPool<PoolableTest> pool = poolableTestBuilder(1, 1,
					Mono.fromCallable(PoolableTest::new)
						.subscribeOn(Schedulers.newParallel("poolable test allocator")),
					deliveryScheduler)
.buildPool();

			//the pool is started with one available element
			//we prepare to acquire it
			Mono<PooledRef<PoolableTest>> borrower = pool.acquire();
			CountDownLatch latch = new CountDownLatch(1);

			//we actually request the acquire from a separate thread and see from which thread the element was delivered
			acquireScheduler.schedule(() -> borrower.subscribe(v -> threadName.set(Thread.currentThread().getName()), e -> latch.countDown(), latch::countDown));
			latch.await(1, TimeUnit.SECONDS);

			assertThat(threadName.get())
					.startsWith("delivery-");
		}

		@Test
		void consistentThreadDeliveringWhenNoElementsButNotFull() throws InterruptedException {
			Scheduler deliveryScheduler = Schedulers.newSingle("delivery");
			AtomicReference<String> threadName = new AtomicReference<>();
			Scheduler acquireScheduler = Schedulers.newSingle("acquire");
			InstrumentedPool<PoolableTest> pool = poolableTestBuilder(0, 1,
					Mono.fromCallable(PoolableTest::new)
						.subscribeOn(Schedulers.newParallel("poolable test allocator")),
					deliveryScheduler)
.buildPool();

			//the pool is started with no elements, and has capacity for 1
			//we prepare to acquire, which would allocate the element
			Mono<PooledRef<PoolableTest>> borrower = pool.acquire();
			CountDownLatch latch = new CountDownLatch(1);

			//we actually request the acquire from a separate thread, but the allocation also happens in a dedicated thread
			//we look at which thread the element was delivered from
			acquireScheduler.schedule(() -> borrower.subscribe(v -> threadName.set(Thread.currentThread().getName()), e -> latch.countDown(), latch::countDown));
			latch.await(1, TimeUnit.SECONDS);

			assertThat(threadName.get())
					.startsWith("delivery-");
		}

		@Test
		void consistentThreadDeliveringWhenNoElementsAndFull() throws InterruptedException {
			Scheduler deliveryScheduler = Schedulers.newSingle("delivery");
			AtomicReference<String> threadName = new AtomicReference<>();
			Scheduler acquireScheduler = Schedulers.newSingle("acquire");
			Scheduler releaseScheduler = Schedulers.fromExecutorService(
					Executors.newSingleThreadScheduledExecutor((r -> new Thread(r,"release"))));
			InstrumentedPool<PoolableTest> pool = poolableTestBuilder(1, 1,
					Mono.fromCallable(PoolableTest::new)
						.subscribeOn(Schedulers.newParallel("poolable test allocator")),
					deliveryScheduler)
.buildPool();

			//the pool is started with one elements, and has capacity for 1.
			//we actually first acquire that element so that next acquire will wait for a release
			PooledRef<PoolableTest> uniqueSlot = pool.acquire().block();
			assertThat(uniqueSlot).isNotNull();

			//we prepare next acquire
			Mono<PooledRef<PoolableTest>> borrower = pool.acquire();
			CountDownLatch latch = new CountDownLatch(1);

			//we actually perform the acquire from its dedicated thread, capturing the thread on which the element will actually get delivered
			acquireScheduler.schedule(() -> borrower.subscribe(v -> threadName.set(Thread.currentThread().getName()),
					e -> latch.countDown(), latch::countDown));
			//after a short while, we release the acquired unique element from a third thread
			releaseScheduler.schedule(uniqueSlot.release()::block, 500, TimeUnit.MILLISECONDS);
			latch.await(1, TimeUnit.SECONDS);

			assertThat(threadName.get())
					.startsWith("delivery-");
		}

		@Test
		@Tag("loops")
		void consistentThreadDeliveringWhenNoElementsAndFullAndRaceDrain_loop() throws InterruptedException {
			for (int i = 0; i < 100; i++) {
				consistentThreadDeliveringWhenNoElementsAndFullAndRaceDrain(i);
			}
		}

		@Test
		void consistentThreadDeliveringWhenNoElementsAndFullAndRaceDrain() throws InterruptedException {
			consistentThreadDeliveringWhenNoElementsAndFullAndRaceDrain(0);
		}

		void consistentThreadDeliveringWhenNoElementsAndFullAndRaceDrain(int i) throws InterruptedException {
			Scheduler deliveryScheduler = Schedulers.newSingle("delivery");
			AtomicReference<String> threadName = new AtomicReference<>();
			AtomicInteger newCount = new AtomicInteger();

			Scheduler acquire1Scheduler = Schedulers.newSingle("acquire1");
			Scheduler racerReleaseScheduler = Schedulers.fromExecutorService(
					Executors.newSingleThreadScheduledExecutor((r -> new Thread(r,"racerRelease"))));
			Scheduler racerAcquireScheduler = Schedulers.newSingle("racerAcquire");

			InstrumentedPool<PoolableTest> pool = poolableTestBuilder(1, 1,
					Mono.fromCallable(() -> new PoolableTest(newCount.getAndIncrement()))
						.subscribeOn(Schedulers.newParallel("poolable test allocator")),
					deliveryScheduler)
.buildPool();

			//the pool is started with one elements, and has capacity for 1.
			//we actually first acquire that element so that next acquire will wait for a release
			PooledRef<PoolableTest> uniqueSlot = pool.acquire().block();
			assertThat(uniqueSlot).isNotNull();

			//we prepare next acquire
			Mono<PooledRef<PoolableTest>> borrower = pool.acquire();
			CountDownLatch latch = new CountDownLatch(1);

			//we actually perform the acquire from its dedicated thread, capturing the thread on which the element will actually get delivered
			acquire1Scheduler.schedule(() -> borrower.subscribe(v -> threadName.set(Thread.currentThread().getName())
					, e -> latch.countDown(), latch::countDown));

			//in parallel, we'll both attempt a second acquire AND release the unique element (each on their dedicated threads
			Mono<PooledRef<PoolableTest>> otherBorrower = pool.acquire();
			racerAcquireScheduler.schedule(() -> otherBorrower.subscribe().dispose(), 100, TimeUnit.MILLISECONDS);
			racerReleaseScheduler.schedule(uniqueSlot.release()::block, 100, TimeUnit.MILLISECONDS);
			latch.await(1, TimeUnit.SECONDS);

			//we expect that, consistently, the poolable is delivered on a `delivery` thread
			assertThat(threadName.get()).as("round #" + i).startsWith("delivery-");

			//we expect that only 1 element was created
			assertThat(newCount).as("elements created in round " + i).hasValue(1);
		}

		@Test
		@Tag("loops")
		void acquireReleaseRaceWithMinSize_loop() {
			final Scheduler racer = Schedulers.fromExecutorService(Executors.newFixedThreadPool(2));
			AtomicInteger newCount = new AtomicInteger();
			try {
				InstrumentedPool<PoolableTest> pool =
				from(Mono.fromCallable(() -> new PoolableTest(newCount.getAndIncrement())))
						.sizeBetween(4, 5)
						.buildPool();

				for (int i = 0; i < 100; i++) {
					RaceTestUtils.race(racer,
							() -> {
								PooledRef<PoolableTest> ref = pool.acquire().block();
								assertThat(ref).isNotNull();
								ref.release().block();
							},
							() -> {
								PooledRef<PoolableTest> ref = pool.acquire().block();
								assertThat(ref).isNotNull();
								ref.release().block();
							});
				}
				//we expect that only 3 element was created
				assertThat(newCount).as("elements created in total").hasValue(4);
			}
			finally {
				racer.dispose();
			}
		}
	}

	@Nested
	static class AcquireInScopeTest {

		@Test
		@DisplayName("acquire delays instead of allocating past maxSize")
		void acquireDelaysNotAllocate() {
			AtomicInteger newCount = new AtomicInteger();
			InstrumentedPool<PoolableTest> pool = poolableTestBuilder(2, 3,
					Mono.defer(() -> Mono.just(new PoolableTest(newCount.incrementAndGet()))))
					.buildPool();

			pool.withPoolable(poolable -> Mono.just(poolable).delayElement(Duration.ofMillis(500))).subscribe();
			pool.withPoolable(poolable -> Mono.just(poolable).delayElement(Duration.ofMillis(500))).subscribe();
			pool.withPoolable(poolable -> Mono.just(poolable).delayElement(Duration.ofMillis(500))).subscribe();

			final Tuple2<Long, PoolableTest> tuple2 = pool.withPoolable(Mono::just).elapsed().blockLast();

			assertThat(tuple2).isNotNull();

			assertThat(tuple2.getT1()).as("pending for 500ms").isCloseTo(500L, Offset.offset(50L));
			assertThat(tuple2.getT2().usedUp).as("discarded twice").isEqualTo(2);
			assertThat(tuple2.getT2().id).as("id").isLessThan(4);
		}

		@Test
		@Tag("loops")
		void allocatedReleasedOrAbortedIfCancelRequestRace_loop() throws InterruptedException {
			AtomicInteger newCount = new AtomicInteger();
			AtomicInteger releasedCount = new AtomicInteger();
			for (int i = 0; i < 100; i++) {
				allocatedReleasedOrAbortedIfCancelRequestRace(i, newCount, releasedCount, i % 2 == 0);
			}
			System.out.println("Total release of " + releasedCount.get() + " for " + newCount.get() + " created over 100 rounds");
		}

		@Test
		void allocatedReleasedOrAbortedIfCancelRequestRace() throws InterruptedException {
			allocatedReleasedOrAbortedIfCancelRequestRace(0, new AtomicInteger(), new AtomicInteger(), true);
			allocatedReleasedOrAbortedIfCancelRequestRace(1, new AtomicInteger(), new AtomicInteger(), false);

		}

		@SuppressWarnings("FutureReturnValueIgnored")
		void allocatedReleasedOrAbortedIfCancelRequestRace(int round, AtomicInteger newCount, AtomicInteger releasedCount, boolean cancelFirst) throws InterruptedException {
			Scheduler scheduler = Schedulers.newParallel("poolable test allocator");

			InstrumentedPool<PoolableTest> pool = poolableTestBuilder(0, 1,
					Mono.defer(() -> Mono.delay(Duration.ofMillis(50)).thenReturn(new PoolableTest(newCount.incrementAndGet())))
						.subscribeOn(scheduler),
					pt -> releasedCount.incrementAndGet())
.buildPool();

			//acquire the only element and capture the subscription, don't request just yet
			CountDownLatch latch = new CountDownLatch(1);
			final BaseSubscriber<PoolableTest> baseSubscriber = new BaseSubscriber<PoolableTest>() {
				@Override
				protected void hookOnSubscribe(Subscription subscription) {
					//don't request
					latch.countDown();
				}
			};
			pool.withPoolable(Mono::just).subscribe(baseSubscriber);
			latch.await();

			final ExecutorService executorService = Executors.newFixedThreadPool(2);
			if (cancelFirst) {
				executorService.submit(baseSubscriber::cancel);
				executorService.submit(baseSubscriber::requestUnbounded);
			}
			else {
				executorService.submit(baseSubscriber::requestUnbounded);
				executorService.submit(baseSubscriber::cancel);
			}

			//release due to cancel is async, give it a bit of time
			await().atMost(100, TimeUnit.MILLISECONDS).with().pollInterval(10, TimeUnit.MILLISECONDS)
				   .untilAsserted(() -> assertThat(releasedCount)
						   .as("released vs created in round " + round + (cancelFirst? " (cancel first)" : " (request first)"))
						   .hasValue(newCount.get()));
		}

		@Test
		void defaultThreadDeliveringWhenHasElements() throws InterruptedException {
			AtomicReference<String> threadName = new AtomicReference<>();
			Scheduler acquireScheduler = Schedulers.newSingle("acquire");
			InstrumentedPool<PoolableTest> pool = poolableTestBuilder(1, 1,
					Mono.fromCallable(PoolableTest::new)
						.subscribeOn(Schedulers.newParallel("poolable test allocator")))
.buildPool();
			pool.warmup().block();

			//the pool is started and warmed up with one available element
			//we prepare to acquire it
			Mono<PoolableTest> borrower = Mono.fromDirect(pool.withPoolable(Mono::just));
			CountDownLatch latch = new CountDownLatch(1);

			//we actually request the acquire from a separate thread and see from which thread the element was delivered
			acquireScheduler.schedule(() -> borrower.subscribe(v -> threadName.set(Thread.currentThread().getName()), e -> latch.countDown(), latch::countDown));
			latch.await(1, TimeUnit.SECONDS);

			assertThat(threadName.get())
					.startsWith("acquire-");
		}

		@Test
		void defaultThreadDeliveringWhenNoElementsButNotFull() throws InterruptedException {
			AtomicReference<String> threadName = new AtomicReference<>();
			Scheduler acquireScheduler = Schedulers.newSingle("acquire");
			InstrumentedPool<PoolableTest> pool = poolableTestBuilder(0, 1,
					Mono.fromCallable(PoolableTest::new)
						.subscribeOn(Schedulers.newParallel("poolable test allocator")))
.buildPool();

			//the pool is started with no elements, and has capacity for 1
			//we prepare to acquire, which would allocate the element
			Mono<PoolableTest> borrower = Mono.fromDirect(pool.withPoolable(Mono::just));
			CountDownLatch latch = new CountDownLatch(1);

			//we actually request the acquire from a separate thread, but the allocation also happens in a dedicated thread
			//we look at which thread the element was delivered from
			acquireScheduler.schedule(() -> borrower.subscribe(v -> threadName.set(Thread.currentThread().getName()), e -> latch.countDown(), latch::countDown));
			latch.await(1, TimeUnit.SECONDS);

			assertThat(threadName.get())
					.startsWith("poolable test allocator-");
		}

		@Test
		void defaultThreadDeliveringWhenNoElementsAndFull() throws InterruptedException {
			AtomicReference<String> threadName = new AtomicReference<>();
			Scheduler acquireScheduler = Schedulers.newSingle("acquire");
			Scheduler releaseScheduler = Schedulers.fromExecutorService(
					Executors.newSingleThreadScheduledExecutor((r -> new Thread(r,"release"))));
			InstrumentedPool<PoolableTest> pool = poolableTestBuilder(1, 1,
					Mono.fromCallable(PoolableTest::new)
						.subscribeOn(Schedulers.newParallel("poolable test allocator")))
.buildPool();

			//the pool is started with one elements, and has capacity for 1.
			//we actually first acquire that element so that next acquire will wait for a release
			PooledRef<PoolableTest> uniqueSlot = pool.acquire().block();
			assertThat(uniqueSlot).isNotNull();

			//we prepare next acquire
			Mono<PoolableTest> borrower = Mono.fromDirect(pool.withPoolable(Mono::just));
			CountDownLatch latch = new CountDownLatch(1);

			//we actually perform the acquire from its dedicated thread, capturing the thread on which the element will actually get delivered
			acquireScheduler.schedule(() -> borrower.subscribe(v -> threadName.set(Thread.currentThread().getName()),
					e -> latch.countDown(), latch::countDown));
			//after a short while, we release the acquired unique element from a third thread
			releaseScheduler.schedule(uniqueSlot.release()::block, 500, TimeUnit.MILLISECONDS);
			latch.await(1, TimeUnit.SECONDS);

			assertThat(threadName.get())
					.isEqualTo("release");
		}

		@Test
		@Tag("loops")
		void defaultThreadDeliveringWhenNoElementsAndFullAndRaceDrain_loop() throws InterruptedException {
			AtomicInteger releaserWins = new AtomicInteger();
			AtomicInteger borrowerWins = new AtomicInteger();

			for (int i = 0; i < 100; i++) {
				defaultThreadDeliveringWhenNoElementsAndFullAndRaceDrain(i, releaserWins, borrowerWins);
			}
			//look at the stats and show them in case of assertion error. We expect all deliveries to be on either of the racer threads.
			//we expect a subset of the deliveries to happen on the second borrower's thread
			String stats = "releaser won " + releaserWins.get() + ", borrower won " + borrowerWins.get();
			assertThat(releaserWins.get()).as("releaser should win some. " + stats).isPositive();
			assertThat(releaserWins.get() + borrowerWins.get()).as(stats).isEqualTo(100);
		}

		@Test
		void defaultThreadDeliveringWhenNoElementsAndFullAndRaceDrain() throws InterruptedException {
			AtomicInteger releaserWins = new AtomicInteger();
			AtomicInteger borrowerWins = new AtomicInteger();

			defaultThreadDeliveringWhenNoElementsAndFullAndRaceDrain(0, releaserWins, borrowerWins);

			assertThat(releaserWins.get() + borrowerWins.get()).isEqualTo(1);
		}

		void defaultThreadDeliveringWhenNoElementsAndFullAndRaceDrain(int round, AtomicInteger releaserWins, AtomicInteger borrowerWins) throws InterruptedException {
			AtomicReference<String> threadName = new AtomicReference<>();
			AtomicInteger newCount = new AtomicInteger();
			Scheduler acquire1Scheduler = Schedulers.newSingle("acquire1");
			Scheduler racerReleaseScheduler = Schedulers.fromExecutorService(
					Executors.newSingleThreadScheduledExecutor((r -> new Thread(r,"racerRelease"))));
			Scheduler racerAcquireScheduler = Schedulers.fromExecutorService(
					Executors.newSingleThreadScheduledExecutor((r -> new Thread(r,"racerAcquire"))));
			Scheduler allocatorScheduler = Schedulers.newParallel("poolable test allocator");

			try {
				InstrumentedPool<PoolableTest> pool = poolableTestBuilder(1, 1,
						Mono.fromCallable(() -> new PoolableTest(newCount.getAndIncrement()))
							.subscribeOn(allocatorScheduler))
.buildPool();

				//the pool is started with one elements, and has capacity for 1.
				//we actually first acquire that element so that next acquire will wait for a release
				PooledRef<PoolableTest> uniqueSlot = pool.acquire().block();
				assertThat(uniqueSlot).isNotNull();

				//we prepare next acquire
				Mono<PoolableTest> borrower = Mono.fromDirect(pool.withPoolable(Mono::just));
				CountDownLatch latch = new CountDownLatch(3);

				//we actually perform the acquire from its dedicated thread, capturing the thread on which the element will actually get delivered
				acquire1Scheduler.schedule(() -> borrower.subscribe(v -> threadName.set(Thread.currentThread().getName())
						, e -> latch.countDown(), latch::countDown));

				//in parallel, we'll both attempt concurrent acquire AND release the unique element (each on their dedicated threads)
				racerAcquireScheduler.schedule(() -> {
					pool.acquire().block();
					latch.countDown();
				}, 100, TimeUnit.MILLISECONDS);
				racerReleaseScheduler.schedule(() -> {
					uniqueSlot.release().block();
					latch.countDown();
				}, 100, TimeUnit.MILLISECONDS);

				assertThat(latch.await(1, TimeUnit.SECONDS)).as("1s").isTrue();

				assertThat(newCount).as("created 1 poolable in round " + round).hasValue(1);

				//we expect that sometimes the race will let the second borrower thread drain, which would mean first borrower
				//will get the element delivered from racerAcquire thread. Yet the rest of the time it would get drained by racerRelease.
				if (threadName.get().startsWith("racerRelease")) releaserWins.incrementAndGet();
				else if (threadName.get().startsWith("racerAcquire")) borrowerWins.incrementAndGet();
				else System.out.println(threadName.get());
			}
			finally {
				acquire1Scheduler.dispose();
				racerAcquireScheduler.dispose();
				racerReleaseScheduler.dispose();
				allocatorScheduler.dispose();
			}
		}

		@Test
		void consistentThreadDeliveringWhenHasElements() throws InterruptedException {
			Scheduler deliveryScheduler = Schedulers.newSingle("delivery");
			AtomicReference<String> threadName = new AtomicReference<>();
			Scheduler acquireScheduler = Schedulers.newSingle("acquire");
			InstrumentedPool<PoolableTest> pool = poolableTestBuilder(1, 1,
					Mono.fromCallable(PoolableTest::new)
						.subscribeOn(Schedulers.newParallel("poolable test allocator")),
					deliveryScheduler)
.buildPool();

			//the pool is started with one available element
			//we prepare to acquire it
			Mono<PoolableTest> borrower = Mono.fromDirect(pool.withPoolable(Mono::just));
			CountDownLatch latch = new CountDownLatch(1);

			//we actually request the acquire from a separate thread and see from which thread the element was delivered
			acquireScheduler.schedule(() -> borrower.subscribe(v -> threadName.set(Thread.currentThread().getName()), e -> latch.countDown(), latch::countDown));
			latch.await(1, TimeUnit.SECONDS);

			assertThat(threadName.get())
					.startsWith("delivery-");
		}

		@Test
		void consistentThreadDeliveringWhenNoElementsButNotFull() throws InterruptedException {
			Scheduler deliveryScheduler = Schedulers.newSingle("delivery");
			AtomicReference<String> threadName = new AtomicReference<>();
			Scheduler acquireScheduler = Schedulers.newSingle("acquire");
			InstrumentedPool<PoolableTest> pool = poolableTestBuilder(0, 1,
					Mono.fromCallable(PoolableTest::new)
						.subscribeOn(Schedulers.newParallel("poolable test allocator")),
					deliveryScheduler)
.buildPool();

			//the pool is started with no elements, and has capacity for 1
			//we prepare to acquire, which would allocate the element
			Mono<PoolableTest> borrower = Mono.fromDirect(pool.withPoolable(Mono::just));
			CountDownLatch latch = new CountDownLatch(1);

			//we actually request the acquire from a separate thread, but the allocation also happens in a dedicated thread
			//we look at which thread the element was delivered from
			acquireScheduler.schedule(() -> borrower.subscribe(v -> threadName.set(Thread.currentThread().getName()), e -> latch.countDown(), latch::countDown));
			latch.await(1, TimeUnit.SECONDS);

			assertThat(threadName.get())
					.startsWith("delivery-");
		}

		@Test
		void consistentThreadDeliveringWhenNoElementsAndFull() throws InterruptedException {
			Scheduler deliveryScheduler = Schedulers.newSingle("delivery");
			AtomicReference<String> threadName = new AtomicReference<>();
			Scheduler acquireScheduler = Schedulers.newSingle("acquire");
			Scheduler releaseScheduler = Schedulers.fromExecutorService(
					Executors.newSingleThreadScheduledExecutor((r -> new Thread(r,"release"))));
			InstrumentedPool<PoolableTest> pool = poolableTestBuilder(1, 1,
					Mono.fromCallable(PoolableTest::new)
						.subscribeOn(Schedulers.newParallel("poolable test allocator")),
					deliveryScheduler)
.buildPool();

			//the pool is started with one elements, and has capacity for 1.
			//we actually first acquire that element so that next acquire will wait for a release
			PooledRef<PoolableTest> uniqueSlot = pool.acquire().block();
			assertThat(uniqueSlot).isNotNull();

			//we prepare next acquire
			Mono<PoolableTest> borrower = Mono.fromDirect(pool.withPoolable(Mono::just));
			CountDownLatch latch = new CountDownLatch(1);

			//we actually perform the acquire from its dedicated thread, capturing the thread on which the element will actually get delivered
			acquireScheduler.schedule(() -> borrower.subscribe(v -> threadName.set(Thread.currentThread().getName()),
					e -> latch.countDown(), latch::countDown));
			//after a short while, we release the acquired unique element from a third thread
			releaseScheduler.schedule(uniqueSlot.release()::block, 500, TimeUnit.MILLISECONDS);
			latch.await(1, TimeUnit.SECONDS);

			assertThat(threadName.get())
					.startsWith("delivery-");
		}

		@Test
		@Tag("loops")
		void consistentThreadDeliveringWhenNoElementsAndFullAndRaceDrain_loop() throws InterruptedException {
			for (int i = 0; i < 100; i++) {
				consistentThreadDeliveringWhenNoElementsAndFullAndRaceDrain(i);
			}
		}

		@Test
		void consistentThreadDeliveringWhenNoElementsAndFullAndRaceDrain() throws InterruptedException {
			consistentThreadDeliveringWhenNoElementsAndFullAndRaceDrain(0);
		}

		void consistentThreadDeliveringWhenNoElementsAndFullAndRaceDrain(int i) throws InterruptedException {
			Scheduler deliveryScheduler = Schedulers.newSingle("delivery");
			AtomicReference<String> threadName = new AtomicReference<>();
			AtomicInteger newCount = new AtomicInteger();

			Scheduler acquire1Scheduler = Schedulers.newSingle("acquire1");
			Scheduler racerReleaseScheduler = Schedulers.fromExecutorService(
					Executors.newSingleThreadScheduledExecutor((r -> new Thread(r,"racerRelease"))));
			Scheduler racerAcquireScheduler = Schedulers.newSingle("racerAcquire");

			InstrumentedPool<PoolableTest> pool = poolableTestBuilder(1, 1,
					Mono.fromCallable(() -> new PoolableTest(newCount.getAndIncrement()))
						.subscribeOn(Schedulers.newParallel("poolable test allocator")),
					deliveryScheduler)
.buildPool();

			//the pool is started with one elements, and has capacity for 1.
			//we actually first acquire that element so that next acquire will wait for a release
			PooledRef<PoolableTest> uniqueSlot = pool.acquire().block();
			assertThat(uniqueSlot).isNotNull();

			//we prepare next acquire
			Mono<PoolableTest> borrower = Mono.fromDirect(pool.withPoolable(Mono::just));
			CountDownLatch latch = new CountDownLatch(1);

			//we actually perform the acquire from its dedicated thread, capturing the thread on which the element will actually get delivered
			acquire1Scheduler.schedule(() -> borrower.subscribe(v -> threadName.set(Thread.currentThread().getName())
					, e -> latch.countDown(), latch::countDown));

			//in parallel, we'll both attempt a second acquire AND release the unique element (each on their dedicated threads
			Mono<PooledRef<PoolableTest>> otherBorrower = pool.acquire();
			racerAcquireScheduler.schedule(() -> otherBorrower.subscribe().dispose(), 100, TimeUnit.MILLISECONDS);
			racerReleaseScheduler.schedule(uniqueSlot.release()::block, 100, TimeUnit.MILLISECONDS);
			latch.await(1, TimeUnit.SECONDS);

			//we expect that, consistently, the poolable is delivered on a `delivery` thread
			assertThat(threadName.get()).as("round #" + i).startsWith("delivery-");

			//we expect that only 1 element was created
			assertThat(newCount).as("elements created in round " + i).hasValue(1);
		}
	}

	@Test
	void stillacquiredAfterPoolDisposedMaintainsCount() {
		AtomicInteger cleanerCount = new AtomicInteger();
		InstrumentedPool<PoolableTest> pool =
				from(Mono.fromCallable(PoolableTest::new))
						.sizeBetween(3, 3)
						.releaseHandler(p -> Mono.fromRunnable(cleanerCount::incrementAndGet))
						.evictionPredicate((value, metadata) -> !value.isHealthy())
						.buildPool();

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

	@SuppressWarnings("FutureReturnValueIgnored")
	@TestUtils.ParameterizedTestWithName
	@CsvSource({"4, 1", "4, 100000", "10, 1", "10, 100000"})
	//see https://github.com/reactor/reactor-pool/issues/65
	void concurrentAcquireCorrectlyAccountsAll(int parallelism, int loops) throws InterruptedException {
		final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(parallelism);
		autoDispose(executorService::shutdownNow);

		for (int l = 0; l < loops; l++) {
			InstrumentedPool<String> fifoPool = autoDispose(
					PoolBuilder.from(Mono.just("foo"))
							   .sizeBetween(0, 100)
							   .buildPool());
			CountDownLatch latch = new CountDownLatch(parallelism);

			for (int i = 0; i < parallelism; i++) {
				executorService.submit(() -> {
					fifoPool.acquire()
							.block();
					latch.countDown();
				});
			}
			boolean awaited = latch.await(1, TimeUnit.SECONDS);
			assertThat(awaited).as("all concurrent acquire served in loop #" + l).isTrue();
		}
	}
}