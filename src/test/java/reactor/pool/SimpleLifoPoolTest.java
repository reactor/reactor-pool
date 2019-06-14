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
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;

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
 * @author Simon Baslé
 */
class SimpleLifoPoolTest {

    //FIXME extract lifo-specific tests into CommonPoolTest

    //==utils for package-private config==
    static final PoolConfig<PoolableTest> poolableTestConfig(int minSize, int maxSize, Mono<PoolableTest> allocator) {
        return from(allocator)
                .initialSize(minSize)
                .sizeMax(maxSize)
                .releaseHandler(pt -> Mono.fromRunnable(pt::clean))
                .evictionPredicate((value, metadata) -> !value.isHealthy())
                .buildConfig();
    }

    static final PoolConfig<PoolableTest> poolableTestConfig(int minSize, int maxSize, Mono<PoolableTest> allocator, Scheduler deliveryScheduler) {
        return from(allocator)
                .initialSize(minSize)
                .sizeMax(maxSize)
                .releaseHandler(pt -> Mono.fromRunnable(pt::clean))
                .evictionPredicate((value, metadata) -> !value.isHealthy())
                .acquisitionScheduler(deliveryScheduler)
                .buildConfig();
    }

    static final PoolConfig<PoolableTest> poolableTestConfig(int minSize, int maxSize, Mono<PoolableTest> allocator,
            Consumer<? super PoolableTest> additionalCleaner) {
        return from(allocator)
                .initialSize(minSize)
                .sizeMax(maxSize)
                .releaseHandler(poolableTest -> Mono.fromRunnable(() -> {
                    poolableTest.clean();
                    additionalCleaner.accept(poolableTest);
                }))
                .evictionPredicate((value, metadata) -> !value.isHealthy())
                .buildConfig();
    }
    //======

    @Test
    void demonstrateAcquireInScopePipeline() throws InterruptedException {
        AtomicInteger counter = new AtomicInteger();
        AtomicReference<String> releaseRef = new AtomicReference<>();

        SimpleLifoPool<String> pool = new SimpleLifoPool<>(
                from(Mono.just("Hello Reactive World"))
                        .sizeMax(1)
                        .releaseHandler(s -> Mono.fromRunnable(()-> releaseRef.set(s)))
                        .buildConfig());

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
        assertThat(pool.poolConfig.allocationStrategy.estimatePermitCount()).as("allocation permits").isZero();
        assertThat(pool.elements).as("available").hasSize(1);
        assertThat(releaseRef).as("released").hasValue("Hello Reactive World");
    }

    @Nested
    @DisplayName("Tests around the acquire() manual mode of acquiring")
    @SuppressWarnings("ClassCanBeStatic")
    class AcquireTest {

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

        void allocatedReleasedOrAbortedIfCancelRequestRace(int round, AtomicInteger newCount, AtomicInteger releasedCount, boolean cancelFirst) throws InterruptedException {
            Scheduler scheduler = Schedulers.newParallel("poolable test allocator");

            PoolConfig<PoolableTest> testConfig = poolableTestConfig(0, 1,
                    Mono.defer(() -> Mono.delay(Duration.ofMillis(50)).thenReturn(new PoolableTest(newCount.incrementAndGet())))
                        .subscribeOn(scheduler),
                    pt -> releasedCount.incrementAndGet());
            SimpleLifoPool<PoolableTest> pool = new SimpleLifoPool<>(testConfig);

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
            PoolConfig<PoolableTest> testConfig = poolableTestConfig(1, 1,
                    Mono.fromCallable(PoolableTest::new)
                        .subscribeOn(Schedulers.newParallel("poolable test allocator")));
            SimpleLifoPool<PoolableTest> pool = new SimpleLifoPool<>(testConfig);

            //the pool is started with one available element
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
            PoolConfig<PoolableTest> testConfig = poolableTestConfig(0, 1,
                    Mono.fromCallable(PoolableTest::new)
                        .subscribeOn(Schedulers.newParallel("poolable test allocator")));
            SimpleLifoPool<PoolableTest> pool = new SimpleLifoPool<>(testConfig);

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
            PoolConfig<PoolableTest> testConfig = poolableTestConfig(1, 1,
                    Mono.fromCallable(PoolableTest::new)
                        .subscribeOn(Schedulers.newParallel("poolable test allocator")));
            SimpleLifoPool<PoolableTest> pool = new SimpleLifoPool<>(testConfig);

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

        //TODO add back acquire/release race tests? these are way harder with LIFO semantics

        @Test
        void consistentThreadDeliveringWhenHasElements() throws InterruptedException {
            Scheduler deliveryScheduler = Schedulers.newSingle("delivery");
            AtomicReference<String> threadName = new AtomicReference<>();
            Scheduler acquireScheduler = Schedulers.newSingle("acquire");
            PoolConfig<PoolableTest> testConfig = poolableTestConfig(1, 1,
                    Mono.fromCallable(PoolableTest::new)
                        .subscribeOn(Schedulers.newParallel("poolable test allocator")),
                    deliveryScheduler);
            SimpleLifoPool<PoolableTest> pool = new SimpleLifoPool<>(testConfig);

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
            PoolConfig<PoolableTest> testConfig = poolableTestConfig(0, 1,
                    Mono.fromCallable(PoolableTest::new)
                        .subscribeOn(Schedulers.newParallel("poolable test allocator")),
                    deliveryScheduler);
            SimpleLifoPool<PoolableTest> pool = new SimpleLifoPool<>(testConfig);

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
            PoolConfig<PoolableTest> testConfig = poolableTestConfig(1, 1,
                    Mono.fromCallable(PoolableTest::new)
                        .subscribeOn(Schedulers.newParallel("poolable test allocator")),
                    deliveryScheduler);
            SimpleLifoPool<PoolableTest> pool = new SimpleLifoPool<>(testConfig);

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
            for (int i = 0; i < 10_000; i++) {
                consistentThreadDeliveringWhenNoElementsAndFullAndRaceDrain(i);
            }
        }

        @Test
        void consistentThreadDeliveringWhenNoElementsAndFullAndRaceDrain() throws InterruptedException {
            consistentThreadDeliveringWhenNoElementsAndFullAndRaceDrain(0);
        }

        void consistentThreadDeliveringWhenNoElementsAndFullAndRaceDrain(int i) throws InterruptedException {
            Scheduler allocatorScheduler = Schedulers.newParallel("poolable test allocator");
            Scheduler deliveryScheduler = Schedulers.newSingle("delivery");
            Scheduler acquire1Scheduler = Schedulers.newSingle("acquire1");
            Scheduler racerScheduler = Schedulers.fromExecutorService(Executors.newFixedThreadPool(2, r -> new Thread(r, "racer")));
            try {
                AtomicReference<String> threadName = new AtomicReference<>();
                AtomicInteger newCount = new AtomicInteger();

                PoolConfig<PoolableTest> testConfig = poolableTestConfig(1, 1,
                        Mono.fromCallable(() -> new PoolableTest(newCount.getAndIncrement()))
                            .subscribeOn(allocatorScheduler),
                        deliveryScheduler);
                SimpleLifoPool<PoolableTest> pool = new SimpleLifoPool<>(testConfig);

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
                RaceTestUtils.race(() -> secondBorrower.subscribe(v -> threadName.compareAndSet(null, Thread.currentThread().getName())
                        , e -> latch.countDown(), latch::countDown),
                        uniqueSlot.release()::block);

                latch.await(1, TimeUnit.SECONDS);

                //we expect that, consistently, the poolable is delivered on a `delivery` thread
                assertThat(threadName.get()).as("round #" + i).startsWith("delivery-");

                //we expect that only 1 element was created
                assertThat(newCount).as("elements created in round " + i).hasValue(1);
            }
            finally {
                allocatorScheduler.dispose();
                deliveryScheduler.dispose();
                racerScheduler.dispose();
                acquire1Scheduler.dispose();
            }
        }
    }

    @Nested
    @DisplayName("Tests around the withPoolable(Function) mode of acquiring")
    @SuppressWarnings("ClassCanBeStatic")
    class AcquireInScopeTest {

        @Test
        @DisplayName("acquire delays instead of allocating past maxSize")
        void acquireDelaysNotAllocate() {
            AtomicInteger newCount = new AtomicInteger();
            SimpleLifoPool<PoolableTest> pool = new SimpleLifoPool<>(poolableTestConfig(2, 3,
                    Mono.defer(() -> Mono.just(new PoolableTest(newCount.incrementAndGet())))));

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

        void allocatedReleasedOrAbortedIfCancelRequestRace(int round, AtomicInteger newCount, AtomicInteger releasedCount, boolean cancelFirst) throws InterruptedException {
            Scheduler scheduler = Schedulers.newParallel("poolable test allocator");

            PoolConfig<PoolableTest> testConfig = poolableTestConfig(0, 1,
                    Mono.defer(() -> Mono.delay(Duration.ofMillis(50)).thenReturn(new PoolableTest(newCount.incrementAndGet())))
                        .subscribeOn(scheduler),
                    pt -> releasedCount.incrementAndGet());
            SimpleLifoPool<PoolableTest> pool = new SimpleLifoPool<>(testConfig);

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
            PoolConfig<PoolableTest> testConfig = poolableTestConfig(1, 1,
                    Mono.fromCallable(PoolableTest::new)
                        .subscribeOn(Schedulers.newParallel("poolable test allocator")));
            SimpleLifoPool<PoolableTest> pool = new SimpleLifoPool<>(testConfig);

            //the pool is started with one available element
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
            PoolConfig<PoolableTest> testConfig = poolableTestConfig(0, 1,
                    Mono.fromCallable(PoolableTest::new)
                        .subscribeOn(Schedulers.newParallel("poolable test allocator")));
            SimpleLifoPool<PoolableTest> pool = new SimpleLifoPool<>(testConfig);

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
            PoolConfig<PoolableTest> testConfig = poolableTestConfig(1, 1,
                    Mono.fromCallable(PoolableTest::new)
                        .subscribeOn(Schedulers.newParallel("poolable test allocator")));
            SimpleLifoPool<PoolableTest> pool = new SimpleLifoPool<>(testConfig);

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

        //TODO add back acquire/release race tests? these are way harder with LIFO semantics

        @Test
        void consistentThreadDeliveringWhenHasElements() throws InterruptedException {
            Scheduler deliveryScheduler = Schedulers.newSingle("delivery");
            AtomicReference<String> threadName = new AtomicReference<>();
            Scheduler acquireScheduler = Schedulers.newSingle("acquire");
            PoolConfig<PoolableTest> testConfig = poolableTestConfig(1, 1,
                    Mono.fromCallable(PoolableTest::new)
                        .subscribeOn(Schedulers.newParallel("poolable test allocator")),
                    deliveryScheduler);
            SimpleLifoPool<PoolableTest> pool = new SimpleLifoPool<>(testConfig);

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
            PoolConfig<PoolableTest> testConfig = poolableTestConfig(0, 1,
                    Mono.fromCallable(PoolableTest::new)
                        .subscribeOn(Schedulers.newParallel("poolable test allocator")),
                    deliveryScheduler);
            SimpleLifoPool<PoolableTest> pool = new SimpleLifoPool<>(testConfig);

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
            PoolConfig<PoolableTest> testConfig = poolableTestConfig(1, 1,
                    Mono.fromCallable(PoolableTest::new)
                        .subscribeOn(Schedulers.newParallel("poolable test allocator")),
                    deliveryScheduler);
            SimpleLifoPool<PoolableTest> pool = new SimpleLifoPool<>(testConfig);

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
            for (int i = 0; i < 10_000; i++) {
                consistentThreadDeliveringWhenNoElementsAndFullAndRaceDrain(i);
            }
        }

        @Test
        void consistentThreadDeliveringWhenNoElementsAndFullAndRaceDrain() throws InterruptedException {
            consistentThreadDeliveringWhenNoElementsAndFullAndRaceDrain(0);
        }

        void consistentThreadDeliveringWhenNoElementsAndFullAndRaceDrain(int i) throws InterruptedException {
            Scheduler allocatorScheduler = Schedulers.newParallel("poolable test allocator");
            Scheduler deliveryScheduler = Schedulers.newSingle("delivery");
            Scheduler acquire1Scheduler = Schedulers.newSingle("acquire1");
            Scheduler racerScheduler = Schedulers.fromExecutorService(
                    Executors.newFixedThreadPool(2, (r -> new Thread(r,"racer"))));

            try {
                AtomicReference<String> threadName = new AtomicReference<>();
                AtomicInteger newCount = new AtomicInteger();


                PoolConfig<PoolableTest> testConfig = poolableTestConfig(1, 1,
                        Mono.fromCallable(() -> new PoolableTest(newCount.getAndIncrement()))
                            .subscribeOn(allocatorScheduler),
                        deliveryScheduler);
                SimpleLifoPool<PoolableTest> pool = new SimpleLifoPool<>(testConfig);

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
                RaceTestUtils.race(
                        () -> otherBorrower.subscribe(v -> threadName.set(Thread.currentThread().getName())
                                , e -> latch.countDown(), latch::countDown),
                        () -> {
                            uniqueSlot.release().block();
                            latch.countDown();
                        },
                        racerScheduler);
                latch.await(1, TimeUnit.SECONDS);

                //we expect that, consistently, the poolable is delivered on a `delivery` thread
                assertThat(threadName.get()).as("round #" + i).startsWith("delivery-");

                //2 elements MIGHT be created if the first acquire wins (since we're in auto-release mode)
                assertThat(newCount.get()).as("1 or 2 elements created in round " + i).isIn(1, 2);
            }
            finally {
                allocatorScheduler.dispose();
                deliveryScheduler.dispose();
                acquire1Scheduler.dispose();
                racerScheduler.dispose();
            }
        }
    }

    @Test
    void stillacquiredAfterPoolDisposedMaintainsCount() {
        AtomicInteger cleanerCount = new AtomicInteger();
        SimpleLifoPool<PoolableTest> pool = new SimpleLifoPool<>(
                from(Mono.fromCallable(PoolableTest::new))
                        .initialSize(3)
                        .sizeMax(3)
                        .releaseHandler(p -> Mono.fromRunnable(cleanerCount::incrementAndGet))
                        .evictionPredicate((value, metadata) -> !value.isHealthy())
                        .buildConfig());

        PooledRef<PoolableTest> acquired1 = pool.acquire().block();
        PooledRef<PoolableTest> acquired2 = pool.acquire().block();
        PooledRef<PoolableTest> acquired3 = pool.acquire().block();

        assertThat(acquired1).as("acquired1").isNotNull();
        assertThat(acquired2).as("acquired2").isNotNull();
        assertThat(acquired3).as("acquired3").isNotNull();

        pool.dispose();

        assertThat(pool.acquired).as("before releases").isEqualTo(3);

        acquired1.release().block();
        acquired2.release().block();
        acquired3.release().block();

        assertThat(pool.acquired).as("after releases").isEqualTo(0);
    }
}