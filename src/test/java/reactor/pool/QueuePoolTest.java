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
import reactor.pool.AbstractPool.DefaultPoolConfig;
import reactor.pool.TestUtils.PoolableTest;
import reactor.pool.util.AllocationStrategies;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.test.util.TestLogger;
import reactor.util.Loggers;
import reactor.util.function.Tuple2;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static reactor.pool.PoolBuilder.from;
import static reactor.pool.util.AllocationStrategies.allocatingMax;

/**
 * @author Simon Baslé
 */
class QueuePoolTest {

    //==utils for package-private config==
    static final DefaultPoolConfig<PoolableTest> poolableTestConfig(int minSize, int maxSize, Mono<PoolableTest> allocator) {
        return from(allocator)
                .initialSize(minSize)
                .allocationStrategy(AllocationStrategies.allocatingMax(maxSize))
                .releaseHandler(pt -> Mono.fromRunnable(pt::clean))
                .evictionPredicate(slot -> !slot.poolable().isHealthy())
                .buildConfig();
    }

    static final DefaultPoolConfig<PoolableTest> poolableTestConfig(int minSize, int maxSize, Mono<PoolableTest> allocator, Scheduler deliveryScheduler) {
        return from(allocator)
                .initialSize(minSize)
                .allocationStrategy(AllocationStrategies.allocatingMax(maxSize))
                .releaseHandler(pt -> Mono.fromRunnable(pt::clean))
                .evictionPredicate(slot -> !slot.poolable().isHealthy())
                .acquisitionScheduler(deliveryScheduler)
                .buildConfig();
    }

    static final DefaultPoolConfig<PoolableTest> poolableTestConfig(int minSize, int maxSize, Mono<PoolableTest> allocator,
            Consumer<? super PoolableTest> additionalCleaner) {
        return from(allocator)
                .initialSize(minSize)
                .allocationStrategy(AllocationStrategies.allocatingMax(maxSize))
                .releaseHandler(poolableTest -> Mono.fromRunnable(() -> {
                    poolableTest.clean();
                    additionalCleaner.accept(poolableTest);
                }))
                .evictionPredicate(slot -> !slot.poolable().isHealthy())
                .buildConfig();
    }
    //======

    @Test
    void demonstrateAcquireInScopePipeline() throws InterruptedException {
        AtomicInteger counter = new AtomicInteger();
        AtomicReference<String> releaseRef = new AtomicReference<>();

        QueuePool<String> pool = new QueuePool<>(
                from(Mono.just("Hello Reactive World"))
                        .allocationStrategy(allocatingMax(1))
                        .releaseHandler(s -> Mono.fromRunnable(()-> releaseRef.set(s)))
                        .buildConfig());

        Flux<String> words = pool.acquireInScope(m -> m
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
        void smokeTest() throws InterruptedException {
            AtomicInteger newCount = new AtomicInteger();
            QueuePool<PoolableTest> pool = new QueuePool<>(poolableTestConfig(2, 3,
                    Mono.defer(() -> Mono.just(new PoolableTest(newCount.incrementAndGet())))));

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

        @Test
        void smokeTestAsync() throws InterruptedException {
            AtomicInteger newCount = new AtomicInteger();
            QueuePool<PoolableTest> pool = new QueuePool<>(poolableTestConfig(2, 3,
                    Mono.defer(() -> Mono.just(new PoolableTest(newCount.incrementAndGet())))
                        .subscribeOn(Schedulers.newParallel("poolable test allocator"))));

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

            if (latch3.await(2, TimeUnit.SECONDS)) { //wait for the re-creation of max elements

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

        @Test
        void returnedReleasedIfBorrowerCancelled() {
            AtomicInteger releasedCount = new AtomicInteger();

            DefaultPoolConfig<PoolableTest> testConfig = poolableTestConfig(1, 1,
                    Mono.fromCallable(PoolableTest::new),
                    pt -> releasedCount.incrementAndGet());
            QueuePool<PoolableTest> pool = new QueuePool<>(testConfig);

            //acquire the only element
            PooledRef<PoolableTest> slot = pool.acquire().block();
            assertThat(slot).isNotNull();

            pool.acquire().subscribe().dispose();

            assertThat(releasedCount).as("before returning").hasValue(0);

            //release the element, which should forward to the cancelled second acquire, itself also cleaning
            slot.release().block();

            assertThat(releasedCount).as("after returning").hasValue(2);
        }

        @Test
        void allocatedReleasedIfBorrowerCancelled() {
            Scheduler scheduler = Schedulers.newParallel("poolable test allocator");
            AtomicInteger newCount = new AtomicInteger();
            AtomicInteger releasedCount = new AtomicInteger();

            DefaultPoolConfig<PoolableTest> testConfig = poolableTestConfig(0, 1,
                    Mono.defer(() -> Mono.delay(Duration.ofMillis(50)).thenReturn(new PoolableTest(newCount.incrementAndGet())))
                        .subscribeOn(scheduler),
                    pt -> releasedCount.incrementAndGet());
            QueuePool<PoolableTest> pool = new QueuePool<>(testConfig);

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

            DefaultPoolConfig<PoolableTest> testConfig = poolableTestConfig(0, 1,
                    Mono.defer(() -> Mono.delay(Duration.ofMillis(50)).thenReturn(new PoolableTest(newCount.incrementAndGet())))
                        .subscribeOn(scheduler),
                    pt -> releasedCount.incrementAndGet());
            QueuePool<PoolableTest> pool = new QueuePool<>(testConfig);

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
        void cleanerFunctionError() {
            DefaultPoolConfig<PoolableTest> testConfig = poolableTestConfig(0, 1, Mono.fromCallable(PoolableTest::new),
                    pt -> { throw new IllegalStateException("boom"); });
            QueuePool<PoolableTest> pool = new QueuePool<>(testConfig);

            PooledRef<PoolableTest> slot = pool.acquire().block();

            assertThat(slot).isNotNull();

            StepVerifier.create(slot.release())
                        .verifyErrorMessage("boom");
        }

        @Test
        void cleanerFunctionErrorDiscards() {
            DefaultPoolConfig<PoolableTest> testConfig = poolableTestConfig(0, 1, Mono.fromCallable(PoolableTest::new),
                    pt -> { throw new IllegalStateException("boom"); });
            QueuePool<PoolableTest> pool = new QueuePool<>(testConfig);

            PooledRef<PoolableTest> slot = pool.acquire().block();

            assertThat(slot).isNotNull();

            StepVerifier.create(slot.release())
                        .verifyErrorMessage("boom");

            assertThat(slot.poolable().discarded).as("discarded despite cleaner error").isEqualTo(1);
        }

        @Test
        void defaultThreadDeliveringWhenHasElements() throws InterruptedException {
            AtomicReference<String> threadName = new AtomicReference<>();
            Scheduler acquireScheduler = Schedulers.newSingle("acquire");
            DefaultPoolConfig<PoolableTest> testConfig = poolableTestConfig(1, 1,
                    Mono.fromCallable(PoolableTest::new)
                        .subscribeOn(Schedulers.newParallel("poolable test allocator")));
            QueuePool<PoolableTest> pool = new QueuePool<>(testConfig);

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
            DefaultPoolConfig<PoolableTest> testConfig = poolableTestConfig(0, 1,
                    Mono.fromCallable(PoolableTest::new)
                        .subscribeOn(Schedulers.newParallel("poolable test allocator")));
            QueuePool<PoolableTest> pool = new QueuePool<>(testConfig);

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
            DefaultPoolConfig<PoolableTest> testConfig = poolableTestConfig(1, 1,
                    Mono.fromCallable(PoolableTest::new)
                        .subscribeOn(Schedulers.newParallel("poolable test allocator")));
            QueuePool<PoolableTest> pool = new QueuePool<>(testConfig);

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
            assertThat(borrowerWins.get()).as(stats).isPositive();
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

            DefaultPoolConfig<PoolableTest> testConfig = poolableTestConfig(1, 1,
                    Mono.fromCallable(() -> new PoolableTest(newCount.getAndIncrement()))
                        .subscribeOn(Schedulers.newParallel("poolable test allocator")));

            QueuePool<PoolableTest> pool = new QueuePool<>(testConfig);

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
            DefaultPoolConfig<PoolableTest> testConfig = poolableTestConfig(1, 1,
                    Mono.fromCallable(PoolableTest::new)
                        .subscribeOn(Schedulers.newParallel("poolable test allocator")),
                    deliveryScheduler);
            QueuePool<PoolableTest> pool = new QueuePool<>(testConfig);

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
            DefaultPoolConfig<PoolableTest> testConfig = poolableTestConfig(0, 1,
                    Mono.fromCallable(PoolableTest::new)
                        .subscribeOn(Schedulers.newParallel("poolable test allocator")),
                    deliveryScheduler);
            QueuePool<PoolableTest> pool = new QueuePool<>(testConfig);

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
            DefaultPoolConfig<PoolableTest> testConfig = poolableTestConfig(1, 1,
                    Mono.fromCallable(PoolableTest::new)
                        .subscribeOn(Schedulers.newParallel("poolable test allocator")),
                    deliveryScheduler);
            QueuePool<PoolableTest> pool = new QueuePool<>(testConfig);

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

            DefaultPoolConfig<PoolableTest> testConfig = poolableTestConfig(1, 1,
                    Mono.fromCallable(() -> new PoolableTest(newCount.getAndIncrement()))
                        .subscribeOn(Schedulers.newParallel("poolable test allocator")),
                    deliveryScheduler);
            QueuePool<PoolableTest> pool = new QueuePool<>(testConfig);

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
    }

    @Nested
    @DisplayName("Tests around the acquireInScope(Function) mode of acquiring")
    @SuppressWarnings("ClassCanBeStatic")
    class AcquireInScopeTest {

        @Test
        @DisplayName("acquire delays instead of allocating past maxSize")
        void acquireDelaysNotAllocate() {
            AtomicInteger newCount = new AtomicInteger();
            QueuePool<PoolableTest> pool = new QueuePool<>(poolableTestConfig(2, 3,
                    Mono.defer(() -> Mono.just(new PoolableTest(newCount.incrementAndGet())))));

            pool.acquireInScope(mono -> mono.delayElement(Duration.ofMillis(500))).subscribe();
            pool.acquireInScope(mono -> mono.delayElement(Duration.ofMillis(500))).subscribe();
            pool.acquireInScope(mono -> mono.delayElement(Duration.ofMillis(500))).subscribe();

            final Tuple2<Long, PoolableTest> tuple2 = pool.acquireInScope(mono -> mono).elapsed().blockLast();

            assertThat(tuple2).isNotNull();

            assertThat(tuple2.getT1()).as("pending for 500ms").isCloseTo(500L, Offset.offset(50L));
            assertThat(tuple2.getT2().usedUp).as("discarded twice").isEqualTo(2);
            assertThat(tuple2.getT2().id).as("id").isLessThan(4);
        }

        @Test
        void smokeTest() {
            AtomicInteger newCount = new AtomicInteger();
            QueuePool<PoolableTest> pool = new QueuePool<>(poolableTestConfig(2, 3,
                    Mono.defer(() -> Mono.just(new PoolableTest(newCount.incrementAndGet())))));
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
                    pool.acquireInScope(mono -> mono.doOnNext(acquired2::add).delayUntil(__ -> trigger2)),
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

        @Test
        @DisplayName("Cancelling a pending acquireInScope() results in it performing immediate cleanup when borrower releases")
        void returnedReleasedIfBorrowerCancelled() {
            AtomicInteger releasedCount = new AtomicInteger();

            DefaultPoolConfig<PoolableTest> testConfig = poolableTestConfig(1, 1,
                    Mono.fromCallable(PoolableTest::new),
                    pt -> releasedCount.incrementAndGet());
            QueuePool<PoolableTest> pool = new QueuePool<>(testConfig);

            //acquire the only element
            PooledRef<PoolableTest> slot = pool.acquire().block();
            assertThat(slot).isNotNull();

            pool.acquireInScope(mono -> mono).subscribe().dispose();

            assertThat(releasedCount).as("before returning").hasValue(0);

            //release the element, which should forward to the cancelled second acquire, itself also cleaning
            slot.release().block();

            assertThat(releasedCount).as("after returning").hasValue(2);
        }

        @Test
        @DisplayName("Cancelling a pending acquireInScope() results in it performing immediate cleanup when allocator emits")
        void allocatedReleasedIfBorrowerCancelled() {
            Scheduler scheduler = Schedulers.newParallel("poolable test allocator");
            AtomicInteger newCount = new AtomicInteger();
            AtomicInteger releasedCount = new AtomicInteger();

            DefaultPoolConfig<PoolableTest> testConfig = poolableTestConfig(0, 1,
                    Mono.defer(() -> Mono.delay(Duration.ofMillis(50)).thenReturn(new PoolableTest(newCount.incrementAndGet())))
                        .subscribeOn(scheduler),
                    pt -> releasedCount.incrementAndGet());
            QueuePool<PoolableTest> pool = new QueuePool<>(testConfig);

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

            DefaultPoolConfig<PoolableTest> testConfig = poolableTestConfig(0, 1,
                    Mono.defer(() -> Mono.delay(Duration.ofMillis(50)).thenReturn(new PoolableTest(newCount.incrementAndGet())))
                        .subscribeOn(scheduler),
                    pt -> releasedCount.incrementAndGet());
            QueuePool<PoolableTest> pool = new QueuePool<>(testConfig);

            //acquire the only element and capture the subscription, don't request just yet
            CountDownLatch latch = new CountDownLatch(1);
            final BaseSubscriber<PoolableTest> baseSubscriber = new BaseSubscriber<PoolableTest>() {
                @Override
                protected void hookOnSubscribe(Subscription subscription) {
                    //don't request
                    latch.countDown();
                }
            };
            pool.acquireInScope(mono -> mono).subscribe(baseSubscriber);
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
        void cleanerFunctionError() {
            DefaultPoolConfig<PoolableTest> testConfig = poolableTestConfig(0, 1, Mono.fromCallable(PoolableTest::new),
                    pt -> { throw new IllegalStateException("boom"); });
            QueuePool<PoolableTest> pool = new QueuePool<>(testConfig);

            StepVerifier.create(pool.acquireInScope(mono -> mono))
                        .expectNextCount(1).as("element still emitted")
                        .verifyErrorSatisfies(t -> assertThat(t).hasMessage("Async resource cleanup failed after onComplete")
                                                                .hasCause(new IllegalStateException("boom")));
        }

        @Test
        void cleanerFunctionErrorDiscards() {
            DefaultPoolConfig<PoolableTest> testConfig = poolableTestConfig(0, 1, Mono.fromCallable(PoolableTest::new),
                    pt -> { throw new IllegalStateException("boom"); });
            QueuePool<PoolableTest> pool = new QueuePool<>(testConfig);
            AtomicReference<Throwable> errorRef = new AtomicReference<>();

            PoolableTest poolable = pool.acquireInScope(mono -> mono)
                                        .onErrorResume(error -> {
                                            errorRef.set(error);
                                            return Mono.empty();
                                        })
                                        .blockLast();

            assertThat(poolable)
                    .isNotNull()
                    .matches(p -> p.discarded == 1L, "discarded despite cleaner error");
        }

        @Test
        void defaultThreadDeliveringWhenHasElements() throws InterruptedException {
            AtomicReference<String> threadName = new AtomicReference<>();
            Scheduler acquireScheduler = Schedulers.newSingle("acquire");
            DefaultPoolConfig<PoolableTest> testConfig = poolableTestConfig(1, 1,
                    Mono.fromCallable(PoolableTest::new)
                        .subscribeOn(Schedulers.newParallel("poolable test allocator")));
            QueuePool<PoolableTest> pool = new QueuePool<>(testConfig);

            //the pool is started with one available element
            //we prepare to acquire it
            Mono<PoolableTest> borrower = Mono.fromDirect(pool.acquireInScope(mono -> mono));
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
            DefaultPoolConfig<PoolableTest> testConfig = poolableTestConfig(0, 1,
                    Mono.fromCallable(PoolableTest::new)
                        .subscribeOn(Schedulers.newParallel("poolable test allocator")));
            QueuePool<PoolableTest> pool = new QueuePool<>(testConfig);

            //the pool is started with no elements, and has capacity for 1
            //we prepare to acquire, which would allocate the element
            Mono<PoolableTest> borrower = Mono.fromDirect(pool.acquireInScope(mono -> mono));
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
            DefaultPoolConfig<PoolableTest> testConfig = poolableTestConfig(1, 1,
                    Mono.fromCallable(PoolableTest::new)
                        .subscribeOn(Schedulers.newParallel("poolable test allocator")));
            QueuePool<PoolableTest> pool = new QueuePool<>(testConfig);

            //the pool is started with one elements, and has capacity for 1.
            //we actually first acquire that element so that next acquire will wait for a release
            PooledRef<PoolableTest> uniqueSlot = pool.acquire().block();
            assertThat(uniqueSlot).isNotNull();

            //we prepare next acquire
            Mono<PoolableTest> borrower = Mono.fromDirect(pool.acquireInScope(mono -> mono));
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
            assertThat(borrowerWins.get()).as(stats).isPositive();
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

            DefaultPoolConfig<PoolableTest> testConfig = poolableTestConfig(1, 1,
                    Mono.fromCallable(() -> new PoolableTest(newCount.getAndIncrement()))
                        .subscribeOn(Schedulers.newParallel("poolable test allocator")));

            QueuePool<PoolableTest> pool = new QueuePool<>(testConfig);

            //the pool is started with one elements, and has capacity for 1.
            //we actually first acquire that element so that next acquire will wait for a release
            PooledRef<PoolableTest> uniqueSlot = pool.acquire().block();
            assertThat(uniqueSlot).isNotNull();

            //we prepare next acquire
            Mono<PoolableTest> borrower = Mono.fromDirect(pool.acquireInScope(mono -> mono));
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
            DefaultPoolConfig<PoolableTest> testConfig = poolableTestConfig(1, 1,
                    Mono.fromCallable(PoolableTest::new)
                        .subscribeOn(Schedulers.newParallel("poolable test allocator")),
                    deliveryScheduler);
            QueuePool<PoolableTest> pool = new QueuePool<>(testConfig);

            //the pool is started with one available element
            //we prepare to acquire it
            Mono<PoolableTest> borrower = Mono.fromDirect(pool.acquireInScope(mono -> mono));
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
            DefaultPoolConfig<PoolableTest> testConfig = poolableTestConfig(0, 1,
                    Mono.fromCallable(PoolableTest::new)
                        .subscribeOn(Schedulers.newParallel("poolable test allocator")),
                    deliveryScheduler);
            QueuePool<PoolableTest> pool = new QueuePool<>(testConfig);

            //the pool is started with no elements, and has capacity for 1
            //we prepare to acquire, which would allocate the element
            Mono<PoolableTest> borrower = Mono.fromDirect(pool.acquireInScope(mono -> mono));
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
            DefaultPoolConfig<PoolableTest> testConfig = poolableTestConfig(1, 1,
                    Mono.fromCallable(PoolableTest::new)
                        .subscribeOn(Schedulers.newParallel("poolable test allocator")),
                    deliveryScheduler);
            QueuePool<PoolableTest> pool = new QueuePool<>(testConfig);

            //the pool is started with one elements, and has capacity for 1.
            //we actually first acquire that element so that next acquire will wait for a release
            PooledRef<PoolableTest> uniqueSlot = pool.acquire().block();
            assertThat(uniqueSlot).isNotNull();

            //we prepare next acquire
            Mono<PoolableTest> borrower = Mono.fromDirect(pool.acquireInScope(mono -> mono));
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

            DefaultPoolConfig<PoolableTest> testConfig = poolableTestConfig(1, 1,
                    Mono.fromCallable(() -> new PoolableTest(newCount.getAndIncrement()))
                        .subscribeOn(Schedulers.newParallel("poolable test allocator")),
                    deliveryScheduler);
            QueuePool<PoolableTest> pool = new QueuePool<>(testConfig);

            //the pool is started with one elements, and has capacity for 1.
            //we actually first acquire that element so that next acquire will wait for a release
            PooledRef<PoolableTest> uniqueSlot = pool.acquire().block();
            assertThat(uniqueSlot).isNotNull();

            //we prepare next acquire
            Mono<PoolableTest> borrower = Mono.fromDirect(pool.acquireInScope(mono -> mono));
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
    void disposingPoolDisposesElements() {
        AtomicInteger cleanerCount = new AtomicInteger();
        QueuePool<PoolableTest> pool = new QueuePool<>(
                from(Mono.fromCallable(PoolableTest::new))
                        .allocationStrategy(allocatingMax(3))
                        .releaseHandler(p -> Mono.fromRunnable(cleanerCount::incrementAndGet))
                        .evictionPredicate(slot -> !slot.poolable().isHealthy())
                        .buildConfig());

        PoolableTest pt1 = new PoolableTest(1);
        PoolableTest pt2 = new PoolableTest(2);
        PoolableTest pt3 = new PoolableTest(3);

        pool.elements.offer(new QueuePool.QueuePooledRef<>(pool, pt1));
        pool.elements.offer(new QueuePool.QueuePooledRef<>(pool, pt2));
        pool.elements.offer(new QueuePool.QueuePooledRef<>(pool, pt3));

        pool.dispose();

        assertThat(pool.elements).hasSize(0);
        assertThat(cleanerCount).as("recycled elements").hasValue(0);
        assertThat(pt1.isDisposed()).as("pt1 disposed").isTrue();
        assertThat(pt2.isDisposed()).as("pt2 disposed").isTrue();
        assertThat(pt3.isDisposed()).as("pt3 disposed").isTrue();
    }

    @Test
    void disposingPoolFailsPendingBorrowers() {
        AtomicInteger cleanerCount = new AtomicInteger();
        QueuePool<PoolableTest> pool = new QueuePool<>(
                from(Mono.fromCallable(PoolableTest::new))
                        .allocationStrategy(allocatingMax(3))
                        .initialSize(3)
                        .releaseHandler(p -> Mono.fromRunnable(cleanerCount::incrementAndGet))
                        .evictionPredicate(slot -> !slot.poolable().isHealthy())
                        .buildConfig());

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

        assertThat(pool.elements).isEmpty();
        assertThat(cleanerCount).as("recycled elements").hasValue(0);
        assertThat(acquired1.isDisposed()).as("acquired1 held").isFalse();
        assertThat(acquired2.isDisposed()).as("acquired2 held").isFalse();
        assertThat(acquired3.isDisposed()).as("acquired3 held").isFalse();
        assertThat(borrowerError.get()).hasMessage("Pool has been shut down");
    }

    @Test
    void releasingToDisposedPoolDisposesElement() {
        AtomicInteger cleanerCount = new AtomicInteger();
        QueuePool<PoolableTest> pool = new QueuePool<>(
                from(Mono.fromCallable(PoolableTest::new))
                        .allocationStrategy(allocatingMax(3))
                        .initialSize(3)
                        .releaseHandler(p -> Mono.fromRunnable(cleanerCount::incrementAndGet))
                        .evictionPredicate(slot -> !slot.poolable().isHealthy())
                        .buildConfig());

        PooledRef<PoolableTest> slot1 = pool.acquire().block();
        PooledRef<PoolableTest> slot2 = pool.acquire().block();
        PooledRef<PoolableTest> slot3 = pool.acquire().block();

        assertThat(slot1).as("slot1").isNotNull();
        assertThat(slot2).as("slot2").isNotNull();
        assertThat(slot3).as("slot3").isNotNull();

        pool.dispose();

        assertThat(pool.elements).isEmpty();

        slot1.release().block();
        slot2.release().block();
        slot3.release().block();

        assertThat(cleanerCount).as("recycled elements").hasValue(0);
        assertThat(slot1.poolable().isDisposed()).as("acquired1 disposed").isTrue();
        assertThat(slot2.poolable().isDisposed()).as("acquired2 disposed").isTrue();
        assertThat(slot3.poolable().isDisposed()).as("acquired3 disposed").isTrue();
    }

    @Test
    void stillacquiredAfterPoolDisposedMaintainsCount() {
        AtomicInteger cleanerCount = new AtomicInteger();
        QueuePool<PoolableTest> pool = new QueuePool<>(
                from(Mono.fromCallable(PoolableTest::new))
                        .initialSize(3)
                        .allocationStrategy(allocatingMax(3))
                        .releaseHandler(p -> Mono.fromRunnable(cleanerCount::incrementAndGet))
                        .evictionPredicate(slot -> !slot.poolable().isHealthy())
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

    @Test
    void acquiringFromDisposedPoolFailsBorrower() {
        AtomicInteger cleanerCount = new AtomicInteger();
        QueuePool<PoolableTest> pool = new QueuePool<>(
                from(Mono.fromCallable(PoolableTest::new))
                        .allocationStrategy(allocatingMax(3))
                        .releaseHandler(p -> Mono.fromRunnable(cleanerCount::incrementAndGet))
                        .evictionPredicate(slot -> !slot.poolable().isHealthy())
                        .buildConfig());

        assertThat(pool.elements).isEmpty();

        pool.dispose();

        StepVerifier.create(pool.acquire())
                    .verifyErrorMessage("Pool has been shut down");

        assertThat(cleanerCount).as("recycled elements").hasValue(0);
    }

    @Test
    void poolIsDisposed() {
        QueuePool<PoolableTest> pool = new QueuePool<>(
                from(Mono.fromCallable(PoolableTest::new))
                        .allocationStrategy(allocatingMax(3))
                        .evictionPredicate(slot -> !slot.poolable().isHealthy())
                        .buildConfig());

        assertThat(pool.isDisposed()).as("not yet disposed").isFalse();

        pool.dispose();

        assertThat(pool.isDisposed()).as("disposed").isTrue();
    }

    @Test
    void disposingPoolClosesCloseable() {
        Formatter uniqueElement = new Formatter();

        QueuePool<Formatter> pool = new QueuePool<>(
                from(Mono.just(uniqueElement))
                        .allocationStrategy(allocatingMax(1))
                        .initialSize(1)
                        .evictionPredicate(slot -> true)
                        .buildConfig());

        pool.dispose();

        assertThatExceptionOfType(FormatterClosedException.class)
                .isThrownBy(uniqueElement::flush);
    }

    @Test
    void allocatorErrorOutsideConstructorIsPropagated() {
        QueuePool<String> pool = new QueuePool<>(
                from(Mono.<String>error(new IllegalStateException("boom")))
                        .allocationStrategy(allocatingMax(1))
                        .initialSize(0)
                        .evictionPredicate(f -> true)
                        .buildConfig());

        assertThatExceptionOfType(IllegalStateException.class)
                .isThrownBy(pool.acquire()::block)
                .withMessage("boom");
    }

    @Test
    void allocatorErrorInConstructorIsThrown() {
        DefaultPoolConfig<Object> config = from(Mono.error(new IllegalStateException("boom")))
                .initialSize(1)
                .allocationStrategy(allocatingMax(1))
                .evictionPredicate(f -> true)
                .buildConfig();

        assertThatExceptionOfType(IllegalStateException.class)
                .isThrownBy(() -> new QueuePool<>(config))
                .withMessage("boom");
    }

    @Test
    void discardCloseableWhenCloseFailureLogs() {
        TestLogger testLogger = new TestLogger();
        Loggers.useCustomLoggers(it -> testLogger);
        try {
            Closeable closeable = () -> {
                throw new IOException("boom");
            };

            QueuePool<Closeable> pool = new QueuePool<>(
                    from(Mono.just(closeable))
                            .initialSize(1)
                            .allocationStrategy(allocatingMax(1))
                            .evictionPredicate(f -> true)
                            .buildConfig());

            pool.dispose();

            assertThat(testLogger.getOutContent())
                    .contains("Failure while discarding a released Poolable that is Closeable, could not close - java.io.IOException: boom");
        }
        finally {
            Loggers.resetLoggerFactory();
        }
    }

    @Nested
    @DisplayName("metrics")
    @SuppressWarnings("ClassCanBeStatic")
    class QueueMetricsTest extends AbstractTestMetrics {

        @Override
        <T> Pool<T> createPool(DefaultPoolConfig<T> poolConfig) {
            return new QueuePool<>(poolConfig);
        }
    }

}