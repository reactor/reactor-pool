/*
 * Copyright (c) 2011-2019 Pivotal Software Inc, All Rights Reserved.
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

package reactor.util.pool.builder;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.util.TestLogger;
import reactor.util.Loggers;
import reactor.util.pool.api.PooledRef;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

/**
 * @author Simon Baslé
 */
class AffinityPoolTest {

    public static final class PoolableTest implements Disposable {

        private static AtomicInteger defaultId = new AtomicInteger();

        private int usedUp;
        private int discarded;
        private final int id;

        PoolableTest() {
            this(defaultId.incrementAndGet());
        }

        PoolableTest(int id) {
            this.id = id;
            this.usedUp = 0;
        }

        void clean() {
            this.usedUp++;
        }

        boolean isHealthy() {
            return usedUp < 2;
        }

        @Override
        public void dispose() {
            discarded++;
        }

        @Override
        public boolean isDisposed() {
            return discarded > 0;
        }

        @Override
        public String toString() {
            return "PoolableTest{id=" + id + ", used=" + usedUp + "}";
        }
    }

    private static final class PoolableTestConfig extends DefaultPoolConfig<PoolableTest> {

        private PoolableTestConfig(int minSize, int maxSize, Mono<PoolableTest> allocator) {
            super(minSize, maxSize,
                    allocator,
                    pt -> Mono.fromRunnable(pt::clean),
                    null,
                    slot -> !slot.poolable().isHealthy(),
                    null);
        }

        private PoolableTestConfig(int minSize, int maxSize, Mono<PoolableTest> allocator, Scheduler deliveryScheduler) {
            super(minSize, maxSize,
                    allocator,
                    pt -> Mono.fromRunnable(pt::clean),
                    null,
                    slot -> !slot.poolable().isHealthy(),
                    deliveryScheduler);
        }

        private PoolableTestConfig(int minSize, int maxSize, Mono<PoolableTest> allocator,
                                   Consumer<? super PoolableTest> additionalCleaner) {
            super(minSize, maxSize,
                    allocator,
                    poolableTest -> Mono.fromRunnable(() -> {
                        poolableTest.clean();
                        additionalCleaner.accept(poolableTest);
                    }),
                    null,
                    slot -> !slot.poolable().isHealthy(),
                    null);
        }
    }

    @Test
    void threadAffinity() throws InterruptedException {
        AffinityPool<String> pool = new AffinityPool<>(new DefaultPoolConfig<>(0, 3,
                Mono.fromCallable(() -> Thread.currentThread().getName().substring(0, 7)),
                s -> Mono.empty(), null, null, null));

        Scheduler thread1 = Schedulers.newSingle("thread1");
        Map<String, Integer> acquired1 = new HashMap<>(3);

        Scheduler thread2 = Schedulers.newSingle("thread2");
        Map<String, Integer> acquired2 = new HashMap<>(3);

        Scheduler thread3 = Schedulers.newSingle("thread3");
        Map<String, Integer> acquired3 = new HashMap<>(3);

        final CountDownLatch releaseLatch = new CountDownLatch(10 * 3);
        for (int i = 0; i < 10; i++) {
            thread1.schedule(() -> pool.acquire()
                            .subscribe(slot -> {
                                acquired1.compute(slot.poolable(), (k, old) -> old == null ? 1 : old + 1);
                                if (!slot.poolable().equals("thread1")) {
                                    System.out.println("unexpected in thread1: " + slot);
                                }

                                thread1.schedule(() -> slot.release().subscribe(), 150, TimeUnit.MILLISECONDS);
                            }, e -> releaseLatch.countDown(), releaseLatch::countDown));

            thread2.schedule(() -> pool.acquire()
                            .subscribe(slot -> {
                                acquired2.compute(slot.poolable(), (k, old) -> old == null ? 1 : old + 1);
                                if (!slot.poolable().equals("thread2")) {
                                    System.out.println("unexpected in thread2: " + slot);
                                }
                                thread2.schedule(() -> slot.release().subscribe(), 200, TimeUnit.MILLISECONDS);
                            }, e -> releaseLatch.countDown(), releaseLatch::countDown));

            thread3.schedule(() -> pool.acquire()
                            .subscribe(slot -> {
                                acquired3.compute(slot.poolable(), (k, old) -> old == null ? 1 : old + 1);
                                if (!slot.poolable().equals("thread3")) {
                                    System.out.println("unexpected in thread3: " + slot);
                                }
                                thread3.schedule(() -> slot.release().subscribe(), 100, TimeUnit.MILLISECONDS);
                            }, e -> releaseLatch.countDown(), releaseLatch::countDown));
        }

        if (releaseLatch.await(35, TimeUnit.SECONDS)) {
            assertThat(acquired1)
                    .as("thread1 acquired")
                    .hasSize(1)
                    .containsEntry("thread1", 10);
            assertThat(acquired2)
                    .as("thread2 acquired")
                    .hasSize(1)
                    .containsEntry("thread2", 10);
            assertThat(acquired3)
                    .as("thread3 acquired")
                    .hasSize(1)
                    .containsEntry("thread3", 10);
        }
        else {
            System.out.println("acquired1: " + acquired1);
            System.out.println("acquired2: " + acquired2);
            System.out.println("acquired3: " + acquired3);
            fail("didn't release all, but " + releaseLatch.getCount());
        }
    }

    @Nested
    @DisplayName("Tests around the acquire() manual mode of acquiring")
    class AcquireTest {

        @Test
        void smokeTest() throws InterruptedException {
            AtomicInteger newCount = new AtomicInteger();
            AffinityPool<PoolableTest> pool = new AffinityPool<>(new PoolableTestConfig(2, 3,
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
            AffinityPool<PoolableTest> pool = new AffinityPool<>(new PoolableTestConfig(2, 3,
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

        @Test
        void returnedReleasedIfBorrowerCancelled() {
            AtomicInteger releasedCount = new AtomicInteger();

            PoolableTestConfig testConfig = new PoolableTestConfig(1, 1,
                    Mono.fromCallable(PoolableTest::new),
                    pt -> releasedCount.incrementAndGet());
            AffinityPool<PoolableTest> pool = new AffinityPool<>(testConfig);

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

            PoolableTestConfig testConfig = new PoolableTestConfig(0, 1,
                    Mono.defer(() -> Mono.delay(Duration.ofMillis(50)).thenReturn(new PoolableTest(newCount.incrementAndGet())))
                            .subscribeOn(scheduler),
                    pt -> releasedCount.incrementAndGet());
            AffinityPool<PoolableTest> pool = new AffinityPool<>(testConfig);

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

            PoolableTestConfig testConfig = new PoolableTestConfig(0, 1,
                    Mono.defer(() -> Mono.delay(Duration.ofMillis(50)).thenReturn(new PoolableTest(newCount.incrementAndGet())))
                            .subscribeOn(scheduler),
                    pt -> releasedCount.incrementAndGet());
            AffinityPool<PoolableTest> pool = new AffinityPool<>(testConfig);

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
            PoolableTestConfig testConfig = new PoolableTestConfig(0, 1, Mono.fromCallable(PoolableTest::new),
                    pt -> { throw new IllegalStateException("boom"); });
            AffinityPool<PoolableTest> pool = new AffinityPool<>(testConfig);

            PooledRef<PoolableTest> slot = pool.acquire().block();

            assertThat(slot).isNotNull();

            StepVerifier.create(slot.release())
                    .verifyErrorMessage("boom");
        }

        @Test
        void cleanerFunctionErrorDiscards() {
            PoolableTestConfig testConfig = new PoolableTestConfig(0, 1, Mono.fromCallable(PoolableTest::new),
                    pt -> { throw new IllegalStateException("boom"); });
            AffinityPool<PoolableTest> pool = new AffinityPool<>(testConfig);

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
            PoolableTestConfig testConfig = new PoolableTestConfig(1, 1,
                    Mono.fromCallable(PoolableTest::new)
                            .subscribeOn(Schedulers.newParallel("poolable test allocator")));
            AffinityPool<PoolableTest> pool = new AffinityPool<>(testConfig);

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
            PoolableTestConfig testConfig = new PoolableTestConfig(0, 1,
                    Mono.fromCallable(PoolableTest::new)
                            .subscribeOn(Schedulers.newParallel("poolable test allocator")));
            AffinityPool<PoolableTest> pool = new AffinityPool<>(testConfig);

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
            PoolableTestConfig testConfig = new PoolableTestConfig(1, 1,
                    Mono.fromCallable(PoolableTest::new)
                            .subscribeOn(Schedulers.newParallel("poolable test allocator")));
            AffinityPool<PoolableTest> pool = new AffinityPool<>(testConfig);

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
            String stats = "releaser won " + releaserWins.get() + ", borrower won " + borrowerWins.get();
            assertThat(releaserWins).as(stats).hasValue(100);
            assertThat(borrowerWins).as(stats).hasValue(0);
            //TODO ensure this is congruent with the AffinityPool configuration
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

            PoolableTestConfig testConfig = new PoolableTestConfig(1, 1,
                    Mono.fromCallable(() -> new PoolableTest(newCount.getAndIncrement()))
                            .subscribeOn(Schedulers.newParallel("poolable test allocator")));

            AffinityPool<PoolableTest> pool = new AffinityPool<>(testConfig);

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

            assertThat(
                    latch.await(5, TimeUnit.SECONDS)
            ).as("first acquire delivered within 5s").isTrue();

            assertThat(newCount).as("created 1 poolable in round " + round).hasValue(1);

            //we expect that sometimes the race will let the second borrower thread drain, which would mean first borrower
            //will get the element delivered from racerAcquire thread. Yet the rest of the time it would get drained by racerRelease.
            if (threadName.get().startsWith("racerRelease")) releaserWins.incrementAndGet();
            else if (threadName.get().startsWith("racerAcquire")) borrowerWins.incrementAndGet();
            else System.out.println(threadName.get());
        }
    }

    @Test
    void disposingPoolDisposesElements() {
        AtomicInteger cleanerCount = new AtomicInteger();
        AffinityPool<PoolableTest> pool = new AffinityPool<>(new DefaultPoolConfig<>(0, 3, Mono.fromCallable(PoolableTest::new),
                p -> Mono.fromRunnable(cleanerCount::incrementAndGet),
                null,
                slot -> !slot.poolable().isHealthy(), null));

        PoolableTest pt1 = new PoolableTest(1);
        PoolableTest pt2 = new PoolableTest(2);
        PoolableTest pt3 = new PoolableTest(3);

        pool.availableElements.offer(new AffinityPool.AffinityPooledRef<>(pool, pt1));
        pool.availableElements.offer(new AffinityPool.AffinityPooledRef<>(pool, pt2));
        pool.availableElements.offer(new AffinityPool.AffinityPooledRef<>(pool, pt3));

        pool.dispose();

        assertThat(pool.availableElements).hasSize(0);
        assertThat(cleanerCount).as("recycled elements").hasValue(0);
        assertThat(pt1.isDisposed()).as("pt1 disposed").isTrue();
        assertThat(pt2.isDisposed()).as("pt2 disposed").isTrue();
        assertThat(pt3.isDisposed()).as("pt3 disposed").isTrue();
    }

    @Test
    void disposingPoolFailsPendingBorrowers() {
        AtomicInteger cleanerCount = new AtomicInteger();
        AffinityPool<PoolableTest> pool = new AffinityPool<>(new DefaultPoolConfig<>(3, 3, Mono.fromCallable(PoolableTest::new),
                p -> Mono.fromRunnable(cleanerCount::incrementAndGet),
                null,
                slot -> !slot.poolable().isHealthy(), null));

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

        assertThat(pool.availableElements).isEmpty();
        assertThat(cleanerCount).as("recycled elements").hasValue(0);
        assertThat(acquired1.isDisposed()).as("acquired1 held").isFalse();
        assertThat(acquired2.isDisposed()).as("acquired2 held").isFalse();
        assertThat(acquired3.isDisposed()).as("acquired3 held").isFalse();
        assertThat(borrowerError.get()).hasMessage("Pool has been shut down");
    }

    @Test
    void releasingToDisposedPoolDisposesElement() {
        AtomicInteger cleanerCount = new AtomicInteger();
        AffinityPool<PoolableTest> pool = new AffinityPool<>(new DefaultPoolConfig<>(3, 3, Mono.fromCallable(PoolableTest::new),
                p -> Mono.fromRunnable(cleanerCount::incrementAndGet),
                null,
                slot -> !slot.poolable().isHealthy(), null));

        PooledRef<PoolableTest> slot1 = pool.acquire().block();
        PooledRef<PoolableTest> slot2 = pool.acquire().block();
        PooledRef<PoolableTest> slot3 = pool.acquire().block();

        assertThat(slot1).as("slot1").isNotNull();
        assertThat(slot2).as("slot2").isNotNull();
        assertThat(slot3).as("slot3").isNotNull();

        pool.dispose();

        assertThat(pool.availableElements).isEmpty();

        slot1.release().block();
        slot2.release().block();
        slot3.release().block();

        assertThat(cleanerCount).as("recycled elements").hasValue(0);
        assertThat(slot1.poolable().isDisposed()).as("acquired1 disposed").isTrue();
        assertThat(slot2.poolable().isDisposed()).as("acquired2 disposed").isTrue();
        assertThat(slot3.poolable().isDisposed()).as("acquired3 disposed").isTrue();
    }

    @Test
    void acquiringFromDisposedPoolFailsBorrower() {
        AtomicInteger cleanerCount = new AtomicInteger();
        AffinityPool<PoolableTest> pool = new AffinityPool<>(new DefaultPoolConfig<>(0, 3, Mono.fromCallable(PoolableTest::new),
                p -> Mono.fromRunnable(cleanerCount::incrementAndGet),
                null,
                slot -> !slot.poolable().isHealthy(), null));

        assertThat(pool.availableElements).isEmpty();

        pool.dispose();

        StepVerifier.create(pool.acquire())
                .verifyErrorMessage("Pool has been shut down");

        assertThat(cleanerCount).as("recycled elements").hasValue(0);
    }

    @Test
    void poolIsDisposed() {
        AffinityPool<PoolableTest> pool = new AffinityPool<>(new DefaultPoolConfig<>(0, 3,
                Mono.fromCallable(PoolableTest::new), null, null,
                slot -> !slot.poolable().isHealthy(), null));

        assertThat(pool.isDisposed()).as("not yet disposed").isFalse();

        pool.dispose();

        assertThat(pool.isDisposed()).as("disposed").isTrue();
    }

    @Test
    void disposingPoolClosesCloseable() {
        Formatter uniqueElement = new Formatter();

        AffinityPool<Formatter> pool = new AffinityPool<>(new DefaultPoolConfig<>(1, 1,
                Mono.just(uniqueElement),
                null, null,
                f -> true, null));

        pool.dispose();

        assertThatExceptionOfType(FormatterClosedException.class)
                .isThrownBy(uniqueElement::flush);
    }

    @Test
    void allocatorErrorOutsideConstructorIsPropagated() {
        AffinityPool<String> pool = new AffinityPool<>(new DefaultPoolConfig<>(0, 1,
                Mono.error(new IllegalStateException("boom")),
                null, null,
                f -> true, null));

        assertThatExceptionOfType(IllegalStateException.class)
                .isThrownBy(pool.acquire()::block)
                .withMessage("boom");
    }

    @Test
    void allocatorErrorInConstructorIsThrown() {
        DefaultPoolConfig<Object> config = new DefaultPoolConfig<>(1, 1,
                Mono.error(new IllegalStateException("boom")),
                null, null,
                f -> true, null);

        assertThatExceptionOfType(IllegalStateException.class)
                .isThrownBy(() -> new AffinityPool<>(config))
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

            AffinityPool<Closeable> pool = new AffinityPool<>(new DefaultPoolConfig<>(1, 1,
                    Mono.just(closeable),
                    null, null,
                    f -> true, null));

            pool.dispose();

            assertThat(testLogger.getOutContent())
                    .contains("Failure while discarding a released Poolable that is Closeable, could not close - java.io.IOException: boom");
        }
        finally {
            Loggers.resetLoggerFactory();
        }
    }
}