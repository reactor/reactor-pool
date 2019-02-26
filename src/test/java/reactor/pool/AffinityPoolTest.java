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

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.assertj.core.data.Offset;
import org.awaitility.Awaitility;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.reactivestreams.Subscription;

import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.pool.AbstractPool.DefaultPoolConfig;
import reactor.pool.TestUtils.PoolableTest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.awaitility.Awaitility.await;

/**
 * @author Simon Baslé
 */
class AffinityPoolTest {

    //==utils for package-private config==
    static final DefaultPoolConfig<PoolableTest> poolableTestConfig(int minSize, int maxSize, Mono<PoolableTest> allocator) {
        return PoolBuilder.from(allocator)
                          .threadAffinity(true)
                          .initialSize(minSize)
                          .sizeMax(maxSize)
                          .releaseHandler(pt -> Mono.fromRunnable(pt::clean))
                          .evictionPredicate(slot -> !slot.poolable().isHealthy())
                          .buildConfig();
    }

    static final DefaultPoolConfig<PoolableTest> poolableTestConfig(int minSize, int maxSize, Mono<PoolableTest> allocator,
            Consumer<? super PoolableTest> additionalCleaner) {
        return PoolBuilder.from(allocator)
                          .threadAffinity(true)
                          .initialSize(minSize)
                          .sizeMax(maxSize)
                          .releaseHandler(poolableTest -> Mono.fromRunnable(() -> {
                              poolableTest.clean();
                              additionalCleaner.accept(poolableTest);
                          }))
                          .evictionPredicate(slot -> !slot.poolable().isHealthy())
                          .buildConfig();
    }
    //======

    @Test
    void threadAffinity() throws InterruptedException, ExecutionException {
        ScheduledExecutorService thread1 = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r,"thread1"));
        ScheduledExecutorService thread2 = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r,"thread2"));
        ScheduledExecutorService thread3 = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r,"thread3"));

        try {
            AffinityPool<String> pool = new AffinityPool<>(
                    PoolBuilder.from(Mono.fromCallable(() -> Thread.currentThread().getName().substring(0, 7)))
                               .threadAffinity(true)
                               .sizeMax(3)
                               .buildConfig());

            Map<String, Integer> acquired1 = new HashMap<>(3);
            Map<String, Integer> acquired2 = new HashMap<>(3);
            Map<String, Integer> acquired3 = new HashMap<>(3);

            //create the resources and get them for the first triggering release
            PooledRef<String> ref1 = thread1.submit(() -> pool.acquire().block()).get();
            PooledRef<String> ref2 = thread2.submit(() -> pool.acquire().block()).get();
            PooledRef<String> ref3 = thread3.submit(() -> pool.acquire().block()).get();

            final CountDownLatch releaseLatch = new CountDownLatch(10 * 3);
            for (int i = 0; i < 10; i++) {
                thread1.submit(() -> pool.acquire()
                                         .subscribe(slot -> {
                                             acquired1.compute(slot.poolable(), (k, old) -> old == null ? 1 : old + 1);
                                             if (!slot.poolable().equals("thread1")) {
                                                 System.out.println("unexpected in thread1: " + slot);
                                             }
                                           thread1.schedule(() -> slot.release().subscribe(), 25, TimeUnit.MILLISECONDS);
                                         }, e -> releaseLatch.countDown(), releaseLatch::countDown));

                thread2.submit(() -> pool.acquire()
                                         .subscribe(slot -> {
                                             acquired2.compute(slot.poolable(), (k, old) -> old == null ? 1 : old + 1);
                                             if (!slot.poolable().equals("thread2")) {
                                                 System.out.println("unexpected in thread2: " + slot);
                                             }
                                           thread2.schedule(() -> slot.release().subscribe(), 25, TimeUnit.MILLISECONDS);
                                         }, e -> releaseLatch.countDown(), releaseLatch::countDown));

                thread3.submit(() -> pool.acquire()
                                         .subscribe(slot -> {
                                             acquired3.compute(slot.poolable(), (k, old) -> old == null ? 1 : old + 1);
                                             if (!slot.poolable().equals("thread3")) {
                                                 System.out.println("unexpected in thread3: " + slot);
                                             }
                                           thread3.schedule(() -> slot.release().subscribe(), 25, TimeUnit.MILLISECONDS);
                                         }, e -> releaseLatch.countDown(), releaseLatch::countDown));

            }
            thread1.submit((Runnable) ref1.release()::block);
            thread2.submit((Runnable) ref2.release()::block);
            thread3.submit((Runnable) ref3.release()::block);

            if (releaseLatch.await(35, TimeUnit.SECONDS)) {
                assertThat(acquired1)
                        .as("thread1 acquired")
                        .containsEntry("thread1", 10)
                        .hasSize(1);
                assertThat(acquired2)
                        .as("thread2 acquired")
                        .containsEntry("thread2", 10)
                        .hasSize(1);
                assertThat(acquired3)
                        .as("thread3 acquired")
                        .containsEntry("thread3", 10)
                        .hasSize(1);
            }
            else {
                System.out.println("acquired1: " + acquired1);
                System.out.println("acquired2: " + acquired2);
                System.out.println("acquired3: " + acquired3);
                fail("didn't release all, but " + releaseLatch.getCount());
            }
        }
        finally {
            thread1.shutdown();
            thread2.shutdown();
            thread3.shutdown();
        }
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

            DefaultPoolConfig<PoolableTest> testConfig = poolableTestConfig(0, 1,
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
        void defaultThreadDeliveringWhenHasElements() throws InterruptedException {
            AtomicReference<String> threadName = new AtomicReference<>();
            Scheduler acquireScheduler = Schedulers.newSingle("acquire");
            DefaultPoolConfig<PoolableTest> testConfig = poolableTestConfig(1, 1,
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
            DefaultPoolConfig<PoolableTest> testConfig = poolableTestConfig(0, 1,
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
            DefaultPoolConfig<PoolableTest> testConfig = poolableTestConfig(1, 1,
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
        }

        @Test
        void defaultThreadDeliveringWhenNoElementsAndFullAndRaceDrain() throws InterruptedException {
            AtomicInteger releaserWins = new AtomicInteger();
            AtomicInteger borrowerWins = new AtomicInteger();

            defaultThreadDeliveringWhenNoElementsAndFullAndRaceDrain(0, releaserWins, borrowerWins);

            assertThat(releaserWins.get()).isEqualTo(1);
        }

        void defaultThreadDeliveringWhenNoElementsAndFullAndRaceDrain(int round, AtomicInteger releaserWins, AtomicInteger borrowerWins) throws InterruptedException {
            AtomicReference<String> threadName = new AtomicReference<>();
            AtomicInteger newCount = new AtomicInteger();
            Scheduler acquireScheduler = Schedulers.fromExecutorService(
                    Executors.newSingleThreadScheduledExecutor((r -> new Thread(r,"acquire"))));
            Scheduler racerAcquireScheduler = Schedulers.fromExecutorService(
                    Executors.newSingleThreadScheduledExecutor((r -> new Thread(r,"racerAcquire"))));

            DefaultPoolConfig<PoolableTest> testConfig = poolableTestConfig(1, 1,
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
            acquireScheduler.schedule(() -> borrower.subscribe(v -> threadName.set(Thread.currentThread().getName()),
                    e -> latch.countDown(),
                    latch::countDown));

            //in parallel, we'll both attempt concurrent acquire AND release the unique element (each on their dedicated threads)
            acquireScheduler.schedule(uniqueSlot.release()::block, 100, TimeUnit.MILLISECONDS);
            racerAcquireScheduler.schedule(pool.acquire()::block, 100, TimeUnit.MILLISECONDS);

            assertThat(latch.await(5, TimeUnit.SECONDS)
            ).as("first acquire delivered within 5s in round " + round).isTrue();

            assertThat(newCount).as("created 1 poolable in round " + round).hasValue(1);

            //we expect that sometimes the race will let the second borrower thread drain, which would mean first borrower
            //will get the element delivered from racerAcquire thread. Yet the rest of the time it would get drained by racerRelease.
            if (threadName.get().startsWith("acquire")) releaserWins.incrementAndGet();
            else if (threadName.get().startsWith("racerAcquire")) borrowerWins.incrementAndGet();
            else System.out.println(threadName.get());
        }

        @Test
        void bestEffortAllocateOrPend() throws InterruptedException {
            AtomicInteger allocCounter = new AtomicInteger();
            AtomicInteger destroyCounter = new AtomicInteger();
            DefaultPoolConfig<Integer> config = PoolBuilder.from(Mono.fromCallable(allocCounter::incrementAndGet))
                                                           .threadAffinity(true)
                                                           .sizeMax(3)
                                                           .evictionPredicate(ref -> ref.acquireCount() >= 3)
                                                           .destroyHandler(i -> Mono.fromRunnable(destroyCounter::incrementAndGet))
                                                           .buildConfig();
            AffinityPool<Integer> pool = new AffinityPool<>(config);

            CountDownLatch latch = new CountDownLatch(10);
            for (int i = 0; i < 10; i++) {
                pool.acquire()
                    .delayElement(Duration.ofMillis(300))
                    .flatMap(PooledRef::release)
                    .doFinally(fin -> latch.countDown())
                    .subscribe();
            }

            if (!latch.await(5, TimeUnit.SECONDS)) {
                fail("Timed out after 5s, allocated " + allocCounter.get() + " and destroyed " + destroyCounter.get());
            }

            assertThat(allocCounter).as("allocations").hasValue(4);
            assertThat(destroyCounter).as("destruction").hasValue(3);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3})
    @Tag("metrics")
    void fastPathMetrics(int concurrency) {
        final TestUtils.InMemoryPoolMetrics recorder = new TestUtils.InMemoryPoolMetrics();
        ScheduledExecutorService acquireExecutor = Executors.newScheduledThreadPool(concurrency);
        Scheduler acquireScheduler = Schedulers.fromExecutorService(acquireExecutor);
        AtomicInteger allocator = new AtomicInteger();
        AtomicInteger released = new AtomicInteger();
        CyclicBarrier firstRefLatch = new CyclicBarrier(concurrency);

        try {
            Pool<String> pool = new AffinityPool<>(
                    PoolBuilder.from(Mono.fromCallable(() -> "---" + allocator.incrementAndGet() + "-->" + Thread.currentThread().getName()))
                               .threadAffinity(true)
                               .sizeMax(concurrency)
                               .metricsRecorder(recorder)
                               .buildConfig());

            for (int threadIndex = 1; threadIndex <= concurrency; threadIndex++) {
                final String threadName = "thread" + threadIndex;
                acquireExecutor.submit(() -> {
                    final PooledRef<String> firstRef = pool.acquire().block();
                    assert firstRef != null;
                    final String sticky = firstRef.poolable();
                    System.out.println(threadName + " should try to stick with " + firstRef);
                    try {
                        firstRefLatch.await(10, TimeUnit.SECONDS);
                    } catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
                        e.printStackTrace();
                    }

                    for (int i = 0; i < 10; i++) {
                        pool.acquire().subscribe(ref -> {
                            String p = ref.poolable();
                            if (!sticky.equals(p)) {
                                System.out.println(threadName + " unexpected " + p);
                            }
                            else {
                                System.out.println(threadName + " " + p);
                            }
                            acquireScheduler.schedule(() ->
                                            ref.release().doFinally(fin -> released.incrementAndGet()).subscribe(),
                                    100, TimeUnit.MILLISECONDS);
                        });
                    }
                    firstRef.release().block();
                });
            }

            Awaitility.await()
                      .atMost(10, TimeUnit.SECONDS)
                      .untilAtomic(released, Matchers.equalTo(10 * concurrency));

            System.out.println("slowPath = " + recorder.getSlowPathCount() + ", fastPath = " + recorder.getFastPathCount());

            assertThat(allocator).hasValue(concurrency);
            long slowPath = recorder.getSlowPathCount();
            long fastPath = recorder.getFastPathCount();
            Offset<Long> errorMargin = Offset.offset((long) (10 * concurrency * 0.2d));

            assertThat(slowPath).as("slowpath").isCloseTo(0L, errorMargin);
            assertThat(fastPath).as("fastpath").isCloseTo(10L * concurrency, errorMargin);
            assertThat(slowPath + fastPath).as("fastpath + slowpath").isEqualTo(10L * concurrency);
        } finally {
            acquireScheduler.dispose();
            acquireExecutor.shutdownNow();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 3, 10, 100})
    @Tag("metrics")
    void slowPathMetrics(int max) {
        final TestUtils.InMemoryPoolMetrics recorder = new TestUtils.InMemoryPoolMetrics();
        ScheduledExecutorService acquireExecutor = Executors.newScheduledThreadPool(max + 1);
        Scheduler acquireScheduler = Schedulers.fromExecutorService(acquireExecutor);
        AtomicInteger allocator = new AtomicInteger();
        AtomicInteger hogged = new AtomicInteger();

        CountDownLatch prepareDataLatch = new CountDownLatch(max);
        CountDownLatch hoggerLatch = new CountDownLatch(1);
        CountDownLatch finalLatch = new CountDownLatch(max);

        try {
            Pool<String> pool = new AffinityPool<>(
                    PoolBuilder.from(Mono.fromCallable(() -> {
                        allocator.incrementAndGet();
                        return Thread.currentThread().getName();
                    }))
                               .threadAffinity(true)
                               .sizeMax(max)
                               .metricsRecorder(recorder)
                               .buildConfig());

            acquireScheduler.schedule(() -> {
                try {
                    prepareDataLatch.await(8, TimeUnit.SECONDS);
                    System.out.println("HEAVY BORROWER WILL NOW QUEUE acquire()");
                    for (int i = 0; i < max; i++) {
                        pool.acquire()
                            .doOnNext(v -> System.out.println("HEAVY BORROWER's pending received " + v.poolable() +
                                    " from " + Thread.currentThread().getName()))
                            .doFinally(fin -> finalLatch.countDown())
                            .subscribe(v -> hogged.incrementAndGet());
                    }
                    System.out.println("HEAVY BORROWER DONE QUEUEING");
                    hoggerLatch.countDown();
                    finalLatch.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    hoggerLatch.countDown();
                }
            });

            for (int threadIndex = 1; threadIndex <= max; threadIndex++) {
                acquireScheduler.schedule(() -> {
                    final PooledRef<String> ref = pool.acquire().block();
                    assert ref != null;
                    System.out.println("Created resource " + ref.poolable());
                    prepareDataLatch.countDown();
                    try {
                        hoggerLatch.await(9, TimeUnit.SECONDS);
                        Thread.sleep(100);
                        System.out.println(Thread.currentThread().getName() + " releasing its resource");
                        ref.release().block();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                });
            }

            Awaitility.await()
                      .atMost(10, TimeUnit.SECONDS)
                      .untilAtomic(hogged, Matchers.equalTo(max));

            System.out.println("slowPath = " + recorder.getSlowPathCount() + ", fastPath = " + recorder.getFastPathCount());

            assertThat(allocator).as("allocated").hasValue(max);
            assertThat(recorder.getRecycledCount()).as("recycled count").isEqualTo(max);
            assertThat(recorder.getSlowPathCount()).as("slowpath").isEqualTo(max);
            assertThat(recorder.getFastPathCount()).as("fastpath").isZero();
        } finally {
            acquireScheduler.dispose();
            acquireExecutor.shutdownNow();
        }
    }
}