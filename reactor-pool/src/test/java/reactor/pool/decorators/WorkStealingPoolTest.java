/*
 * Copyright (c) 2024 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.pool.decorators;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.pool.*;
import reactor.scheduler.clock.SchedulerClock;
import reactor.test.StepVerifier;
import reactor.test.scheduler.VirtualTimeScheduler;
import reactor.test.util.RaceTestUtils;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.function.Tuples;

import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

import reactor.core.scheduler.Schedulers;
import reactor.pool.InstrumentedPool;

class WorkStealingPoolTest {
    final static Logger log = Loggers.getLogger(WorkStealingPoolTest.class);
    static final int LOOPS = 100000;
    static final int POOLS = 10;
    static final int POOL_SIZE = 1000;
    PoolScheduler<PiCalculator> pool;
    List<ExecutorService> execs;

    static final class PiCalculator {
        public double calculatePi(int terms) {
            double pi = 0.0;
            for (int i = 0; i < terms; i++) {
                double term = 1.0 / (2 * i + 1);
                if (i % 2 == 0) {
                    pi += term;
                } else {
                    pi -= term;
                }
            }
            return pi * 4.0;
        }
    }

    @BeforeEach
    void init() {
        execs = IntStream.range(0, POOLS).mapToObj(i -> Executors.newSingleThreadExecutor()).collect(Collectors.toList());
        Iterator<ExecutorService> execsIter = execs.iterator();
        Mono<PiCalculator> allocator = Mono.defer(() -> Mono.just(new PiCalculator()));
        pool = InstrumentedPoolDecorators.concurrentPools(POOLS, allocator, poolBuilder ->
                Tuples.of(poolBuilder.sizeBetween(1, POOL_SIZE / POOLS).buildPool(), execsIter.next()));
    }

    @AfterEach
    void destroy() {
        pool.disposeLater().block(Duration.ofSeconds(3));
        execs.forEach(exec -> {
            try {
                exec.shutdown();
                if (!exec.awaitTermination(10, TimeUnit.SECONDS)) {
                    throw new RuntimeException("Could not terminate executor timely.");
                }
            } catch (InterruptedException e) {
            }
        });
    }

    @Test
    @RepeatedTest(100)
    void smokeTest() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);

        pool.withPoolable(piCalculator -> Mono.just(piCalculator.calculatePi(3000)))
                .subscribe(pi -> latch.countDown());

        if (!latch.await(10, TimeUnit.SECONDS)) {
            fail("could not acquire resource timely");
        }

        assertThat(pool.metrics().allocatedSize()).isGreaterThan(0);

        await().atMost(3, TimeUnit.SECONDS).with().pollInterval(10, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> assertThat(pool.metrics().idleSize())
                        .as("idle size should be equal to 1")
                        .isGreaterThan(0));
        System.out.println("Workers: " + pool.toString());
    }

    @Test
    @RepeatedTest(100)
    void timeoutTest() throws InterruptedException {
        // allocate all resources
        List<PooledRef<PiCalculator>> refs = IntStream.range(0, POOL_SIZE)
                .mapToObj(i -> pool.acquire(Duration.ofSeconds(4)).block())
                .collect(Collectors.toList());

        //error, we should get a timed out
        pool.acquire(Duration.ofMillis(1))
                .as(StepVerifier::create)
                .expectError(PoolAcquireTimeoutException.class)
                .verify(Duration.ofSeconds(3));

        refs.forEach(ref -> ref.release().block(Duration.ofSeconds(1)));
        System.out.println("Workers: " + pool.toString());
    }

    @Test
    @RepeatedTest(100)
    void acquireAllTest() throws InterruptedException {
        List<PooledRef<PiCalculator>> refs = IntStream.range(0, POOL_SIZE)
                .mapToObj(i -> pool.acquire(Duration.ofSeconds(4)).block())
                .collect(Collectors.toList());
        await().atMost(3, TimeUnit.SECONDS).with().pollInterval(10, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> assertThat(pool.metrics().idleSize())
                        .as("idle size should be equal to 0")
                        .isEqualTo(0));
        refs.forEach(r -> r.release().block(Duration.ofSeconds(1)));
    }

    @Test
    @RepeatedTest(100)
    void testReleaseAfterQuiescenceFullPools() throws InterruptedException {
        List<PooledRef<PiCalculator>> refs = IntStream.range(0, POOL_SIZE)
                .mapToObj(i -> pool.acquire(Duration.ofSeconds(1)).block())
                .collect(Collectors.toList());

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<PooledRef<PiCalculator>> ref = new AtomicReference<>();
        pool.acquire().subscribe(r -> {
            ref.set(r);
            latch.countDown();
        });

        AtomicBoolean removed = new AtomicBoolean();

        refs.remove(0)
                .release()
                .doOnSuccess(__ -> {
                    removed.set(true);
                })
                .block(Duration.ofSeconds(1));

        assertThat(latch.await(1, TimeUnit.SECONDS)).isTrue();

        assertThat(ref.get()).isNotNull();
        assertThat(pool.metrics().allocatedSize()).isEqualTo(POOL_SIZE);
        ref.get().release().block(Duration.ofSeconds(1));
        refs.forEach(r -> r.release().block(Duration.ofSeconds(1)));
        await().atMost(3, TimeUnit.SECONDS).with().pollInterval(10, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> assertThat(pool.metrics().idleSize())
                        .as("idle size should be equal to 1")
                        .isGreaterThan(0));
    }

    @Test
    @RepeatedTest(10)
    void testPerformance() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(LOOPS);
        Flux.range(0, LOOPS)
                .flatMap(i -> pool.withPoolable(piCalculator -> Mono.just(piCalculator.calculatePi(3000))))
                .doOnNext(pi -> latch.countDown())
                .subscribe();

        assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
        assertThat(pool.stealCount() > 0).isTrue();
    }

    @Test
    @Tag("loops")
    void acquireReleaseRaceWithMinSize_loop() {
        final Scheduler racer = Schedulers.fromExecutorService(Executors.newFixedThreadPool(2));
        AtomicInteger newCount = new AtomicInteger();
        try {
            Mono<TestUtils.PoolableTest> allocator = Mono.fromCallable(() -> new TestUtils.PoolableTest(newCount.getAndIncrement()));
            Iterator<ExecutorService> execsIter = execs.iterator();

            InstrumentedPool<TestUtils.PoolableTest> pool = InstrumentedPoolDecorators.concurrentPools(2, allocator, poolBuilder ->
                    Tuples.of(poolBuilder.sizeBetween(2, 5)
                            .buildPool(), execsIter.next()));

            for (int i = 0; i < 100; i++) {
                RaceTestUtils.race(racer,
                        () -> pool.acquire().block().release().block(),
                        () -> pool.acquire().block().release().block());
            }
            //we expect that only 3 element was created
            assertThat(newCount).as("elements created in total").hasValue(4);
        }
        finally {
            pool.disposeLater().block(Duration.ofSeconds(3));
            racer.dispose();
        }
    }

    @Test
    @RepeatedTest(10)
    @Disabled
    void testAvoidReallocationAfterEviction() throws InterruptedException {
        VirtualTimeScheduler vts = VirtualTimeScheduler.create();
        AtomicInteger allocCounter = new AtomicInteger();
        Mono<MyResource> allocator = Mono.defer(() -> Mono.just(new MyResource(vts, allocCounter.incrementAndGet())));

        Iterator<ExecutorService> execsIter = execs.iterator();
        InstrumentedPool<MyResource> pool = InstrumentedPoolDecorators.concurrentPools(2, allocator, poolBuilder ->
                Tuples.of(poolBuilder.sizeBetween(0, 1)
                        .evictionPredicate((poolable, metadata) -> vts.now(TimeUnit.MILLISECONDS) - poolable.releaseTimestamp > 4000)
				        .releaseHandler(pt -> Mono.fromRunnable(pt::release))
                        .buildPool(), execsIter.next()));

        PooledRef<MyResource> ref1 = pool.acquire().block();
        PooledRef<MyResource> ref2 = pool.acquire().block();

        ref1.release().block();
        assertThatCode(() -> vts.advanceTimeBy(Duration.ofSeconds(10))).doesNotThrowAnyException();
        ref2.release().block();
        ref2 = pool.acquire().block();

        assertThat(ref2.poolable().id).as("allocations").isEqualTo(2);
        assertThat(allocCounter).as("allocations").hasValue(2);

        ref1 = pool.acquire().block();
        assertThat(ref1.poolable().id).as("allocations").isEqualTo(3);
        assertThat(allocCounter).as("allocations").hasValue(3);
        assertThat(pool.metrics().allocatedSize()).as("metrics().allocatedSize()").isEqualTo(2);

        ref1.release().block();
        ref2.release().block();
        pool.disposeLater().block(Duration.ofSeconds(3));
    }

    @Test
    @RepeatedTest(100)
    @Disabled
    void testAvoidReallocationAfterEvictionMany() throws InterruptedException {
        VirtualTimeScheduler vts = VirtualTimeScheduler.create();

        AtomicInteger allocCounter = new AtomicInteger();
        AtomicInteger destroyCounter = new AtomicInteger();

        Iterator<ExecutorService> execsIter = execs.iterator();
        Mono<Integer> allocator = Mono.fromCallable(allocCounter::incrementAndGet);

        InstrumentedPool<Integer> pool = InstrumentedPoolDecorators.concurrentPools(POOLS, allocator, poolBuilder ->
                Tuples.of(poolBuilder.sizeBetween(0, POOL_SIZE / POOLS)
                        .evictionPredicate((poolable, metadata) -> metadata.idleTime() >= 4000)
                        .evictInBackground(Duration.ofSeconds(5), vts)
                        .destroyHandler(i -> Mono.fromRunnable(destroyCounter::incrementAndGet))
                        .clock(SchedulerClock.of(vts))
                        .buildPool(), execsIter.next()));

        List<PooledRef<Integer>> refs = IntStream.range(0, POOL_SIZE)
                .mapToObj(i -> pool.acquire(Duration.ofSeconds(1)).block())
                .collect(Collectors.toList());

        assertThat(allocCounter).as("allocations").hasValue(POOL_SIZE);

        for (int i = 0; i < POOL_SIZE-1; i ++) {
            refs.remove(0).release().block();
        }

        assertThatCode(() -> vts.advanceTimeBy(Duration.ofSeconds(10))).doesNotThrowAnyException();

        refs.remove(0).release().block();

        PooledRef<Integer> ref = pool.acquire().block();
        assertThat(allocCounter).as("allocations").hasValue(POOL_SIZE);
        assertThat(ref.poolable()).as("allocations").isEqualTo(POOL_SIZE);
        assertThat(pool.metrics().allocatedSize()).as("allocations").isEqualTo(1);
        ref.release().block();
        pool.disposeLater().block(Duration.ofSeconds(3));
    }

    @Test
    @RepeatedTest(100)
    @Disabled
    void testAvoidReallocationAfterEvictionDaemon() throws InterruptedException {
        VirtualTimeScheduler vts = VirtualTimeScheduler.create();

        AtomicInteger allocCounter = new AtomicInteger();
        AtomicInteger destroyCounter = new AtomicInteger();

        Iterator<ExecutorService> execsIter = execs.iterator();
        Mono<Integer> allocator = Mono.fromCallable(allocCounter::incrementAndGet);

        InstrumentedPool<Integer> pool = InstrumentedPoolDecorators.concurrentPools(2, allocator, poolBuilder ->
                Tuples.of(poolBuilder.sizeBetween(0, 1)
                        .evictionPredicate((poolable, metadata) -> metadata.idleTime() >= 4000)
                        .evictInBackground(Duration.ofSeconds(5), vts)
                        .destroyHandler(i -> Mono.fromRunnable(destroyCounter::incrementAndGet))
                        .clock(SchedulerClock.of(vts))
                        .buildPool(), execsIter.next()));

        PooledRef<Integer> ref1 = pool.acquire().block();
        PooledRef<Integer> ref2 = pool.acquire().block();

        ref1.release().block();
        assertThatCode(() -> vts.advanceTimeBy(Duration.ofSeconds(10))).doesNotThrowAnyException();
        ref2.release().block();
        ref2 = pool.acquire().block();
        assertThat(ref2.poolable()).as("allocations").isEqualTo(2);
        ref2.release().block();
        assertThat(allocCounter).as("allocations").hasValue(2);
        assertThat(pool.metrics().allocatedSize()).as("allocations").isEqualTo(1);
        pool.disposeLater().block(Duration.ofSeconds(3));
    }

    static final class MyResource {
        volatile long releaseTimestamp;
        final int id;
        final VirtualTimeScheduler vts;

        MyResource(VirtualTimeScheduler vts, int id) {
            this.vts = vts;
            this.id = id;
        }

        void release() {
            releaseTimestamp = vts.now(TimeUnit.MILLISECONDS);
        }
    }
}