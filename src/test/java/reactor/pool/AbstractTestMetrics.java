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

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.pool.AbstractPool.DefaultPoolConfig;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.awaitility.Awaitility.await;

/**
 * @author Simon Baslé
 */
abstract class AbstractTestMetrics {

    protected InMemoryPoolMetrics recorder;

    @BeforeEach
    void initRecorder() {
        this.recorder = new InMemoryPoolMetrics();
    }

    abstract <T> Pool<T> createPool(DefaultPoolConfig<T> poolConfig);

    @Test
    void recordsAllocationInConstructor() {
        AtomicBoolean flip = new AtomicBoolean();
        //note the starter method here is irrelevant, only the config is created and passed to createPool
        DefaultPoolConfig<String> config = PoolBuilder.from(
                Mono.defer(() -> {
                    if (flip.compareAndSet(false, true))
                        return Mono.just("foo").delayElement(Duration.ofMillis(100));
                    else {
                        flip.compareAndSet(true, false);
                        return Mono.error(new IllegalStateException("boom"));
                    }
                }))
                .initialSize(10)
                .metricsRecorder(recorder)
                .buildConfig();

        assertThatIllegalStateException()
                .isThrownBy(() -> createPool(config));

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

    @Test
    void recordsAllocationInBorrow() {
        AtomicBoolean flip = new AtomicBoolean();
        //note the starter method here is irrelevant, only the config is created and passed to createPool
        DefaultPoolConfig<String> config = PoolBuilder.from(
                Mono.defer(() -> {
                    if (flip.compareAndSet(false, true))
                        return Mono.just("foo").delayElement(Duration.ofMillis(100));
                    else {
                        flip.compareAndSet(true, false);
                        return Mono.error(new IllegalStateException("boom"));
                    }
                }))
                .metricsRecorder(recorder)
                .buildConfig();
        Pool<String> pool = createPool(config);

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

    @Test
    void recordsResetLatencies() {
        AtomicBoolean flip = new AtomicBoolean();
        //note the starter method here is irrelevant, only the config is created and passed to createPool
        DefaultPoolConfig<String> config = PoolBuilder.from(Mono.just("foo"))
                .releaseHandler(s -> {
                    if (flip.compareAndSet(false, true))
                        return Mono.delay(Duration.ofMillis(100)).then();
                    else {
                        flip.compareAndSet(true, false);
                        return Mono.empty();
                    }
                })
                .metricsRecorder(recorder)
                .buildConfig();
        Pool<String> pool = createPool(config);

        pool.acquire().flatMap(PooledRef::release).block();
        pool.acquire().flatMap(PooledRef::release).block();

        assertThat(recorder.getResetCount()).as("reset").isEqualTo(2);

        long min = recorder.getResetHistogram().getMinValue();
        assertThat(min).isCloseTo(0L, Offset.offset(50L));

        long max = recorder.getResetHistogram().getMaxValue();
        assertThat(max).isCloseTo(100L, Offset.offset(50L));
    }

    @Test
    void recordsDestroyLatencies() {
        AtomicBoolean flip = new AtomicBoolean();
        //note the starter method here is irrelevant, only the config is created and passed to createPool
        DefaultPoolConfig<String> config = PoolBuilder.from(Mono.just("foo"))
                .evictionPredicate(t -> true)
                .destroyHandler(s -> {
                    if (flip.compareAndSet(false, true))
                        return Mono.delay(Duration.ofMillis(500)).then();
                    else {
                        flip.compareAndSet(true, false);
                        return Mono.empty();
                    }
                })
                .metricsRecorder(recorder)
                .buildConfig();
        Pool<String> pool = createPool(config);

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

    @Test
    void recordsResetVsRecycle() {
        AtomicReference<String> content = new AtomicReference<>("foo");
        //note the starter method here is irrelevant, only the config is created and passed to createPool
        DefaultPoolConfig<String> config = PoolBuilder.from(Mono.fromCallable(() -> content.getAndSet("bar")))
                .evictionPredicate(EvictionPredicates.poolableMatches("foo"::equals))
                .metricsRecorder(recorder)
                .buildConfig();
        Pool<String> pool = createPool(config);

        pool.acquire().flatMap(PooledRef::release).block();
        pool.acquire().flatMap(PooledRef::release).block();

        assertThat(recorder.getResetCount()).as("reset").isEqualTo(2);
        assertThat(recorder.getDestroyCount()).as("destroy").isEqualTo(1);
        assertThat(recorder.getRecycledCount()).as("recycle").isEqualTo(1);
    }

    @Test
    void recordsLifetime() throws InterruptedException {
        AtomicInteger allocCounter = new AtomicInteger();
        AtomicInteger destroyCounter = new AtomicInteger();
        //note the starter method here is irrelevant, only the config is created and passed to createPool
        DefaultPoolConfig<Integer> config = PoolBuilder.from(Mono.fromCallable(allocCounter::incrementAndGet))
                .allocationStrategy(AllocationStrategies.allocatingMax(2))
                .evictionPredicate(EvictionPredicates.acquiredMoreThan(2))
                .destroyHandler(i -> Mono.fromRunnable(destroyCounter::incrementAndGet))
                .metricsRecorder(recorder)
                .buildConfig();
        Pool<Integer> pool = createPool(config);

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

    @Test
    void recordsIdleTimeFromConstructor() throws InterruptedException {
        AtomicInteger allocCounter = new AtomicInteger();
        //note the starter method here is irrelevant, only the config is created and passed to createPool
        DefaultPoolConfig<Integer> config = PoolBuilder.from(Mono.fromCallable(allocCounter::incrementAndGet))
                .allocationStrategy(AllocationStrategies.allocatingMax(2))
                .initialSize(2)
                .metricsRecorder(recorder)
                .buildConfig();
        Pool<Integer> pool = createPool(config);

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

    @Test
    void recordsIdleTimeBetweenAcquires() throws InterruptedException {
        AtomicInteger allocCounter = new AtomicInteger();
        //note the starter method here is irrelevant, only the config is created and passed to createPool
        DefaultPoolConfig<Integer> config = PoolBuilder.from(Mono.fromCallable(allocCounter::incrementAndGet))
                .allocationStrategy(AllocationStrategies.allocatingMax(2))
                .initialSize(2)
                .metricsRecorder(recorder)
                .buildConfig();
        Pool<Integer> pool = createPool(config);

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
