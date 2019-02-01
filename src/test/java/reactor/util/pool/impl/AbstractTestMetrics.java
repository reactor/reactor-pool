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

package reactor.util.pool.impl;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.util.pool.api.*;
import reactor.util.pool.metrics.InMemoryPoolMetrics;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

/**
 * @author Simon Baslé
 */
abstract class AbstractTestMetrics {

    private InMemoryPoolMetrics recorder;

    @BeforeEach
    void initRecorder() {
        this.recorder = new InMemoryPoolMetrics();
    }

    abstract <T> Pool<T> createPool(PoolConfig<T> poolConfig);

    @Test
    void recordsAllocationInConstructor() {
        AtomicBoolean flip = new AtomicBoolean();
        PoolConfig<String> config = PoolConfigBuilder.allocateWith(
                Mono.defer(() -> {
                    if (flip.compareAndSet(false, true))
                        return Mono.just("foo").delayElement(Duration.ofMillis(100));
                    else {
                        flip.compareAndSet(true, false);
                        return Mono.error(new IllegalStateException("boom"));
                    }
                }))
                .initialSizeOf(10)
                .recordMetricsWith(recorder)
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
        PoolConfig<String> config = PoolConfigBuilder.allocateWith(
                Mono.defer(() -> {
                    if (flip.compareAndSet(false, true))
                        return Mono.just("foo").delayElement(Duration.ofMillis(100));
                    else {
                        flip.compareAndSet(true, false);
                        return Mono.error(new IllegalStateException("boom"));
                    }
                }))
                .recordMetricsWith(recorder)
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
        PoolConfig<String> config = PoolConfigBuilder.allocateWith(Mono.just("foo"))
                .resetResourcesWith(s -> {
                    if (flip.compareAndSet(false, true))
                        return Mono.delay(Duration.ofMillis(100)).then();
                    else {
                        flip.compareAndSet(true, false);
                        return Mono.empty();
                    }
                })
                .recordMetricsWith(recorder)
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
        PoolConfig<String> config = PoolConfigBuilder.allocateWith(Mono.just("foo"))
                .evictionPredicate(t -> true)
                .destroyResourcesWith(s -> {
                    if (flip.compareAndSet(false, true))
                        return Mono.delay(Duration.ofMillis(500)).then();
                    else {
                        flip.compareAndSet(true, false);
                        return Mono.empty();
                    }
                })
                .recordMetricsWith(recorder)
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
        PoolConfig<String> config = PoolConfigBuilder.allocateWith(Mono.fromCallable(() -> content.getAndSet("bar")))
                .evictionPredicate(EvictionPredicates.poolableMatches("foo"::equals))
                .recordMetricsWith(recorder)
                .buildConfig();
        Pool<String> pool = createPool(config);

        pool.acquire().flatMap(PooledRef::release).block();
        pool.acquire().flatMap(PooledRef::release).block();

        assertThat(recorder.getResetCount()).as("reset").isEqualTo(2);
        assertThat(recorder.getDestroyCount()).as("destroy").isEqualTo(1);
        assertThat(recorder.getRecycledCount()).as("recycle").isEqualTo(1);
    }
}
