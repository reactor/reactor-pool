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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.util.pool.api.Pool;
import reactor.util.pool.api.PoolConfig;
import reactor.util.pool.api.PoolConfigBuilder;
import reactor.util.pool.api.PooledRef;
import reactor.util.pool.metrics.InMemoryPoolMetrics;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

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
    void recordAllocationInConstructor() {
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
    void recordAllocationInBorrow() {
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
        long maxSuccess = recorder.getAllocationSuccessHistogram().getMaxValue();
        assertThat(minSuccess)
                .as("allocation success latency")
                .isBetween(100L, 150L)
                .isNotEqualTo(maxSuccess);

        long minError = recorder.getAllocationErrorHistogram().getMinValue();
        long maxError = recorder.getAllocationErrorHistogram().getMaxValue();
        assertThat(minError)
                .as("allocation error latency")
                .isBetween(0L, 15L)
                .isNotEqualTo(maxError);
    }

    @Test
    void test() {//FIXME continue here
        recorder.getRecycledCount();
    }
}
