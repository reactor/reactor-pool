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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

import io.reactivex.Flowable;
import org.junit.jupiter.api.Test;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.test.publisher.PublisherProbe;

import static org.assertj.core.api.Assertions.assertThat;

class PoolBuilderTest {

    @Test
    void idlePredicate() {
        BiPredicate<Object, PooledRefMetadata> predicate = PoolBuilder.idlePredicate(Duration.ofSeconds(3));

        TestUtils.TestPooledRef<String> outbounds = new TestUtils.TestPooledRef<>("anything", 100, 4, 100);
        assertThat(predicate.test(outbounds.poolable, outbounds.metadata())).as("clearly out of bounds").isTrue();

        TestUtils.TestPooledRef<String> inbounds = new TestUtils.TestPooledRef<>("anything", 100, 2, 100);
        assertThat(predicate.test(inbounds.poolable, inbounds.metadata())).as("clearly within bounds").isFalse();

        TestUtils.TestPooledRef<String> barelyOutbounds = new TestUtils.TestPooledRef<>("anything", 100, 3, 100);
        assertThat(predicate.test(barelyOutbounds.poolable, barelyOutbounds.metadata())).as("ttl is inclusive").isTrue();
    }

    @Test
    void fromPublisherMonoDoesntCancel() {
        AtomicInteger source = new AtomicInteger();
        final PublisherProbe<Integer> probe = PublisherProbe.of(Mono.fromCallable(source::incrementAndGet));

        PoolBuilder<Integer> builder = PoolBuilder.from(probe.mono());
        final AbstractPool.DefaultPoolConfig<Integer> config = builder.buildConfig();

        StepVerifier.create(config.allocator)
                    .expectNext(1)
                    .verifyComplete();

        probe.assertWasNotCancelled();
        probe.assertWasSubscribed();
    }

    @Test
    void fromPublisherFluxCancels() {
        AtomicInteger source = new AtomicInteger();
        final PublisherProbe<Integer> probe = PublisherProbe.of(Mono.fromCallable(source::incrementAndGet).repeat(3));

        PoolBuilder<Integer> builder = PoolBuilder.from(probe.flux());
        final AbstractPool.DefaultPoolConfig<Integer> config = builder.buildConfig();

        StepVerifier.create(config.allocator)
                    .expectNext(1)
                    .verifyComplete();

        probe.assertWasCancelled();
        probe.assertWasSubscribed();
    }

    @Test
    void fromPublisherFlowableCancels() {
        AtomicBoolean cancelled = new AtomicBoolean();
        Flowable<Integer> source = Flowable.range(1, 4)
                                  .doOnCancel(() -> cancelled.set(true));

        PoolBuilder<Integer> builder = PoolBuilder.from(source);
        final AbstractPool.DefaultPoolConfig<Integer> config = builder.buildConfig();

        StepVerifier.create(config.allocator)
                    .expectNext(1)
                    .verifyComplete();

        assertThat(cancelled).isTrue();
    }

    @Test
    void fromDowncastedMono() {
        Mono<Long> source = Flux.range(1, 10).count();
        Predicate<Number> numberPredicate = n -> n.intValue() == 10;

        PoolBuilder<Number> poolBuilder = PoolBuilder.from(source);
        AbstractPool.DefaultPoolConfig<Number> config = poolBuilder.buildConfig();

        StepVerifier.create(config.allocator)
                    .assertNext(n -> assertThat(n).matches(numberPredicate))
                    .verifyComplete();
    }

    @Test
    void threadAffinityDefaultToFalse() {
        PoolBuilder<Integer> poolBuilder = PoolBuilder.from(Mono.just(1));
        assertThat(poolBuilder.isThreadAffinity).as("threadAffinity").isFalse();
    }
}