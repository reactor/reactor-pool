/*
 * Copyright (c) 2018-2022 VMware Inc. or its affiliates, All Rights Reserved.
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

import io.reactivex.Flowable;
import org.junit.jupiter.api.Test;

import reactor.core.Disposable;
import reactor.core.Disposables;
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

		PoolBuilder<Integer, PoolConfig<Integer>> builder = PoolBuilder.from(probe.mono());
		final PoolConfig<Integer> config = builder.buildConfig();

		StepVerifier.create(config.allocator())
					.expectNext(1)
					.verifyComplete();

		probe.assertWasNotCancelled();
		probe.assertWasSubscribed();
	}

	@Test
	void fromPublisherFluxCancels() {
		AtomicInteger source = new AtomicInteger();
		final PublisherProbe<Integer> probe = PublisherProbe.of(Mono.fromCallable(source::incrementAndGet).repeat(3));

		PoolBuilder<Integer, PoolConfig<Integer>> builder = PoolBuilder.from(probe.flux());
		final PoolConfig<Integer> config = builder.buildConfig();

		StepVerifier.create(config.allocator())
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

		PoolBuilder<Integer, PoolConfig<Integer>> builder = PoolBuilder.from(source);
		final PoolConfig<Integer> config = builder.buildConfig();

		StepVerifier.create(config.allocator())
					.expectNext(1)
					.verifyComplete();

		assertThat(cancelled).isTrue();
	}

	@Test
	void fromDowncastedMono() {
		Mono<Long> source = Flux.range(1, 10).count();
		Predicate<Number> numberPredicate = n -> n.intValue() == 10;

		PoolBuilder<Number, PoolConfig<Number>> poolBuilder = PoolBuilder.from(source);
		PoolConfig<Number> config = poolBuilder.buildConfig();

		StepVerifier.create(config.allocator())
					.assertNext(n -> assertThat(n).matches(numberPredicate))
					.verifyComplete();
	}

	@Test
	void pendingAcquireTimerCustomized() {
		final BiFunction<Runnable, Duration, Disposable> customizedBiFunction = (r, d) -> {
			r.run();
			return Disposables.disposed();
		};
		PoolBuilder<Integer, PoolConfig<Integer>> poolBuilder = PoolBuilder.from(Mono.just(1))
				.pendingAcquireTimer(customizedBiFunction);
		PoolConfig<Integer> config = poolBuilder.buildConfig();

		assertThat(config.pendingAcquireTimer()).isSameAs(customizedBiFunction);
	}

	@Test
	void customizedConfig() {
		FooBarOnlyPool<String> customPool =
				PoolBuilder.from(Mono.just("hello"))
						   .extraConfiguration(it -> new FooExtraConfig<>(it).foo(true))
						   .extraConfiguration(fc -> new FooBarExtraConfig<>(fc).bar(true))
						   .build(FooBarOnlyPool::new);

		assertThat(customPool.config.isBar()).as("bar").isTrue();
		assertThat(customPool.config.isFoo()).as("foo").isTrue();
	}

	static class FooBarOnlyPool<T> implements Pool<T> {

		private final FooBarExtraConfig<T> config;

		public FooBarOnlyPool(FooBarExtraConfig<T> config) {
			this.config = config;
		}

		@Override
		public Mono<Integer> warmup() {
			return Mono.just(0);
		}

		@Override
		public Mono<PooledRef<T>> acquire() {
			return null;
		}

		@Override
		public Mono<PooledRef<T>> acquire(Duration timeout) {
			return null;
		}

		@Override
		public Mono<Void> disposeLater() {
			return null;
		}
	}

	static class FooExtraConfig<T> extends DefaultPoolConfig<T> {

		private boolean isFoo = false;

		FooExtraConfig(PoolConfig<T> toCopy) {
			super(toCopy);
		}

		public FooExtraConfig<T> foo(boolean isFoo) {
			this.isFoo = isFoo;
			return this;
		}

		public boolean isFoo() {
			return isFoo;
		}
	}

	static class FooBarExtraConfig<T> extends FooExtraConfig<T> {

		private boolean isBar;

		public FooBarExtraConfig(FooExtraConfig<T> toCopy) {
			super(toCopy);
			foo(toCopy.isFoo);
			this.isBar = false;
		}

		public FooBarExtraConfig<T> bar(boolean isBar) {
			this.isBar = isBar;
			return this;
		}

		public boolean isBar() {
			return isBar;
		}
	}
}