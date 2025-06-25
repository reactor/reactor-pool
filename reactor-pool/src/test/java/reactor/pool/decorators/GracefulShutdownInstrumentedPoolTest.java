/*
 * Copyright (c) 2021-2025 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.pool.InstrumentedPool;
import reactor.pool.PoolBuilder;
import reactor.pool.PoolConfig;
import reactor.pool.PoolShutdownException;
import reactor.pool.PooledRef;
import reactor.pool.decorators.GracefulShutdownInstrumentedPool.GracefulRef;
import reactor.test.StepVerifier;
import reactor.test.StepVerifierOptions;
import reactor.test.scheduler.VirtualTimeScheduler;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.times;

/**
 * @author Simon Baslé
 */
class GracefulShutdownInstrumentedPoolTest {

	@Test
	void smokeTestDisposeGracefully() {
		GracefulShutdownInstrumentedPool<String> pool = PoolBuilder.from(Mono.just("foo"))
			.sizeBetween(0, 5)
			.buildPoolAndDecorateWith(InstrumentedPoolDecorators::gracefulShutdown);

		PooledRef<String> ref1 = pool.acquire().block();
		assertThat(ref1).isNotNull();

		pool.disposeGracefully(Duration.ofMinutes(1)).subscribe();

		assertThat(pool.getOriginalPool().isDisposed()).as("original pool isDisposed").isFalse();
		assertThat(pool.isDisposed()).as("wrapping pool isDisposed").isFalse();
		assertThat(pool.isGracefullyShuttingDown()).as("wrapping pool isGracefullyShuttingDown").isTrue();

		StepVerifier.create(pool.acquire())
			.expectErrorMessage("The pool is being gracefully shut down and won't accept new acquire calls")
			.verify();

		ref1.release().block();

		await().atMost(1, TimeUnit.SECONDS).untilAsserted(() -> {
				assertThat(pool.getOriginalPool().isDisposed()).as("original pool isDisposed").isTrue();
				assertThat(pool.isDisposed()).as("wrapping pool isDisposed").isTrue();

			}
		);
	}

	@Test
	void cantInstantiateWithNull() {
		assertThatNullPointerException().isThrownBy(() -> new GracefulShutdownInstrumentedPool<>(null))
			.withMessage("originalPool");
	}

	@Test
	void canAccessOriginalPool() {
		InstrumentedPool<String> pool = PoolBuilder.from(Mono.just("foo")).buildPool();
		GracefulShutdownInstrumentedPool<String> gsPool = InstrumentedPoolDecorators.gracefulShutdown(pool);

		assertThat(gsPool.getOriginalPool()).isSameAs(pool);
	}

	@Test
	void poolDelegatedMethods() {
		PoolConfig<String> config = PoolBuilder.from(Mono.just("foo")).buildPool().config();

		@SuppressWarnings("unchecked") InstrumentedPool<String> pool = Mockito.mock(InstrumentedPool.class);
		Mockito.when(pool.config()).thenReturn(config);

		GracefulShutdownInstrumentedPool<?> gsPool = InstrumentedPoolDecorators.gracefulShutdown(pool);

		gsPool.metrics();
		gsPool.config();
		gsPool.warmup();
		gsPool.isDisposed();
		gsPool.disposeLater();

		Mockito.verify(pool).metrics();
		//config is invoked at construction
		Mockito.verify(pool, times(2)).config();
		Mockito.verify(pool).warmup();
		Mockito.verify(pool).isDisposed();
		Mockito.verify(pool).disposeLater();
	}


	@Test
	void poolRefDelegatedMethods() {
		GracefulShutdownInstrumentedPool<String> gsPool = PoolBuilder.from(Mono.just("foo"))
			.buildPoolAndDecorateWith(InstrumentedPoolDecorators::gracefulShutdown);

		GracefulRef ref = (GracefulRef) gsPool.acquire().block();
		assertThat(ref).isNotNull();

		assertThat(ref.poolable()).as("poolable").isSameAs(ref.originalRef.poolable());
		assertThat(ref.metadata()).as("metadata").isSameAs(ref.originalRef.metadata());
	}

	@Test
	void timeoutSchedulerIsOriginalEvictInBackground() {
		Scheduler scheduler = Schedulers.newSingle("test");
		final GracefulShutdownInstrumentedPool<Object> pool =
			PoolBuilder.from(Mono.empty())
				.evictInBackground(Duration.ofHours(1), scheduler)
				.buildPoolAndDecorateWith(InstrumentedPoolDecorators::gracefulShutdown);

		//no need to actually let the scheduler live
		scheduler.dispose();

		assertThat(pool.timeoutScheduler).isSameAs(scheduler);
	}

	@Test
	void timeoutSchedulerDefaultsToParallelIfEvictNotSet() {
		final GracefulShutdownInstrumentedPool<Object> pool =
			PoolBuilder.from(Mono.empty())
				.buildPoolAndDecorateWith(InstrumentedPoolDecorators::gracefulShutdown);

		assertThat(pool.timeoutScheduler)
			.isSameAs(Schedulers.parallel())
			.isNotSameAs(pool.config().evictInBackgroundScheduler());
	}

	@Test
	void timeoutSchedulerDefaultsToParallelIfConfigInaccessible() {
		InstrumentedPool<?> pool = new InstrumentedPool<Object>() {
			@Override
			public PoolMetrics metrics() {
				return null;
			}

			@Override
			public Mono<Integer> warmup() {
				return null;
			}

			@Override
			public Mono<PooledRef<Object>> acquire() {
				return null;
			}

			@Override
			public Mono<PooledRef<Object>> acquire(Duration timeout) {
				return null;
			}

			@Override
			public Mono<Void> disposeLater() {
				return null;
			}

			@Override
			public PoolConfig<Object> config() {
				throw new UnsupportedOperationException("test pool");
			}
		};

		assertThatExceptionOfType(UnsupportedOperationException.class)
			.isThrownBy(pool::config);

		GracefulShutdownInstrumentedPool<?> gsPool = InstrumentedPoolDecorators.gracefulShutdown(pool);

		assertThat(gsPool.timeoutScheduler).isSameAs(Schedulers.parallel());
	}

	@Test
	void disposeLaterDirectlyDisposesUnderlyingPool() {
		InstrumentedPool<String> pool = PoolBuilder.from(Mono.just("foo")).buildPool();
		GracefulShutdownInstrumentedPool<String> gsPool = InstrumentedPoolDecorators.gracefulShutdown(pool);

		//dispose the pool
		StepVerifier.create(gsPool.disposeLater()).verifyComplete();

		assertThat(pool.isDisposed()).as("pool isDisposed").isTrue();
		assertThat(gsPool.isDisposed()).as("gsPool isDisposed").isTrue();
	}

	@Test
	void disposeGracefullyIsImmediateIfPoolIdle() {
		InstrumentedPool<String> pool = PoolBuilder.from(Mono.just("foo")).buildPool();
		GracefulShutdownInstrumentedPool<String> gsPool = InstrumentedPoolDecorators.gracefulShutdown(pool);

		//dispose the pool with grace period
		StepVerifier.create(gsPool.disposeGracefully(Duration.ofSeconds(5)))
			.expectComplete()
			.verify(Duration.ofMillis(100));

		assertThat(pool.isDisposed()).as("pool isDisposed").isTrue();
		assertThat(gsPool.isDisposed()).as("gsPool isDisposed").isTrue();
	}

	@Test
	void disposeGracefullyTwiceOnIdlePool() {
		InstrumentedPool<String> pool = PoolBuilder.from(Mono.just("foo")).buildPool();
		GracefulShutdownInstrumentedPool<String> gsPool = InstrumentedPoolDecorators.gracefulShutdown(pool);

		//dispose the pool with grace period
		Mono<Void> dispose1 = gsPool.disposeGracefully(Duration.ofSeconds(5));
		Mono<Void> dispose2 = gsPool.disposeGracefully(Duration.ofSeconds(10));

		StepVerifier.create(dispose1).expectComplete().verify(Duration.ofMillis(200));
		StepVerifier.create(dispose2).expectComplete().verify(Duration.ofMillis(200));

		assertThat(dispose1).isSameAs(dispose2);
	}

	@Test
	void disposeGracefullyTwiceWithActualTimeoutWaitForSameTime() {
		InstrumentedPool<String> pool = PoolBuilder.from(Mono.just("foo")).buildPool();
		GracefulShutdownInstrumentedPool<String> gsPool = InstrumentedPoolDecorators.gracefulShutdown(pool);

		//this will cause graceful shutdown to block
		PooledRef<String> ref1 = gsPool.acquire().block();
		assertThat(ref1).isNotNull();

		final boolean[] timedOut = new boolean[2];
		final Mono<Void>[] disposeMonos = new Mono[2];
		//dispose the pool with grace period, twice: 2s and 10s (the later duration should be ignored)
		disposeMonos[0] = gsPool.disposeGracefully(Duration.ofSeconds(2));
		disposeMonos[1] = gsPool.disposeGracefully(Duration.ofSeconds(10));

		Duration duration = StepVerifier.create(Mono.when(
				disposeMonos[0].onErrorResume(TimeoutException.class, e -> { timedOut[0] = true; return Mono.empty();} ),
				disposeMonos[1].onErrorResume(TimeoutException.class, e -> { timedOut[1] = true; return Mono.empty();} )
			))
			.expectComplete()
			.verify(Duration.ofSeconds(15));

		assertThat(disposeMonos[0]).as("same dispose monos").isSameAs(disposeMonos[1]);
		assertThat(duration.toMillis()).as("2s takes precedence").isCloseTo(2000, Offset.offset(200L));
		assertThat(timedOut).as("both disposeMonos errored with a TimeoutException").containsExactly(true, true);
	}

	@Test
	void acquireIsFailFastIfShuttingDownGracefully() {
		InstrumentedPool<String> pool = PoolBuilder.from(Mono.just("foo")).buildPool();
		GracefulShutdownInstrumentedPool<String> gsPool = InstrumentedPoolDecorators.gracefulShutdown(pool);

		//dispose the pool with grace period. don't wait for the completion as we want to test what happens in the meantime
		gsPool.disposeGracefully(Duration.ofSeconds(5)).subscribe();

		StepVerifier.create(gsPool.acquire())
				.verifyErrorSatisfies(e -> assertThat(e)
					.isInstanceOf(PoolShutdownException.class)
					.hasMessage("The pool is being gracefully shut down and won't accept new acquire calls"));
	}

	@Test
	void acquireTtlIsFailFastIfShuttingDownGracefully() {
		InstrumentedPool<String> pool = PoolBuilder.from(Mono.just("foo")).buildPool();
		GracefulShutdownInstrumentedPool<String> gsPool = InstrumentedPoolDecorators.gracefulShutdown(pool);

		//dispose the pool with grace period. don't wait for the completion as we want to test what happens in the meantime
		gsPool.disposeGracefully(Duration.ofSeconds(5)).subscribe();

		StepVerifier.create(gsPool.acquire(Duration.ofSeconds(5)))
				.verifyErrorSatisfies(e -> assertThat(e)
					.isInstanceOf(PoolShutdownException.class)
					.hasMessage("The pool is being gracefully shut down and won't accept new acquire calls"));
	}

	@Test
	void disposeGracefullyTriggersTimeoutIfRefNotReleased() {
		VirtualTimeScheduler scheduler = VirtualTimeScheduler.create();
		InstrumentedPool<String> pool = PoolBuilder.from(Mono.just("foo"))
			.evictInBackground(Duration.ofHours(1), scheduler)
			.buildPool();
		GracefulShutdownInstrumentedPool<String> gsPool = InstrumentedPoolDecorators.gracefulShutdown(pool);

		PooledRef<String> ref1 = gsPool.acquire().block();
		assertThat(ref1).isNotNull();

		StepVerifier.create(gsPool.disposeGracefully(Duration.ofSeconds(20)),
				StepVerifierOptions.create().virtualTimeSchedulerSupplier(() -> scheduler))
			.expectSubscription()
			.thenAwait(Duration.ofSeconds(20))
			.expectErrorMessage("Pool has forcefully shut down after graceful timeout of PT20S")
			.verify();
	}

	@Test
	void isInGracePeriod() {
		GracefulShutdownInstrumentedPool<String> gsPool = PoolBuilder.from(Mono.just("foo"))
			.buildPoolAndDecorateWith(InstrumentedPoolDecorators::gracefulShutdown);

		PooledRef<String> ref = gsPool.acquire().block();
		assertThat(ref).isNotNull();

		assertThat(gsPool.isGracefullyShuttingDown()).as("isGracefullyShuttingDown before").isFalse();
		assertThat(gsPool.isInGracePeriod()).as("isInGracePeriod before").isFalse();

		gsPool.disposeGracefully(Duration.ofMillis(500)).subscribe(v -> {}, e -> {});

		assertThat(gsPool.isGracefullyShuttingDown()).as("isGracefullyShuttingDown during").isTrue();
		assertThat(gsPool.isInGracePeriod()).as("isInGracePeriod before").isTrue();

		await().atMost(1, TimeUnit.SECONDS)
			.untilAsserted(() -> {
				assertThat(gsPool.isGracefullyShuttingDown()).as("isGracefullyShuttingDown after").isTrue();
				assertThat(gsPool.isInGracePeriod()).as("isInGracePeriod after").isFalse();
				assertThat(gsPool.isDisposed()).as("isDisposed after").isTrue();

			});
	}

	void gracefulShutdownTerminatesWhen(BiFunction<Long, PooledRef<Long>, Mono<Void>> refTermination) {
		AtomicLong source = new AtomicLong();
		List<PooledRef<Long>> acquired = new ArrayList<>();

		GracefulShutdownInstrumentedPool<Long> gsPool = PoolBuilder.from(Mono.fromSupplier(source::incrementAndGet))
			.sizeBetween(0, 10)
			.buildPoolAndDecorateWith(InstrumentedPoolDecorators::gracefulShutdown);

		Flux.range(1, 10)
			.flatMap(i -> gsPool.acquire())
			.doOnNext(acquired::add)
			.blockLast();

		AtomicReference<SignalType> termination = new AtomicReference<>();
		gsPool.disposeGracefully(Duration.ofSeconds(200))
			.doFinally(termination::set)
			.log()
			.subscribe();

		AtomicInteger refCompleteCount = new AtomicInteger();
		AtomicInteger refErrorCount = new AtomicInteger();

		for (PooledRef<Long> ref : acquired) {
			long index = ref.poolable();
			refTermination.apply(index, ref)
				.subscribe(v -> {}, e -> refErrorCount.incrementAndGet(), refCompleteCount::incrementAndGet);
			if (index < 10) {
				assertThat(gsPool.isInGracePeriod()).as("inGracePeriod during loop #" + index).isTrue();
			}
		}

		assertThat(refCompleteCount).as("refCompleteCount").hasValue(10);
		assertThat(refErrorCount).as("refErrorCount").hasValue(0);
		assertThat(termination).as("poll disposeGracefully end type").hasValue(SignalType.ON_COMPLETE);

		assertThat(gsPool.isInGracePeriod()).as("inGracePeriod after invalidation/release").isFalse();
		assertThat(gsPool.isDisposed()).as("isDisposed after invalidation/release").isTrue();
		InstrumentedPool.PoolMetrics metrics = gsPool.metrics();
		assertThat(metrics.idleSize()).as("idleSize").isZero();
		assertThat(metrics.acquiredSize()).as("acquiredSize").isZero();
		assertThat(metrics.pendingAcquireSize()).as("pendingAcquireSize").isZero();
		assertThat(metrics.allocatedSize()).as("allocatedSize").isZero();
	}

	@Test
	void gracefulShutdownTerminatesWhenAllInvalidated() {
		gracefulShutdownTerminatesWhen((index, ref) -> ref.invalidate());
	}

	@Test
	void gracefulShutdownTerminatesWhenAllReleased() {
		gracefulShutdownTerminatesWhen((index, ref) -> ref.release());
	}

	@Test
	void gracefulShutdownTerminatesWhenMixOfReleasedAndInvalidated() {
		gracefulShutdownTerminatesWhen((index, ref) -> index % 2 == 0 ? ref.invalidate() : ref.release());
	}
	
	@Test
	void gracefulRefDuplicateInvalidate() {
		AtomicLong source = new AtomicLong();

		GracefulShutdownInstrumentedPool<Long> gsPool = PoolBuilder.from(Mono.fromSupplier(source::incrementAndGet))
			.sizeBetween(0, 10)
			.buildPoolAndDecorateWith(InstrumentedPoolDecorators::gracefulShutdown);

		PooledRef<Long> ref1 = gsPool.acquire().block();
		assertThat(ref1).isNotNull();
		PooledRef<Long> ref2 = gsPool.acquire().block();
		assertThat(ref2).isNotNull();

		assertThat(gsPool.acquireTracker).hasValue(2);

		ref1.invalidate().block();
		ref1.invalidate().block();
		ref1.invalidate().block();
		ref1.invalidate().block();
		ref1.invalidate().block();


		assertThat(gsPool.acquireTracker).as("ref1 only").hasValue(1);

		ref2.invalidate().block();
		assertThat(gsPool.acquireTracker).as("ref1 and ref2").hasValue(0);
	}

	@Test
	void gracefulRefDuplicateReleaseNoOp() {
		AtomicLong source = new AtomicLong();

		GracefulShutdownInstrumentedPool<Long> gsPool = PoolBuilder.from(Mono.fromSupplier(source::incrementAndGet))
			.sizeBetween(0, 10)
			.buildPoolAndDecorateWith(InstrumentedPoolDecorators::gracefulShutdown);

		PooledRef<Long> ref1 = gsPool.acquire().block();
		assertThat(ref1).isNotNull();
		PooledRef<Long> ref2 = gsPool.acquire().block();
		assertThat(ref2).isNotNull();

		assertThat(gsPool.acquireTracker).hasValue(2);

		ref1.release().block();
		ref1.release().block();
		ref1.release().block();
		ref1.release().block();
		ref1.release().block();


		assertThat(gsPool.acquireTracker).as("ref1 only").hasValue(1);

		ref2.invalidate().block();
		assertThat(gsPool.acquireTracker).as("ref1 and ref2").hasValue(0);
	}

	@Test
	void gracefulRefDuplicateInvalidateAndReleaseNoOp() {
		AtomicLong source = new AtomicLong();

		GracefulShutdownInstrumentedPool<Long> gsPool = PoolBuilder.from(Mono.fromSupplier(source::incrementAndGet))
			.sizeBetween(0, 10)
			.buildPoolAndDecorateWith(InstrumentedPoolDecorators::gracefulShutdown);

		PooledRef<Long> ref1 = gsPool.acquire().block();
		assertThat(ref1).isNotNull();
		PooledRef<Long> ref2 = gsPool.acquire().block();
		assertThat(ref2).isNotNull();

		assertThat(gsPool.acquireTracker).hasValue(2);

		ref1.release().block();
		ref1.invalidate().block();
		ref1.release().block();
		ref1.invalidate().block();
		ref1.release().block();

		assertThat(gsPool.acquireTracker).as("ref1 only").hasValue(1);

		ref2.invalidate().block();
		assertThat(gsPool.acquireTracker).as("ref1 and ref2").hasValue(0);
	}

	@Test
	void acquiredCountIfAcquireCancelled() {
		AtomicLong source = new AtomicLong();

		GracefulShutdownInstrumentedPool<Long> gsPool = PoolBuilder.from(Mono.fromSupplier(source::incrementAndGet))
			.sizeBetween(0, 1)
			.buildPoolAndDecorateWith(InstrumentedPoolDecorators::gracefulShutdown);

		//go to capacity
		gsPool.acquire().block();
		//pending acquire
		Disposable acquire2 = gsPool.acquire().subscribe();

		assertThat(gsPool.acquireTracker).as("acquireTracker before cancel").hasValue(2);

		acquire2.dispose();

		assertThat(gsPool.acquireTracker).as("acquireTracker after cancel").hasValue(1);
		assertThat(gsPool.metrics().pendingAcquireSize()).as("pendingAcquireSize").isZero();
	}

	@Test
	void acquiredCountIfAcquireTtlCancelled() {
		AtomicLong source = new AtomicLong();

		GracefulShutdownInstrumentedPool<Long> gsPool = PoolBuilder.from(Mono.fromSupplier(source::incrementAndGet))
			.sizeBetween(0, 1)
			.buildPoolAndDecorateWith(InstrumentedPoolDecorators::gracefulShutdown);

		//go to capacity
		gsPool.acquire().block();
		//pending acquire
		Disposable acquire2 = gsPool.acquire(Duration.ofSeconds(5)).subscribe();

		assertThat(gsPool.acquireTracker).as("acquireTracker before cancel").hasValue(2);

		acquire2.dispose();

		assertThat(gsPool.acquireTracker).as("acquireTracker after cancel").hasValue(1);
		assertThat(gsPool.metrics().pendingAcquireSize()).as("pendingAcquireSize").isZero();
	}
}