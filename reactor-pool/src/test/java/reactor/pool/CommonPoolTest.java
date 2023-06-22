/*
 * Copyright (c) 2019-2023 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.io.Closeable;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.FormatterClosedException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.Assumptions;
import org.assertj.core.data.Offset;
import org.awaitility.Awaitility;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.pool.InstrumentedPool.PoolMetrics;
import reactor.pool.TestUtils.ParameterizedTestWithName;
import reactor.pool.TestUtils.PoolableTest;
import reactor.scheduler.clock.SchedulerClock;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.test.scheduler.VirtualTimeScheduler;
import reactor.test.util.RaceTestUtils;
import reactor.test.util.TestLogger;
import reactor.util.Loggers;
import reactor.util.context.Context;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.is;

/**
 * @author Simon Baslé
 */
public class CommonPoolTest {

	enum PoolStyle {

		IDLE_LRU(true),
		IDLE_MRU(false);

		private final boolean isLru;

		PoolStyle(boolean isLru) {
			this.isLru = isLru;
		}

		public <T, CONF extends PoolConfig<T>> AbstractPool<T> apply(PoolBuilder<T, CONF> builder) {
			return (AbstractPool<T>) builder.idleResourceReuseOrder(isLru).buildPool();
		}
	}
	
	//keeping the methods below in case more flavors need to be added to CommonPoolTest

	static List<PoolStyle> allPools() {
		return Arrays.asList(PoolStyle.IDLE_LRU, PoolStyle.IDLE_MRU);
	}

	static List<PoolStyle> mruPools() {
		return Collections.singletonList(PoolStyle.IDLE_MRU);
	}

	static List<PoolStyle> lruPools() {
		return Collections.singletonList(PoolStyle.IDLE_LRU);
	}

	@ParameterizedTestWithName
	@MethodSource("allPools")
	void smokeTestFifo(PoolStyle configAdjuster) throws InterruptedException {
		AtomicInteger newCount = new AtomicInteger();

		PoolBuilder<PoolableTest, ?> builder = PoolBuilder
				//default maxUse is 5, but this test relies on it being 2
				.from(Mono.defer(() -> Mono.just(new PoolableTest(newCount.incrementAndGet(), 2))))
				.sizeBetween(2, 3)
				.releaseHandler(pt -> Mono.fromRunnable(pt::clean))
				.evictionPredicate((poolable, metadata) -> !poolable.isHealthy());

		Pool<PoolableTest> pool = configAdjuster.apply(builder);
		pool.warmup().block();

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

		List<PoolableTest> poolables1 = acquired1.stream().map(PooledRef::poolable).collect(Collectors.toList());
		List<PoolableTest> poolables2 = acquired2.stream().map(PooledRef::poolable).collect(Collectors.toList());
		assertThat(poolables1)
				.as("poolable1 and 2 all used up")
				.hasSameElementsAs(poolables2)
				.allMatch(poolable -> poolable.usedUp == 2);

		assertThat(acquired3)
				.as("acquired3 all new")
				.allSatisfy(slot -> assertThat(slot.poolable().usedUp).isZero());
	}

	@ParameterizedTestWithName
	@MethodSource("allPools")
	void smokeTestInScopeFifo(PoolStyle configAdjuster) {
		AtomicInteger newCount = new AtomicInteger();

		PoolBuilder<PoolableTest, ?> builder = PoolBuilder
				//default maxUse is 5, but this test relies on it being 2
				.from(Mono.defer(() -> Mono.just(new PoolableTest(newCount.incrementAndGet(), 2))))
				.sizeBetween(2, 3)
				.releaseHandler(pt -> Mono.fromRunnable(pt::clean))
				.evictionPredicate((poolable, metadata) -> !poolable.isHealthy());
		Pool<PoolableTest> pool = configAdjuster.apply(builder);
		pool.warmup().block();

		TestPublisher<Integer> trigger1 = TestPublisher.create();
		TestPublisher<Integer> trigger2 = TestPublisher.create();
		TestPublisher<Integer> trigger3 = TestPublisher.create();

		List<PoolableTest> acquired1 = new ArrayList<>();

		Mono.when(
				pool.withPoolable(poolable -> Mono.just(poolable).doOnNext(acquired1::add).delayUntil(__ -> trigger1)),
				pool.withPoolable(poolable -> Mono.just(poolable).doOnNext(acquired1::add).delayUntil(__ -> trigger1)),
				pool.withPoolable(poolable -> Mono.just(poolable).doOnNext(acquired1::add).delayUntil(__ -> trigger1))
		).subscribe();

		List<PoolableTest> acquired2 = new ArrayList<>();
		Mono.when(
				pool.withPoolable(poolable -> Mono.just(poolable).doOnNext(acquired2::add).delayUntil(__ -> trigger2)),
				pool.withPoolable(poolable -> Mono.just(poolable).doOnNext(acquired2::add).delayUntil(__ -> trigger2)),
				pool.withPoolable(poolable -> Mono.just(poolable).doOnNext(acquired2::add).delayUntil(__ -> trigger2))
		).subscribe();

		List<PoolableTest> acquired3 = new ArrayList<>();
		Mono.when(
				pool.withPoolable(poolable -> Mono.just(poolable).doOnNext(acquired3::add).delayUntil(__ -> trigger3)),
				pool.withPoolable(poolable -> Mono.just(poolable).doOnNext(acquired3::add).delayUntil(__ -> trigger3)),
				pool.withPoolable(poolable -> Mono.just(poolable).doOnNext(acquired3::add).delayUntil(__ -> trigger3))
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

	@ParameterizedTestWithName
	@MethodSource("allPools")
	void smokeTestAsyncFifo(PoolStyle configAdjuster) throws InterruptedException {
		AtomicInteger newCount = new AtomicInteger();

		PoolBuilder<PoolableTest, ?> builder = PoolBuilder
				//default maxUse is 5, but this test relies on it being 2
				.from(Mono.defer(() -> Mono.just(new PoolableTest(newCount.incrementAndGet(), 2)))
				          .subscribeOn(Schedulers.newParallel("poolable test allocator")))
				.sizeBetween(2, 3)
				.releaseHandler(pt -> Mono.fromRunnable(pt::clean))
				.evictionPredicate((poolable, metadata) -> !poolable.isHealthy());

		Pool<PoolableTest> pool = configAdjuster.apply(builder);
		pool.warmup().block();

		List<PooledRef<PoolableTest>> acquired1 = new CopyOnWriteArrayList<>();
		CountDownLatch latch1 = new CountDownLatch(3);
		pool.acquire().subscribe(acquired1::add, Throwable::printStackTrace, latch1::countDown);
		pool.acquire().subscribe(acquired1::add, Throwable::printStackTrace, latch1::countDown);
		pool.acquire().subscribe(acquired1::add, Throwable::printStackTrace, latch1::countDown);

		List<PooledRef<PoolableTest>> acquired2 = new CopyOnWriteArrayList<>();
		pool.acquire().subscribe(acquired2::add);
		pool.acquire().subscribe(acquired2::add);
		pool.acquire().subscribe(acquired2::add);

		List<PooledRef<PoolableTest>> acquired3 = new CopyOnWriteArrayList<>();
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

			List<PoolableTest> poolables1 = acquired1.stream().map(PooledRef::poolable).collect(Collectors.toList());
			List<PoolableTest> poolables2 = acquired2.stream().map(PooledRef::poolable).collect(Collectors.toList());

			assertThat(poolables1)
					.as("poolables 1 and 2 all used up")
					.hasSameElementsAs(poolables2)
					.allSatisfy(poolable -> assertThat(poolable.usedUp).isEqualTo(2));

			assertThat(acquired3)
					.as("acquired3 all new")
					.allSatisfy(slot -> {
						assertThat(slot).as("acquire3 slot").isNotNull();
						assertThat(slot.poolable()).as("acquire3 poolable").isNotNull();
						assertThat(slot.poolable().usedUp).as("acquire3 usedUp").isZero();
					});
		}
		else {
			fail("not enough new elements generated, missing " + latch3.getCount());
		}
	}

	@ParameterizedTestWithName
	@MethodSource("mruPools")
	void smokeTestMruIdle(PoolStyle configAdjuster) {
		AtomicInteger newCount = new AtomicInteger();
		PoolBuilder<PoolableTest, ?> builder =
				PoolBuilder.from(Mono.fromSupplier(() -> new PoolableTest(newCount.incrementAndGet())))
				           .sizeBetween(0, 3)
				           .releaseHandler(pt -> Mono.fromRunnable(pt::clean))
				           .evictionPredicate((value, metadata) -> !value.isHealthy());
		AbstractPool<PoolableTest> pool = configAdjuster.apply(builder);

		PooledRef<PoolableTest> ref1 = pool.acquire().block();
		PooledRef<PoolableTest> ref2 = pool.acquire().block();
		PooledRef<PoolableTest> ref3 = pool.acquire().block();

		ref2.release().block();
		ref1.release().block();
		ref3.release().block();

		assertThat(pool.idleSize()).as("pool fully idle").isEqualTo(3);

		pool.acquire()
		    .as(StepVerifier::create)
		    .assertNext(ref -> assertThat(ref.poolable().id).as("MRU first call returns ref3").isEqualTo(3))
		    .verifyComplete();

		pool.acquire()
		    .as(StepVerifier::create)
		    .assertNext(ref -> assertThat(ref.poolable().id).as("MRU second call returns ref1").isEqualTo(1))
		    .verifyComplete();

		assertThat(pool.metrics().idleSize()).as("idleSize after 2 MRU calls").isOne();
	}

	@ParameterizedTestWithName
	@MethodSource("allPools")
	void firstAcquireCausesWarmupWithMinSize(PoolStyle configAdjuster)
			throws InterruptedException {
		AtomicInteger allocationCount = new AtomicInteger();

		PoolBuilder<PoolableTest, ?> builder = PoolBuilder
				.from(Mono.fromCallable(() -> {
					int id = allocationCount.incrementAndGet();
					return new PoolableTest(id);
				}))
				.sizeBetween(4, 8)
				.releaseHandler(p -> Mono.fromRunnable(p::clean));

		final Pool<PoolableTest> pool = configAdjuster.apply(builder);
		//notice no explicit warmup in this one
		assertThat(pool).isInstanceOf(InstrumentedPool.class);
		final PoolMetrics metrics = ((InstrumentedPool) pool).metrics();

		assertThat(metrics.allocatedSize()).as("constructor allocated").isEqualTo(0);

		final PooledRef<PoolableTest> firstAcquire = pool.acquire()
		                                                 .block();

		assertThat(metrics.allocatedSize()).as("first acquire allocated").isEqualTo(4);
		assertThat(metrics.idleSize()).as("warmup idle").isEqualTo(3);

		final PooledRef<PoolableTest> secondAcquire = pool.acquire()
		                                                  .block();

		assertThat(metrics.allocatedSize()).as("second acquire allocated").isEqualTo(4);
		assertThat(metrics.idleSize()).as("second acquire idle").isEqualTo(2);

		//give time for an unexpected warmup to take place so that the last assertion is valid
		Thread.sleep(100);
		assertThat(allocationCount).as("total allocations").hasValue(4);
	}

	@ParameterizedTestWithName
	@MethodSource("allPools")
	void destroyBelowMinSizeThreshold(PoolStyle configAdjuster)
			throws InterruptedException {
		AtomicInteger allocationCount = new AtomicInteger();

		PoolBuilder<PoolableTest, ?> builder = PoolBuilder
				.from(Mono.fromCallable(() -> {
					int id = allocationCount.incrementAndGet();
					return new PoolableTest(id);
				}))
				.sizeBetween(3, 4)
				.releaseHandler(p -> Mono.fromRunnable(p::clean));

		final Pool<PoolableTest> pool = configAdjuster.apply(builder);
		assertThat(pool).isInstanceOf(InstrumentedPool.class);
		final PoolMetrics metrics = ((InstrumentedPool) pool).metrics();

		final PooledRef<PoolableTest> ref1 = pool.acquire().block();
		final PooledRef<PoolableTest> ref2 = pool.acquire().block();
		final PooledRef<PoolableTest> ref3 = pool.acquire().block();
		final PooledRef<PoolableTest> ref4 = pool.acquire().block();

		assertThat(metrics.allocatedSize()).as("initial allocated").isEqualTo(4);
		assertThat(metrics.idleSize()).as("initial idle").isEqualTo(0);

		ref1.invalidate().block();
		ref2.invalidate().block();
		ref3.invalidate().block();

		assertThat(metrics.allocatedSize()).as("3 releases: allocated").isEqualTo(1);
		assertThat(metrics.idleSize()).as("3 releases: idle").isEqualTo(0);

		PooledRef<PoolableTest> ref5 = pool.acquire()
		                                   .block();

		assertThat(metrics.allocatedSize()).as("second acquire allocated").isEqualTo(3); //1 borrowed, 1 acquiring, 1 extra matches min of 3
		assertThat(metrics.idleSize()).as("second acquire idle").isEqualTo(1); //only 1 extra created this time

		//give time for an unexpected warmup to take place so that the last assertion is valid
		Thread.sleep(100);
		assertThat(allocationCount).as("total allocations").hasValue(6);
	}

	@ParameterizedTestWithName
	@MethodSource("allPools")
	void returnedNotReleasedIfBorrowerCancelledEarly(PoolStyle configAdjuster) {
		AtomicInteger releasedCount = new AtomicInteger();

		PoolBuilder<PoolableTest, ?> builder = PoolBuilder
				.from(Mono.fromCallable(PoolableTest::new))
				.sizeBetween(1, 1)
				.releaseHandler(poolableTest -> Mono.fromRunnable(() -> {
					poolableTest.clean();
					releasedCount.incrementAndGet();
				}))
				.evictionPredicate((poolable, metadata) -> !poolable.isHealthy());

		Pool<PoolableTest> pool = configAdjuster.apply(builder);
		pool.warmup().block();

		//acquire the only element and hold on to it
		PooledRef<PoolableTest> slot = pool.acquire().block();
		assertThat(slot).isNotNull();

		pool.acquire().subscribe().dispose();

		assertThat(releasedCount).as("before returning").hasValue(0);

		//release the element, which should only mark it once, as the pending acquire should not be visible anymore
		slot.release().block();

		assertThat(releasedCount).as("after returning").hasValue(1);
	}

	@ParameterizedTestWithName
	@MethodSource("allPools")
	void fixedPoolReplenishOnDestroy(PoolStyle configAdjuster) throws Exception{
		AtomicInteger releasedCount = new AtomicInteger();
		AtomicInteger destroyedCount = new AtomicInteger();

		PoolBuilder<PoolableTest, ?> builder = PoolBuilder
				.from(Mono.fromCallable(PoolableTest::new))
				.sizeBetween(1, 1)
				.destroyHandler(poolableTest -> Mono.fromRunnable(() -> {
					poolableTest.clean();
					releasedCount.incrementAndGet();
				}))
				.releaseHandler(poolableTest -> Mono.fromRunnable(() -> {
					poolableTest.clean();
					destroyedCount.incrementAndGet();
				}))
				.evictionPredicate((poolable, metadata) -> !poolable.isHealthy());

		Pool<PoolableTest> pool = configAdjuster.apply(builder);
		pool.warmup().block();

		//acquire the only element
		PooledRef<PoolableTest> slot = pool.acquire().block();
		AtomicReference<PooledRef<PoolableTest>> acquired = new AtomicReference<>();
		CountDownLatch latch = new CountDownLatch(1);
		pool.acquire().subscribe(p -> {
			acquired.set(p);
			latch.countDown();
		});
		assertThat(slot).isNotNull();

		slot.invalidate().block();

		assertThat(releasedCount).as("before returning").hasValue(1);
		assertThat(destroyedCount).as("before returning").hasValue(0);

		//release the element, which should forward to the cancelled second acquire, itself also cleaning
		latch.await(5, TimeUnit.SECONDS);
		assertThat(acquired.get()).isNotNull();

		acquired.get().release().block();

		assertThat(destroyedCount).as("after returning").hasValue(1);
		assertThat(releasedCount).as("after returning").hasValue(1);
	}


	@ParameterizedTestWithName
	@MethodSource("allPools")
	void returnedNotReleasedIfBorrowerInScopeCancelledEarly(PoolStyle configAdjuster) {
		AtomicInteger releasedCount = new AtomicInteger();
		PoolBuilder<PoolableTest, ?> builder =
				PoolBuilder.from(Mono.fromCallable(PoolableTest::new))
				           .sizeBetween(1, 1)
				           .releaseHandler(poolableTest -> Mono.fromRunnable(() -> {
					           poolableTest.clean();
					           releasedCount.incrementAndGet();
				           }))
				           .evictionPredicate((poolable, metadata) -> !poolable.isHealthy());
		Pool<PoolableTest> pool = configAdjuster.apply(builder);
		pool.warmup().block();

		//acquire the only element and hold on to it
		PooledRef<PoolableTest> slot = pool.acquire().block();
		assertThat(slot).isNotNull();

		pool.withPoolable(Mono::just).subscribe().dispose();

		assertThat(releasedCount).as("before returning").hasValue(0);

		//release the element after the second acquire has been cancelled, so it should never "see" it
		slot.release().block();

		assertThat(releasedCount).as("after returning").hasValue(1);
	}


	@ParameterizedTestWithName
	@MethodSource("allPools")
	void allocatedReleasedIfBorrowerCancelled(PoolStyle configAdjuster) {
		Scheduler scheduler = Schedulers.newParallel("poolable test allocator");
		AtomicInteger newCount = new AtomicInteger();
		AtomicInteger releasedCount = new AtomicInteger();

		PoolBuilder<PoolableTest, ?> builder = PoolBuilder
				.from(Mono.defer(() -> Mono.delay(Duration.ofMillis(50)).thenReturn(new PoolableTest(newCount.incrementAndGet())))
				          .subscribeOn(scheduler))
				.sizeBetween(0, 1)
				.releaseHandler(poolableTest -> Mono.fromRunnable(() -> {
					poolableTest.clean();
					releasedCount.incrementAndGet();
				}))
				.evictionPredicate((poolable, metadata) -> !poolable.isHealthy());

		Pool<PoolableTest> pool = configAdjuster.apply(builder);

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


	@ParameterizedTestWithName
	@MethodSource("allPools")
	void allocatedReleasedIfBorrowerInScopeCancelled(PoolStyle configAdjuster) {
		Scheduler scheduler = Schedulers.newParallel("poolable test allocator");
		AtomicInteger newCount = new AtomicInteger();
		AtomicInteger releasedCount = new AtomicInteger();

		PoolBuilder<PoolableTest, ?> builder =
				PoolBuilder.from(Mono.defer(() -> Mono.delay(Duration.ofMillis(50)).thenReturn(new PoolableTest(newCount.incrementAndGet())))
				                     .subscribeOn(scheduler))
				           .sizeBetween(0, 1)
				           .releaseHandler(poolableTest -> Mono.fromRunnable(() -> {
					           poolableTest.clean();
					           releasedCount.incrementAndGet();
				           }))
				           .evictionPredicate((poolable, metadata) -> !poolable.isHealthy());
		Pool<PoolableTest> pool = configAdjuster.apply(builder);

		//acquire the only element and immediately dispose
		pool.withPoolable(Mono::just).subscribe().dispose();

		//release due to cancel is async, give it a bit of time
		await()
				.atMost(100, TimeUnit.MILLISECONDS)
				.with().pollInterval(10, TimeUnit.MILLISECONDS)
				.untilAsserted(
						() -> assertThat(releasedCount).as("released").hasValue(1));

		assertThat(newCount).as("created").hasValue(1);
	}

	@ParameterizedTestWithName
	@MethodSource("allPools")
	void pendingLimitSync(PoolStyle configAdjuster) {
		AtomicInteger allocatorCount = new AtomicInteger();
		Disposable.Composite composite = Disposables.composite();

		try {
			PoolBuilder<Integer, ?> builder = PoolBuilder.from(Mono.fromCallable(allocatorCount::incrementAndGet))
			                                             .sizeBetween(1, 1)
			                                             .maxPendingAcquire(1);
			AbstractPool<Integer> pool = configAdjuster.apply(builder);
			pool.warmup().block();
			PooledRef<Integer> hold = pool.acquire().block();

			AtomicReference<Throwable> error = new AtomicReference<>();
			AtomicInteger errorCount = new AtomicInteger();
			AtomicInteger otherTerminationCount = new AtomicInteger();

			for (int i = 0; i < 2; i++) {
				composite.add(
						pool.acquire()
						    .doFinally(fin -> {
							    if (SignalType.ON_ERROR == fin) errorCount.incrementAndGet();
							    else otherTerminationCount.incrementAndGet();
						    })
						    .doOnError(error::set)
						    .subscribe()
				);
			}

			assertThat(pool.pendingAcquireSize()).as("pending counter limited to 1").isEqualTo(1);

			assertThat(errorCount).as("immediate error of extraneous pending").hasValue(1);
			assertThat(otherTerminationCount).as("no other immediate termination").hasValue(0);
			assertThat(error.get()).as("extraneous pending error")
			                       .isInstanceOf(PoolAcquirePendingLimitException.class)
			                       .hasMessage("Pending acquire queue has reached its maximum size of 1");

			hold.release().block();

			assertThat(errorCount).as("error count stable after release").hasValue(1);
			assertThat(otherTerminationCount).as("pending succeeds after release").hasValue(1);
		}
		finally {
			composite.dispose();
		}
	}

	@ParameterizedTestWithName
	@MethodSource("allPools")
	void pendingLimitAsync(PoolStyle configAdjuster) {
		AtomicInteger allocatorCount = new AtomicInteger();
		final Disposable.Composite composite = Disposables.composite();

		try {
			PoolBuilder<Integer, ?> builder = PoolBuilder.from(Mono.fromCallable(allocatorCount::incrementAndGet))
			                                             .sizeBetween(1, 1)
			                                             .maxPendingAcquire(1);
			AbstractPool<Integer> pool = configAdjuster.apply(builder);
			pool.warmup().block();
			PooledRef<Integer> hold = pool.acquire().block();

			AtomicReference<Throwable> error = new AtomicReference<>();
			AtomicInteger errorCount = new AtomicInteger();
			AtomicInteger otherTerminationCount = new AtomicInteger();

			final Runnable runnable = () -> composite.add(
					pool.acquire()
					    .doFinally(fin -> {
					    	if (SignalType.ON_ERROR == fin) errorCount.incrementAndGet();
					    	else otherTerminationCount.incrementAndGet();
					    })
					    .doOnError(error::set)
					    .subscribe(v -> {}, e -> {})
			);
			RaceTestUtils.race(runnable, runnable);

			assertThat(pool.pendingAcquireSize()).as("pending counter limited to 1").isEqualTo(1);

			assertThat(errorCount).as("immediate error of extraneous pending").hasValue(1);
			assertThat(otherTerminationCount).as("no other immediate termination").hasValue(0);
			assertThat(error.get()).as("extraneous pending error")
			                       .isInstanceOf(PoolAcquirePendingLimitException.class)
			                       .hasMessage("Pending acquire queue has reached its maximum size of 1");

			hold.release().block();

			assertThat(errorCount).as("error count stable after release").hasValue(1);
			assertThat(otherTerminationCount).as("pending succeeds after release").hasValue(1);
		}
		finally {
			composite.dispose();
		}
	}

	@ParameterizedTestWithName
	@MethodSource("allPools")
	void cleanerFunctionError(PoolStyle configAdjuster) {
		PoolBuilder<PoolableTest, ?> builder = PoolBuilder
				.from(Mono.fromCallable(PoolableTest::new))
				.sizeBetween(0, 1)
				.releaseHandler(poolableTest -> Mono.fromRunnable(() -> {
					poolableTest.clean();
					throw new IllegalStateException("boom");
				}))
				.evictionPredicate((poolable, metadata) -> !poolable.isHealthy());

		Pool<PoolableTest> pool = configAdjuster.apply(builder);

		PooledRef<PoolableTest> slot = pool.acquire().block();

		assertThat(slot).isNotNull();

		StepVerifier.create(slot.release())
		            .verifyErrorMessage("boom");
	}

	@ParameterizedTestWithName
	@MethodSource("allPools")
	void cleanerFunctionErrorDiscards(PoolStyle configAdjuster) {
		PoolBuilder<PoolableTest, ?> builder = PoolBuilder
				.from(Mono.fromCallable(PoolableTest::new))
				.sizeBetween(0, 1)
				.releaseHandler(poolableTest -> Mono.fromRunnable(() -> {
					poolableTest.clean();
					throw new IllegalStateException("boom");
				}))
				.evictionPredicate((poolable, metadata) -> !poolable.isHealthy());

		Pool<PoolableTest> pool = configAdjuster.apply(builder);

		PooledRef<PoolableTest> slot = pool.acquire().block();

		assertThat(slot).isNotNull();

		StepVerifier.create(slot.release())
		            .verifyErrorMessage("boom");

		assertThat(slot.poolable().discarded).as("discarded despite cleaner error").isEqualTo(1);
	}


	@ParameterizedTestWithName
	@MethodSource("allPools")
	void disposingPoolDisposesElements(PoolStyle configAdjuster) {
		AtomicInteger cleanerCount = new AtomicInteger();

		PoolBuilder<PoolableTest, ?> builder = PoolBuilder
				.from(Mono.fromCallable(PoolableTest::new))
				.sizeBetween(0, 3)
				.releaseHandler(p -> Mono.fromRunnable(cleanerCount::incrementAndGet))
				.evictionPredicate((poolable, metadata) -> !poolable.isHealthy());

		AbstractPool<PoolableTest> pool = configAdjuster.apply(builder);

		PoolableTest pt1 = new PoolableTest(1);
		PoolableTest pt2 = new PoolableTest(2);
		PoolableTest pt3 = new PoolableTest(3);

		pool.elementOffer(pt1);
		pool.elementOffer(pt2);
		pool.elementOffer(pt3);
		//important: acquire the permits to simulate the proper creation of resources
		assertThat(pool.poolConfig.allocationStrategy().getPermits(3)).as("permits granted").isEqualTo(3);

		//this releases the permits acquired above
		pool.dispose();

		assertThat(pool.idleSize()).as("idleSize").isZero();
		assertThat(cleanerCount).as("recycled elements").hasValue(0);
		assertThat(pt1.isDisposed()).as("pt1 disposed").isTrue();
		assertThat(pt2.isDisposed()).as("pt2 disposed").isTrue();
		assertThat(pt3.isDisposed()).as("pt3 disposed").isTrue();
		assertThat(pool.poolConfig.allocationStrategy().estimatePermitCount()).as("permits available post dispose").isEqualTo(3);
	}

	@ParameterizedTestWithName
	@MethodSource("allPools")
	void disposingPoolFailsPendingBorrowers(PoolStyle configAdjuster) {
		AtomicInteger cleanerCount = new AtomicInteger();
		PoolBuilder<PoolableTest, ?> builder = PoolBuilder
				.from(Mono.fromCallable(PoolableTest::new))
				.sizeBetween(3, 3)
				.releaseHandler(p -> Mono.fromRunnable(cleanerCount::incrementAndGet))
				.evictionPredicate((poolable, metadata) -> !poolable.isHealthy());

		AbstractPool<PoolableTest> pool = configAdjuster.apply(builder);
		pool.warmup().block();

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

		assertThat(pool.idleSize()).as("idleSize").isZero();
		assertThat(cleanerCount).as("recycled elements").hasValue(0);
		assertThat(acquired1.isDisposed()).as("acquired1 held").isFalse();
		assertThat(acquired2.isDisposed()).as("acquired2 held").isFalse();
		assertThat(acquired3.isDisposed()).as("acquired3 held").isFalse();
		assertThat(borrowerError.get()).hasMessage("Pool has been shut down");
	}

	@ParameterizedTestWithName
	@MethodSource("allPools")
	void releasingToDisposedPoolDisposesElement(PoolStyle configAdjuster) {
		AtomicInteger cleanerCount = new AtomicInteger();
		PoolBuilder<PoolableTest, ?> builder = PoolBuilder
				.from(Mono.fromCallable(PoolableTest::new))
				.sizeBetween(3, 3)
				.releaseHandler(p -> Mono.fromRunnable(cleanerCount::incrementAndGet))
				.evictionPredicate((poolable, metadata) -> !poolable.isHealthy());
		AbstractPool<PoolableTest> pool = configAdjuster.apply(builder);
		pool.warmup().block();

		PooledRef<PoolableTest> slot1 = pool.acquire().block();
		PooledRef<PoolableTest> slot2 = pool.acquire().block();
		PooledRef<PoolableTest> slot3 = pool.acquire().block();

		assertThat(slot1).as("slot1").isNotNull();
		assertThat(slot2).as("slot2").isNotNull();
		assertThat(slot3).as("slot3").isNotNull();

		pool.dispose();

		assertThat(pool.idleSize()).as("idleSize").isZero();

		slot1.release().block();
		slot2.release().block();
		slot3.release().block();

		assertThat(cleanerCount).as("recycled elements").hasValue(0);
		assertThat(slot1.poolable().isDisposed()).as("acquired1 disposed").isTrue();
		assertThat(slot2.poolable().isDisposed()).as("acquired2 disposed").isTrue();
		assertThat(slot3.poolable().isDisposed()).as("acquired3 disposed").isTrue();
	}

	@ParameterizedTestWithName
	@MethodSource("allPools")
	void acquiringFromDisposedPoolFailsBorrower(PoolStyle configAdjuster) {
		AtomicInteger cleanerCount = new AtomicInteger();
		PoolBuilder<PoolableTest, ?> builder = PoolBuilder
				.from(Mono.fromCallable(PoolableTest::new))
				.sizeBetween(0, 3)
				.releaseHandler(p -> Mono.fromRunnable(cleanerCount::incrementAndGet))
				.evictionPredicate((poolable, metadata) -> !poolable.isHealthy());
		AbstractPool<PoolableTest> pool = configAdjuster.apply(builder);

		assertThat(pool.idleSize()).as("idleSize").isZero();

		pool.dispose();

		StepVerifier.create(pool.acquire())
		            .verifyErrorMessage("Pool has been shut down");

		assertThat(cleanerCount).as("recycled elements").hasValue(0);
	}

	@ParameterizedTestWithName
	@MethodSource("allPools")
	void poolIsDisposed(PoolStyle configAdjuster) {
		PoolBuilder<PoolableTest, ?> builder = PoolBuilder
				.from(Mono.fromCallable(PoolableTest::new))
				.sizeBetween(0, 3)
				.evictionPredicate((poolable, metadata) -> !poolable.isHealthy());
		AbstractPool<PoolableTest> pool = configAdjuster.apply(builder);

		assertThat(pool.isDisposed()).as("not yet disposed").isFalse();

		pool.dispose();

		assertThat(pool.isDisposed()).as("disposed").isTrue();
	}

	@ParameterizedTestWithName
	@MethodSource("allPools")
	void disposingPoolClosesCloseable(PoolStyle configAdjuster) {
		Formatter uniqueElement = new Formatter();

		PoolBuilder<Formatter, ?> builder = PoolBuilder
				.from(Mono.just(uniqueElement))
				.sizeBetween(1, 1)
				.evictionPredicate((poolable, metadata) -> true);
		AbstractPool<Formatter> pool = configAdjuster.apply(builder);
		pool.warmup().block();

		pool.dispose();

		assertThatExceptionOfType(FormatterClosedException.class)
				.isThrownBy(uniqueElement::flush);
	}

	@ParameterizedTestWithName
	@MethodSource("allPools")
	void disposeLaterIsLazy(PoolStyle configAdjuster) {
		Formatter uniqueElement = new Formatter();

		PoolBuilder<Formatter, ?> builder = PoolBuilder
				.from(Mono.just(uniqueElement))
				.sizeBetween(1, 1)
				.evictionPredicate((poolable, metadata) -> true);
		AbstractPool<Formatter> pool = configAdjuster.apply(builder);
		pool.warmup().block();

		Mono<Void> disposeMono = pool.disposeLater();
		Mono<Void> disposeMono2 = pool.disposeLater();

		assertThat(disposeMono)
				.isNotSameAs(disposeMono2);

		assertThatCode(uniqueElement::flush)
				.doesNotThrowAnyException();
	}

	@ParameterizedTestWithName
	@MethodSource("allPools")
	void disposeLaterCompletesWhenAllReleased(PoolStyle configAdjuster) {
		List<AtomicBoolean> elements = Arrays.asList(new AtomicBoolean(), new AtomicBoolean(), new AtomicBoolean());
		AtomicInteger index = new AtomicInteger(0);

		PoolBuilder<AtomicBoolean, ?> builder = PoolBuilder
				.from(Mono.fromCallable(() -> elements.get(index.getAndIncrement())))
				.sizeBetween(3, 3)
				.evictionPredicate((poolable, metadata) -> true)
				.destroyHandler(ab -> Mono.fromRunnable(() -> ab.set(true)));
		AbstractPool<AtomicBoolean> pool = configAdjuster.apply(builder);
		pool.warmup().block();

		Mono<Void> disposeMono = pool.disposeLater();

		assertThat(elements).as("before disposeLater subscription").noneMatch(AtomicBoolean::get);

		disposeMono.block();

		assertThat(elements).as("after disposeLater done").allMatch(AtomicBoolean::get);
	}

	@ParameterizedTestWithName
	@MethodSource("allPools")
	void disposeLaterReleasedConcurrently(PoolStyle configAdjuster) {
		AtomicInteger live = new AtomicInteger(0);

		PoolBuilder<Integer, ?> builder = PoolBuilder
				.from(Mono.fromCallable(live::getAndIncrement))
				.sizeBetween(3, 3)
				.evictionPredicate((poolable, metadata) -> true)
				.destroyHandler(ab -> Mono.delay(Duration.ofMillis(500))
				                          .doOnNext(v -> live.decrementAndGet())
				                          .then());
		AbstractPool<Integer> pool = configAdjuster.apply(builder);
		pool.warmup().block();

		Mono<Void> disposeMono = pool.disposeLater();

		assertThat(live).as("before disposeLater subscription").hasValue(3);

		disposeMono.subscribe();

		Awaitility.await().atMost(600, TimeUnit.MILLISECONDS) //serially would add up to 1500ms
		          .untilAtomic(live, CoreMatchers.is(0));
	}

	@ParameterizedTestWithName
	@MethodSource("allPools")
	void allocatorErrorInAcquireDrains_NoMinSize(PoolStyle configAdjuster) {
		AtomicInteger errorThrown = new AtomicInteger();
		PoolBuilder<String, ?> builder = PoolBuilder
				.from(Mono.delay(Duration.ofMillis(50))
						.flatMap(l -> {
					if (errorThrown.compareAndSet(0, 1)) {
						return Mono.<String>error(new IllegalStateException("boom"));
					}
					else {
						return Mono.just("success");
					}
				}))
				.sizeBetween(0, 1)
				.evictionPredicate((poolable, metadata) -> true);
		AbstractPool<String> pool = configAdjuster.apply(builder);

		StepVerifier.create(Flux.just(pool.acquire().onErrorResume(e -> Mono.empty()), pool.acquire())
								.flatMap(Function.identity()))
					.expectNextMatches(pooledRef -> "success".equals(pooledRef.poolable()))
					.expectComplete()
					.verify(Duration.ofSeconds(5));
	}

	@ParameterizedTestWithName
	@MethodSource("allPools")
	@Tag("loops")
	void loopAllocatorErrorInAcquireDrains_NoMinSize(PoolStyle configAdjuster) {
		TestLogger testLogger = new TestLogger();
		Loggers.useCustomLoggers(s -> testLogger);
		try {
			for (int i = 0; i < 100; i++) {
				allocatorErrorInAcquireDrains_NoMinSize(configAdjuster);
			}
			String log = testLogger.getOutContent();
			if (!log.isEmpty()) {
				System.out.println("Log in loop test, removed duplicated lines:");
				Stream.of(log.split("\n"))
				      .distinct()
				      .forEach(System.out::println);
			}
		}
		finally {
			Loggers.resetLoggerFactory();
		}
	}

	@ParameterizedTestWithName
	@MethodSource("allPools")
	void allocatorErrorInAcquireDrains_WithMinSize(PoolStyle configAdjuster) {
		PoolBuilder<String, ?> builder = PoolBuilder
				.from(Mono.delay(Duration.ofMillis(50))
						.flatMap(l -> Mono.<String>error(new IllegalStateException("boom"))))
				.sizeBetween(3, 3)
				.evictionPredicate((poolable, metadata) -> true);
		AbstractPool<String> pool = configAdjuster.apply(builder);

		StepVerifier.create(
				Flux.just(pool.acquire().onErrorResume(e -> Mono.empty()),
						pool.acquire().onErrorResume(e -> Mono.empty()),
						pool.acquire())
				    .flatMap(Function.identity()))
		            .expectErrorSatisfies(e -> assertThat(e).isInstanceOf(IllegalStateException.class)
		                                                    .hasMessage("boom"))
		            .verify(Duration.ofSeconds(5));
	}

	@ParameterizedTestWithName
	@MethodSource("allPools")
	@Tag("loops")
	void loopAllocatorErrorInAcquireDrains_WithMinSize(PoolStyle configAdjuster) {
		TestLogger testLogger = new TestLogger();
		Loggers.useCustomLoggers(s -> testLogger);
		try {
			for (int i = 0; i < 100; i++) {
				allocatorErrorInAcquireDrains_WithMinSize(configAdjuster);
			}
			String log = testLogger.getOutContent();
			if (!log.isEmpty()) {
				System.out.println("Log in loop test, removed duplicated lines:");
				Stream.of(log.split("\n"))
				      .distinct()
				      .forEach(System.out::println);
			}
		}
		finally {
			Loggers.resetLoggerFactory();
		}
	}

	@ParameterizedTestWithName
	@MethodSource("allPools")
	void allocatorErrorInAcquireIsPropagated(PoolStyle configAdjuster) {
		PoolBuilder<String, ?> builder = PoolBuilder
				.from(Mono.<String>error(new IllegalStateException("boom")))
				.sizeBetween(0, 1)
				.evictionPredicate((poolable, metadata) -> true);
		AbstractPool<String> pool = configAdjuster.apply(builder);

		StepVerifier.create(pool.acquire())
		            .verifyErrorSatisfies(e -> assertThat(e).isInstanceOf(IllegalStateException.class)
		                                                    .hasMessage("boom"));
	}

	@ParameterizedTestWithName
	@MethodSource("allPools")
	void allocatorErrorInWarmupIsPropagated(PoolStyle configAdjuster) {
		final PoolBuilder<Object, ?> builder = PoolBuilder
				.from(Mono.error(new IllegalStateException("boom")))
				.sizeBetween(1, 1)
				.evictionPredicate((poolable, metadata) -> true);
		AbstractPool<Object> pool = configAdjuster.apply(builder);

		StepVerifier.create(pool.warmup())
		            .verifyErrorSatisfies(e -> assertThat(e).isInstanceOf(IllegalStateException.class)
		                                                    .hasMessage("boom"));
	}

	@ParameterizedTestWithName
	@MethodSource("allPools")
	void discardCloseableWhenCloseFailureLogs(PoolStyle configAdjuster) {
		TestLogger testLogger = new TestLogger();
		Loggers.useCustomLoggers(it -> testLogger);
		try {
			Closeable closeable = () -> {
				throw new IOException("boom");
			};

			PoolBuilder<Closeable, ?> builder = PoolBuilder
					.from(Mono.just(closeable))
					.sizeBetween(1, 1)
					.evictionPredicate((poolable, metadata) -> true);
			AbstractPool<Closeable> pool = configAdjuster.apply(builder);
			pool.warmup().block();

			pool.dispose();

			assertThat(testLogger.getOutContent())
					.contains("Failure while discarding a released Poolable that is Closeable, could not close - java.io.IOException: boom");
		}
		finally {
			Loggers.resetLoggerFactory();
		}
	}

	@ParameterizedTestWithName
	@MethodSource("allPools")
	void pendingTimeoutNotImpactedByLongAllocation(PoolStyle configAdjuster) {
		VirtualTimeScheduler vts1 = VirtualTimeScheduler.getOrSet();

		try {
			PoolBuilder<String, ?> builder = PoolBuilder
					.from(Mono.just("delayed").delaySubscription(Duration.ofMillis(500)))
					.sizeBetween(0, 1);
			AbstractPool<String> pool = configAdjuster.apply(builder);

			StepVerifier.withVirtualTime(
					() -> pool.acquire(Duration.ofMillis(100)),
					() -> vts1,
					1)
			            .expectSubscription()
			            .expectNoEvent(Duration.ofMillis(500))
			            .assertNext(n -> {
			            	Assertions.assertThat(n.poolable()).isEqualTo("delayed");
			            	n.release()
				             .subscribe();
			            })
			            .verifyComplete();


			VirtualTimeScheduler vts2 = VirtualTimeScheduler.getOrSet();

			StepVerifier.withVirtualTime(
					() -> pool.acquire(Duration.ofMillis(100)).map(PooledRef::poolable),
					() -> vts2,
					1)
			            .expectSubscription()
			            .expectNext("delayed")
			            .verifyComplete();
		}
		finally {
			VirtualTimeScheduler.reset();
		}
	}

	@ParameterizedTestWithName
	@MethodSource("allPools")
	void pendingTimeoutImpactedByLongRelease(PoolStyle configAdjuster) {
		PoolBuilder<String, ?> builder = PoolBuilder
				.from(Mono.just("instant"))
				.sizeBetween(0, 1);
		AbstractPool<String> pool = configAdjuster.apply(builder);

		PooledRef<String> uniqueRef = pool.acquire().block();

		StepVerifier.withVirtualTime(() -> pool.acquire(Duration.ofMillis(100)).map(PooledRef::poolable))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(100))
		            .thenAwait(Duration.ofMillis(1))
		            .verifyErrorSatisfies(e -> assertThat(e)
				            .isInstanceOf(TimeoutException.class)
				            .isExactlyInstanceOf(PoolAcquireTimeoutException.class)
				            .hasMessage("Pool#acquire(Duration) has been pending for more than the configured timeout of 100ms"));
	}

	@ParameterizedTestWithName
	@MethodSource("allPools")
	void pendingTimeoutDoesntCauseExtraReleasePostTimeout(PoolStyle configAdjuster) {
		AtomicInteger resource = new AtomicInteger();
		PoolBuilder<AtomicInteger, ?> builder = PoolBuilder
				.from(Mono.just(resource))
				.releaseHandler(atomic -> Mono.fromRunnable(atomic::incrementAndGet))
				.sizeBetween(0, 1);
		AbstractPool<AtomicInteger> pool = configAdjuster.apply(builder);

		PooledRef<AtomicInteger> uniqueRef = pool.acquire().block();
		assert uniqueRef != null;

		StepVerifier.withVirtualTime(() -> pool.acquire(Duration.ofMillis(100)).map(PooledRef::poolable))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(100))
		            .thenAwait(Duration.ofMillis(1))
		            .verifyErrorSatisfies(e -> assertThat(e)
				            .isInstanceOf(TimeoutException.class)
				            .isExactlyInstanceOf(PoolAcquireTimeoutException.class)
				            .hasMessage("Pool#acquire(Duration) has been pending for more than the configured timeout of 100ms"));

		assertThat(resource).as("post timeout but before resource available").hasValue(0);

		uniqueRef.release().block();
		assertThat(resource).as("post timeout and after resource available").hasValue(1);
	}

	@ParameterizedTestWithName
	@MethodSource("allPools")
	void pendingTimeoutWithCustomAcquireTimer(PoolStyle configAdjuster) {
		AtomicInteger resource = new AtomicInteger();
		AtomicBoolean customTimeout = new AtomicBoolean();
		PoolBuilder<AtomicInteger, ?> builder = PoolBuilder
				.from(Mono.just(resource))
				.releaseHandler(atomic -> Mono.fromRunnable(atomic::incrementAndGet))
				.sizeBetween(0, 1)
				.pendingAcquireTimer((r, d) -> {
					customTimeout.set(true);
					return Schedulers.parallel().schedule(r, d.toMillis(), TimeUnit.MILLISECONDS);
				});
		AbstractPool<AtomicInteger> pool = configAdjuster.apply(builder);

		PooledRef<AtomicInteger> uniqueRef = pool.acquire().block();
		assert uniqueRef != null;

		StepVerifier.withVirtualTime(() -> pool.acquire(Duration.ofMillis(100)).map(PooledRef::poolable))
				.expectSubscription()
				.expectNoEvent(Duration.ofMillis(100))
				.thenAwait(Duration.ofMillis(1))
				.verifyErrorSatisfies(e -> assertThat(e)
						.isInstanceOf(TimeoutException.class)
						.isExactlyInstanceOf(PoolAcquireTimeoutException.class)
						.hasMessage("Pool#acquire(Duration) has been pending for more than the configured timeout of 100ms"));

		assertThat(customTimeout).as("custom pendingAcquireTimer invoked").isTrue();

		assertThat(resource).as("post timeout but before resource available").hasValue(0);

		uniqueRef.release().block();
		assertThat(resource).as("post timeout and after resource available").hasValue(1);
	}

	@ParameterizedTestWithName
	@MethodSource("allPools")
	void eachBorrowerCanOnlyReleaseOnce(PoolStyle configAdjuster) {
		AtomicInteger resource = new AtomicInteger();
		PoolBuilder<AtomicInteger, ?> builder = PoolBuilder
				.from(Mono.just(resource))
				.releaseHandler(atomic -> Mono.fromRunnable(atomic::incrementAndGet))
				.sizeBetween(0, 1);
		AbstractPool<AtomicInteger> pool = configAdjuster.apply(builder);

		PooledRef<AtomicInteger> acquire1 = pool.acquire().block();

		Mono<Void> releaserMono1 = acquire1.release();
		releaserMono1.block();
		releaserMono1.block();
		Mono<Void> releaserMono2 = acquire1.release();
		releaserMono2.block();
		releaserMono2.block();

		assertThat(resource).as("first acquire multi-release").hasValue(1);

		Mono<Void> releaserMono3 = pool.acquire().block().release();
		releaserMono3.block();
		releaserMono3.block();

		assertThat(resource).as("second acquire multi-release").hasValue(2);
	}

	@ParameterizedTestWithName
	@MethodSource("allPools")
	void eachBorrowerCanOnlyInvalidateOnce(PoolStyle configAdjuster) {
		AtomicInteger resource = new AtomicInteger();
		PoolBuilder<AtomicInteger, ?> builder = PoolBuilder
				.from(Mono.just(resource))
				.destroyHandler(atomic -> Mono.fromRunnable(atomic::incrementAndGet))
				.sizeBetween(0, 1);
		AbstractPool<AtomicInteger> pool = configAdjuster.apply(builder);

		PooledRef<AtomicInteger> acquire1 = pool.acquire().block();

		Mono<Void> invalidateMono1 = acquire1.invalidate();
		invalidateMono1.block();
		invalidateMono1.block();
		final Mono<Void> invalidateMono2 = acquire1.invalidate();
		invalidateMono2.block();
		invalidateMono2.block();

		assertThat(resource).as("first acquire multi-invalidate").hasValue(1);

		final Mono<Void> invalidateMono3 = pool.acquire().block().invalidate();
		invalidateMono3.block();
		invalidateMono3.block();

		assertThat(resource).as("second acquire multi-invalidate").hasValue(2);
	}

	// === METRICS ===

	protected TestUtils.InMemoryPoolMetrics recorder;

	@BeforeEach
	void initRecorder() {
		this.recorder = new TestUtils.InMemoryPoolMetrics(new TestUtils.NanoTimeClock());
	}

	@ParameterizedTestWithName
	@MethodSource("allPools")
	@Tag("metrics")
	void recordsAllocationCountInWarmup(PoolStyle configAdjuster) {
		AtomicBoolean flip = new AtomicBoolean();
		//note the starter method here is irrelevant, only the config is created and passed to createPool
		PoolBuilder<String, ?> builder = PoolBuilder
				.from(Mono.defer(() -> {
					if (flip.compareAndSet(false, true)) {
						return Mono.just("foo");
					}
					else {
						flip.compareAndSet(true, false);
						return Mono.error(new IllegalStateException("boom"));
					}
				}))
				.sizeBetween(10, Integer.MAX_VALUE)
				.metricsRecorder(recorder)
				.clock(recorder.getClock());
		AbstractPool<String> pool = configAdjuster.apply(builder);

		assertThatIllegalStateException()
				.isThrownBy(() -> pool.warmup().block());

		assertThat(recorder.getAllocationTotalCount()).isEqualTo(2);
		assertThat(recorder.getAllocationSuccessCount())
				.isOne()
				.isEqualTo(recorder.getAllocationSuccessHistogram().getTotalCount());
		assertThat(recorder.getAllocationErrorCount())
				.isOne()
				.isEqualTo(recorder.getAllocationErrorHistogram().getTotalCount());
	}

	@ParameterizedTestWithName
	@MethodSource("allPools")
	@Tag("metrics")
	void recordsAllocationCountInBorrow(PoolStyle configAdjuster) {
		AtomicBoolean flip = new AtomicBoolean();
		//note the starter method here is irrelevant, only the config is created and passed to createPool
		PoolBuilder<String, ?> builder = PoolBuilder
				.from(Mono.defer(() -> {
					if (flip.compareAndSet(false, true)) {
						return Mono.just("foo");
					}
					else {
						flip.compareAndSet(true, false);
						return Mono.error(new IllegalStateException("boom"));
					}
				}))
				.metricsRecorder(recorder)
				.clock(recorder.getClock());
		Pool<String> pool = configAdjuster.apply(builder);

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
	}

	@ParameterizedTestWithName
	@MethodSource("allPools")
	@Tag("metrics")
	void recordsAllocationLatenciesInWarmup(PoolStyle configAdjuster) {
		AtomicBoolean flip = new AtomicBoolean();
		//note the starter method here is irrelevant, only the config is created and passed to createPool
		PoolBuilder<String, ?> builder = PoolBuilder
				.from(Mono.defer(() -> {
					if (flip.compareAndSet(false, true)) {
						return Mono.just("foo").delayElement(Duration.ofMillis(100));
					}
					else {
						flip.compareAndSet(true, false);
						return Mono.delay(Duration.ofMillis(200)).then(Mono.error(new IllegalStateException("boom")));
					}
				}))
				.sizeBetween(10, Integer.MAX_VALUE)
				.metricsRecorder(recorder)
				.clock(recorder.getClock());
		AbstractPool<String> pool = configAdjuster.apply(builder);

		// warmup will eagerly subscribe 10 times to the allocator.
		// The five first subscribtions will success (after around 100 millis), and some allocation should fail after around
		// 200 millis.
		assertThatIllegalStateException()
				.isThrownBy(() -> pool.warmup().block());

		// at least 5 allocation should be successful
		assertThat(recorder.getAllocationSuccessCount()).isEqualTo(5);
		// at least 1 allocation should have failed
		assertThat(recorder.getAllocationErrorCount()).isGreaterThanOrEqualTo(1);
		// at least 6 allocations should have taken place
		assertThat(recorder.getAllocationTotalCount()).isGreaterThanOrEqualTo(6);

		long minSuccess = recorder.getAllocationSuccessHistogram().getMinValue();
		long minError = recorder.getAllocationErrorHistogram().getMinValue();

		assertThat(minSuccess).as("allocation success latency").isGreaterThanOrEqualTo(100L);
		assertThat(minError).as("allocation error latency").isGreaterThanOrEqualTo(200L);
	}

	@ParameterizedTestWithName
	@MethodSource("allPools")
	@Tag("metrics")
	void recordsAllocationLatenciesInBorrow(PoolStyle configAdjuster) {
		AtomicBoolean flip = new AtomicBoolean();
		//note the starter method here is irrelevant, only the config is created and passed to createPool
		PoolBuilder<String, ?> builder = PoolBuilder
				.from(Mono.defer(() -> {
					if (flip.compareAndSet(false, true)) {
						return Mono.just("foo").delayElement(Duration.ofMillis(100));
					}
					else {
						flip.compareAndSet(true, false);
						return Mono.delay(Duration.ofMillis(200)).then(Mono.error(new IllegalStateException("boom")));
					}
				}))
				.metricsRecorder(recorder)
				.clock(recorder.getClock());
		Pool<String> pool = configAdjuster.apply(builder);

		pool.acquire().block(); //success
		pool.acquire().map(PooledRef::poolable)
		    .onErrorReturn("error").block(); //error

		long minSuccess = recorder.getAllocationSuccessHistogram().getMinValue();
		assertThat(minSuccess)
				.as("allocation success latency")
				.isGreaterThanOrEqualTo(100L);

		long minError = recorder.getAllocationErrorHistogram().getMinValue();
		assertThat(minError)
				.as("allocation error latency")
				.isGreaterThanOrEqualTo(200L);
	}

	@ParameterizedTestWithName
	@MethodSource("allPools")
	@Tag("metrics")
	void recordsResetLatencies(PoolStyle configAdjuster) {
		AtomicBoolean flip = new AtomicBoolean();
		//note the starter method here is irrelevant, only the config is created and passed to createPool
		PoolBuilder<String, ?> builder = PoolBuilder
				.from(Mono.just("foo"))
				.releaseHandler(s -> {
					if (flip.compareAndSet(false,
							true)) {
						return Mono.delay(Duration.ofMillis(
								100))
						           .then();
					}
					else {
						flip.compareAndSet(true,
								false);
						return Mono.empty();
					}
				})
				.metricsRecorder(recorder)
				.clock(recorder.getClock());
		Pool<String> pool = configAdjuster.apply(builder);

		pool.acquire().flatMap(PooledRef::release).block();
		pool.acquire().flatMap(PooledRef::release).block();

		assertThat(recorder.getResetCount()).as("reset").isEqualTo(2);

		long min = recorder.getResetHistogram().getMinValue();
		assertThat(min).isCloseTo(0L, Offset.offset(50L));

		long max = recorder.getResetHistogram().getMaxValue();
		assertThat(max).isCloseTo(100L, Offset.offset(50L));
	}

	@ParameterizedTestWithName
	@MethodSource("allPools")
	@Tag("metrics")
	void recordsDestroyLatencies(PoolStyle configAdjuster) {
		AtomicBoolean flip = new AtomicBoolean();
		//note the starter method here is irrelevant, only the config is created and passed to createPool
		PoolBuilder<String, ?> builder = PoolBuilder
				.from(Mono.just("foo"))
				.evictionPredicate((poolable, metadata) -> true)
				.destroyHandler(s -> {
					if (flip.compareAndSet(false,
							true)) {
						return Mono.delay(Duration.ofMillis(500))
						           .then();
					}
					else {
						flip.compareAndSet(true,
								false);
						return Mono.empty();
					}
				})
				.metricsRecorder(recorder)
				.clock(recorder.getClock());
		Pool<String> pool = configAdjuster.apply(builder);

		pool.acquire().flatMap(PooledRef::release).block();
		pool.acquire().flatMap(PooledRef::release).block();

		await().pollDelay(500, TimeUnit.MILLISECONDS)
		       .atMost(1, TimeUnit.SECONDS)
		       .untilAsserted(() -> assertThat(recorder.getDestroyCount()).as("destroy after 500ms").isEqualTo(2));

		long min = recorder.getDestroyHistogram().getMinValue();
		assertThat(min).isCloseTo(0L, Offset.offset(50L));

		long max = recorder.getDestroyHistogram().getMaxValue();
		assertThat(max).isCloseTo(500L, Offset.offset(50L));
	}

	@ParameterizedTestWithName
	@MethodSource("allPools")
	@Tag("metrics")
	void recordsResetVsRecycle(PoolStyle configAdjuster) {
		AtomicReference<String> content = new AtomicReference<>("foo");
		//note the starter method here is irrelevant, only the config is created and passed to createPool
		PoolBuilder<String, ?> builder = PoolBuilder
				.from(Mono.fromCallable(() -> content.getAndSet("bar")))
				.evictionPredicate((poolable, metadata) -> "foo".equals(poolable))
				.metricsRecorder(recorder)
				.clock(recorder.getClock());
		Pool<String> pool = configAdjuster.apply(builder);

		pool.acquire().flatMap(PooledRef::release).block();
		pool.acquire().flatMap(PooledRef::release).block();

		assertThat(recorder.getResetCount()).as("reset").isEqualTo(2);
		assertThat(recorder.getDestroyCount()).as("destroy").isEqualTo(1);
		assertThat(recorder.getRecycledCount()).as("recycle").isEqualTo(1);
	}

	@ParameterizedTestWithName
	@MethodSource("allPools")
	@Tag("metrics")
	void recordsLifetime(PoolStyle configAdjuster) throws InterruptedException {
		AtomicInteger allocCounter = new AtomicInteger();
		AtomicInteger destroyCounter = new AtomicInteger();
		//note the starter method here is irrelevant, only the config is created and passed to createPool
		PoolBuilder<Integer, ?> builder = PoolBuilder
				.from(Mono.fromCallable(allocCounter::incrementAndGet))
				.sizeBetween(0, 2)
				.evictionPredicate((poolable, metadata) -> metadata.acquireCount() >= 2)
				.destroyHandler(i -> Mono.fromRunnable(destroyCounter::incrementAndGet))
				.metricsRecorder(recorder)
				.clock(recorder.getClock());
		Pool<Integer> pool = configAdjuster.apply(builder);

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

	@ParameterizedTestWithName
	@MethodSource("allPools")
	@Tag("metrics")
	void recordsIdleTimeFromConstructor(PoolStyle configAdjuster) throws InterruptedException {
		AtomicInteger allocCounter = new AtomicInteger();
		//note the starter method here is irrelevant, only the config is created and passed to createPool
		PoolBuilder<Integer, ?> builder = PoolBuilder
				.from(Mono.fromCallable(allocCounter::incrementAndGet))
				.sizeBetween(2, 2)
				.metricsRecorder(recorder)
				.clock(recorder.getClock());
		Pool<Integer> pool = configAdjuster.apply(builder);
		pool.warmup().block();


		//wait 125ms and 250ms before first acquire respectively
		Thread.sleep(125);
		pool.acquire().block();
		Thread.sleep(125);
		pool.acquire().block();

		assertThat(allocCounter).as("allocations").hasValue(2);

		assertThat(recorder.getIdleTimeHistogram().getMinNonZeroValue())
				.as("min idle time")
				.isCloseTo(125L, Offset.offset(25L));
		assertThat(recorder.getIdleTimeHistogram().getMaxValue())
				.as("max idle time")
				.isCloseTo(250L, Offset.offset(25L));
	}

	@ParameterizedTestWithName
	@MethodSource("allPools")
	@Tag("metrics")
	void recordsIdleTimeBetweenAcquires(PoolStyle configAdjuster) throws InterruptedException {
		AtomicInteger allocCounter = new AtomicInteger();
		//note the starter method here is irrelevant, only the config is created and passed to createPool
		PoolBuilder<Integer, ?> builder = PoolBuilder
				.from(Mono.fromCallable(allocCounter::incrementAndGet))
				.sizeBetween(2, 2)
				.metricsRecorder(recorder)
				.clock(recorder.getClock());
		Pool<Integer> pool = configAdjuster.apply(builder);
		pool.warmup().block();

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

		assertThat(recorder.getIdleTimeHistogram().getMinNonZeroValue())
				.as("min idle time")
				.isCloseTo(125L, Offset.offset(20L));

		assertThat(recorder.getIdleTimeHistogram().getMaxValue())
				.as("max idle time")
				.isCloseTo(300L, Offset.offset(40L));
	}

	@ParameterizedTestWithName
	@MethodSource("allPools")
	@Tag("metrics")
	void acquireTimeout(PoolStyle configAdjuster) {
		AtomicInteger allocCounter = new AtomicInteger();
		AtomicInteger didReset = new AtomicInteger();
		//note the starter method here is irrelevant, only the config is created and passed to createPool
		PoolBuilder<Integer, ?> builder = PoolBuilder
				.from(Mono.fromCallable(allocCounter::incrementAndGet))
				.releaseHandler(i -> Mono.fromRunnable(didReset::incrementAndGet))
				.sizeBetween(0, 1);
		Pool<Integer> pool = configAdjuster.apply(builder);

		PooledRef<Integer> uniqueElement = Objects.requireNonNull(pool.acquire().block());

		assertThat(uniqueElement.metadata().acquireCount()).isOne();

		assertThatExceptionOfType(RuntimeException.class)
				.isThrownBy(() -> pool.acquire().timeout(Duration.ofMillis(50)).block())
				.withCauseExactlyInstanceOf(TimeoutException.class);

		assertThat(didReset).hasValue(0);

		uniqueElement.release().block();
		assertThat(uniqueElement.metadata().acquireCount()).as("acquireCount post timeout-then-release").isOne();

		assertThat(pool.acquire().block()).isNotNull();
		assertThat(allocCounter).as("final allocation count").hasValue(1);
		assertThat(didReset).hasValue(1);
	}

	@ParameterizedTestWithName
	@MethodSource("allPools")
	@Tag("metrics")
	void instrumentedPoolsMetricsAreSelfViews(PoolStyle configAdjuster) {
		PoolBuilder<Integer, ?> builder = PoolBuilder.from(Mono.just(1));
		Pool<Integer> pool = configAdjuster.apply(builder);

		assertThat(pool).isInstanceOf(InstrumentedPool.class);

		PoolMetrics metrics = ((InstrumentedPool) pool).metrics();

		assertThat(pool).isSameAs(metrics);
	}

	@ParameterizedTestWithName
	@MethodSource("allPools")
	@Tag("metrics")
	void instrumentAllocatedIdleAcquired(PoolStyle configAdjuster) {
		PoolBuilder<Integer, ?> builder = PoolBuilder.from(Mono.just(1))
				.sizeBetween(0, 1);
		InstrumentedPool<Integer> pool = configAdjuster.apply(builder);
		PoolMetrics poolMetrics = pool.metrics();

		assertThat(poolMetrics.allocatedSize()).as("allocated at start").isZero();

		PooledRef<Integer> ref = pool.acquire().block();

		assertThat(poolMetrics.allocatedSize()).as("allocated at first acquire").isOne();
		assertThat(poolMetrics.idleSize()).as("idle at first acquire").isZero();
		assertThat(poolMetrics.acquiredSize()).as("acquired size at first acquire").isOne();

		ref.release().block();

		assertThat(poolMetrics.allocatedSize()).as("allocated after release").isOne();
		assertThat(poolMetrics.idleSize()).as("idle after release").isOne();
		assertThat(poolMetrics.acquiredSize()).as("acquired after release").isZero();
	}

	@ParameterizedTestWithName
	@MethodSource("allPools")
	@Tag("metrics")
	void instrumentAllocatedIdleAcquired_1(PoolStyle configAdjuster)
			throws Exception {
		PoolBuilder<Integer, ?> builder = PoolBuilder.from(Mono.just(1))
				.sizeBetween(0, 1);
		InstrumentedPool<Integer> pool = configAdjuster.apply(builder);
		PoolMetrics poolMetrics = pool.metrics();

		AtomicInteger allocated = new AtomicInteger(poolMetrics.allocatedSize());
		AtomicInteger idle = new AtomicInteger(poolMetrics.idleSize());
		AtomicInteger acquired = new AtomicInteger(poolMetrics.acquiredSize());
		AtomicReference<PooledRef<Integer>> ref = new AtomicReference<>();

		assertThat(allocated.get()).as("allocated at start").isZero();

		CountDownLatch latch1 = new CountDownLatch(1);
		pool.acquire()
				.subscribe(pooledRef -> {
					allocated.set(poolMetrics.allocatedSize());
					idle.set(poolMetrics.idleSize());
					acquired.set(poolMetrics.acquiredSize());
					ref.set(pooledRef);
					latch1.countDown();
				});

		assertThat(latch1.await(30, TimeUnit.SECONDS)).isTrue();
		assertThat(allocated.get()).as("allocated at first acquire").isOne();
		assertThat(idle.get()).as("idle at first acquire").isZero();
		assertThat(acquired.get()).as("acquired size at first acquire").isOne();

		CountDownLatch latch2 = new CountDownLatch(1);
		ref.get()
				.release()
				.subscribe(null,
						null,
						() -> {
							allocated.set(poolMetrics.allocatedSize());
							idle.set(poolMetrics.idleSize());
							acquired.set(poolMetrics.acquiredSize());
							latch2.countDown();
						});

		assertThat(latch2.await(30, TimeUnit.SECONDS)).isTrue();
		assertThat(allocated.get()).as("allocated after release").isOne();
		assertThat(idle.get()).as("idle after release").isOne();
		assertThat(acquired.get()).as("acquired after release").isZero();
	}

	@ParameterizedTestWithName
	@MethodSource("allPools")
	@Tag("metrics")
	void instrumentPendingAcquire(PoolStyle configAdjuster) {
		PoolBuilder<Integer, ?> builder = PoolBuilder.from(Mono.just(1))
				.sizeBetween(0, 1);
		InstrumentedPool<Integer> pool = configAdjuster.apply(builder);
		PoolMetrics poolMetrics = pool.metrics();

		PooledRef<Integer> ref = pool.acquire().block();

		assertThat(poolMetrics.pendingAcquireSize()).as("first acquire not pending").isZero();


		pool.acquire().subscribe();

		assertThat(poolMetrics.pendingAcquireSize()).as("second acquire put in pending").isOne();

		ref.release().block();

		assertThat(poolMetrics.pendingAcquireSize()).as("second acquire not pending after release").isZero();
	}

	@ParameterizedTestWithName
	@MethodSource("allPools")
	@Tag("metrics")
	void getConfigMaxPendingAcquire(PoolStyle configAdjuster) {
		PoolBuilder<Integer, ?> builder = PoolBuilder.from(Mono.just(1))
		                                          .maxPendingAcquire(12);
		InstrumentedPool<Integer> pool = configAdjuster.apply(builder);
		PoolMetrics poolMetrics = pool.metrics();

		assertThat(poolMetrics.getMaxPendingAcquireSize()).isEqualTo(12);
	}

	@ParameterizedTestWithName
	@MethodSource("allPools")
	@Tag("metrics")
	void getConfigMaxPendingAcquireUnbounded(PoolStyle configAdjuster) {
		PoolBuilder<Integer, ?> builder = PoolBuilder.from(Mono.just(1))
		                                          .maxPendingAcquireUnbounded();
		InstrumentedPool<Integer> pool = configAdjuster.apply(builder);
		PoolMetrics poolMetrics = pool.metrics();

		assertThat(poolMetrics.getMaxPendingAcquireSize()).isEqualTo(Integer.MAX_VALUE);
	}

	@ParameterizedTestWithName
	@MethodSource("allPools")
	@Tag("metrics")
	void getConfigMaxSize(PoolStyle configAdjuster) {
		PoolBuilder<Integer, ?> builder = PoolBuilder.from(Mono.just(1))
		                                          .sizeBetween(0, 22);
		InstrumentedPool<Integer> pool = configAdjuster.apply(builder);
		PoolMetrics poolMetrics = pool.metrics();

		assertThat(poolMetrics.getMaxAllocatedSize()).isEqualTo(22);
	}

	@ParameterizedTestWithName
	@MethodSource("allPools")
	@Tag("metrics")
	void getConfigMaxSizeUnbounded(PoolStyle configAdjuster) {
		PoolBuilder<Integer, ?> builder = PoolBuilder.from(Mono.just(1))
		                                          .sizeUnbounded();
		InstrumentedPool<Integer> pool = configAdjuster.apply(builder);
		PoolMetrics poolMetrics = pool.metrics();

		assertThat(poolMetrics.getMaxAllocatedSize()).isEqualTo(Integer.MAX_VALUE);
	}

	@ParameterizedTestWithName
	@MethodSource("allPools")
	void invalidateRaceIdleState(PoolStyle configAdjuster)
			throws Exception {
		AtomicInteger destroyCounter = new AtomicInteger();
		PoolBuilder<Integer, ?> builder = PoolBuilder.from(Mono.just(1))
		                                             .evictionPredicate((obj, meta) -> true)
		                                             .destroyHandler(i -> Mono.fromRunnable(destroyCounter::incrementAndGet))
		                                             .sizeBetween(0, 1)
		                                             .maxPendingAcquire(1);
		InstrumentedPool<Integer> pool = configAdjuster.apply(builder);
		int loops = 4000;

		final CountDownLatch latch = new CountDownLatch(loops * 2);

		for (int i = 0; i < loops; i++) {
			final PooledRef<Integer> pooledRef = pool.acquire().block();
			RaceTestUtils.race(() -> pooledRef.release().subscribe(v -> {}, e -> latch.countDown(),
					latch::countDown),
					() -> pooledRef.invalidate().subscribe(v -> {}, e -> latch.countDown(),
							latch::countDown));
		}

		assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();

		assertThat(destroyCounter).hasValue(4000);
	}

	@ParameterizedTestWithName
	@MethodSource("lruPools")
	void releaseAllOnAcquire(PoolStyle configAdjuster) {
		AtomicInteger intSource = new AtomicInteger();
		AtomicInteger releasedIndex = new AtomicInteger();
		ConcurrentLinkedQueue<Integer> destroyed = new ConcurrentLinkedQueue<>();

		PoolBuilder<Integer, PoolConfig<Integer>> builder =
				PoolBuilder.from(Mono.fromCallable(intSource::incrementAndGet))
				           .evictionPredicate((obj, meta) -> obj <= releasedIndex.get())
				           .destroyHandler(i -> Mono.fromRunnable(() -> destroyed.offer(i)))
				           .sizeBetween(0, 5)
				           .maxPendingAcquire(100);

		InstrumentedPool<Integer> pool = configAdjuster.apply(builder);

		//acquire THEN release four refs
		List<PooledRef<Integer>> fourRefs = Arrays.asList(
				pool.acquire().block(),
				pool.acquire().block(),
				pool.acquire().block(),
				pool.acquire().block()
		);
		for (PooledRef<Integer> ref : fourRefs) {
			ref.release().block();
		}
		//4 idle
		assertThat(pool.metrics().idleSize()).as("idleSize").isEqualTo(4);
		assertThat(destroyed).as("none destroyed so far").isEmpty();

		//set the release predicate to release <= 3 on acquire
		releasedIndex.set(4);

		assertThat(pool.acquire().block().poolable()).as("allocated post idle").isEqualTo(5);
		assertThat(intSource).as("did generate a new value").hasValue(5);
		assertThat(destroyed).as("single acquire released all idle")
		                     .containsExactly(1, 2, 3, 4);
	}

	@ParameterizedTestWithName
	@MethodSource("allPools")
	void releaseRacingWithPoolClose(PoolStyle configAdjuster)
			throws InterruptedException {
		for (int i = 0; i < 10_000; i++) {
			final int round = i + 1;
			ConcurrentLinkedQueue<AtomicInteger> created = new ConcurrentLinkedQueue<>();
			PoolBuilder<AtomicInteger, PoolConfig<AtomicInteger>> builder =
					PoolBuilder.from(Mono.fromCallable(() -> {
						AtomicInteger resource = new AtomicInteger(round);
						created.add(resource);
						return resource;
					}))
					           .evictionPredicate((obj, meta) -> false)
					           .destroyHandler(ai -> Mono.fromRunnable(() -> ai.set(-1)))
					           .sizeBetween(0, 4);

			InstrumentedPool<AtomicInteger> pool = configAdjuster.apply(builder);

			PooledRef<AtomicInteger> ref = pool.acquire().block();

			final CountDownLatch latch = new CountDownLatch(2);
			//acquire-and-release, vs pool disposal
			RaceTestUtils.race(
					() -> ref.release().doFinally(__ -> latch.countDown()).subscribe(),
					() -> pool.disposeLater().doFinally(__ -> latch.countDown()).subscribe()
			);

			assertThat(latch.await(30, TimeUnit.SECONDS)).as("latch counted down").isTrue();
			assertThat(pool.isDisposed()).as("pool isDisposed").isTrue();
			assertThat(pool.metrics().idleSize()).as("pool has no idle elements").isZero();

//			Thread.sleep(10);
			assertThat(created).allSatisfy(ai -> assertThat(ai).hasValue(-1));
		}
	}

	@ParameterizedTestWithName
	@MethodSource("allPools")
	void poolCloseRacingWithRelease(PoolStyle configAdjuster)
			throws InterruptedException {
		for (int i = 0; i < 10_000; i++) {
			final int round = i + 1;
			ConcurrentLinkedQueue<AtomicInteger> created = new ConcurrentLinkedQueue<>();
			PoolBuilder<AtomicInteger, PoolConfig<AtomicInteger>> builder =
					PoolBuilder.from(Mono.fromCallable(() -> {
						AtomicInteger resource = new AtomicInteger(round);
						created.add(resource);
						return resource;
					}))
					           .evictionPredicate((obj, meta) -> false)
					           .destroyHandler(ai -> Mono.fromRunnable(() -> ai.set(-1)))
					           .sizeBetween(0, 4);

			InstrumentedPool<AtomicInteger> pool = configAdjuster.apply(builder);

			PooledRef<AtomicInteger> ref = pool.acquire().block();

			final CountDownLatch latch = new CountDownLatch(2);
			//acquire-and-release, vs pool disposal
			RaceTestUtils.race(
					() -> pool.disposeLater().doFinally(__ -> latch.countDown()).subscribe(),
					() -> ref.release().doFinally(__ -> latch.countDown()).subscribe()
			);

			assertThat(latch.await(30, TimeUnit.SECONDS)).as("latch counted down").isTrue();
			assertThat(pool.isDisposed()).as("pool isDisposed").isTrue();
			assertThat(pool.metrics().idleSize()).as("pool has no idle elements").isZero();
			assertThat(created).allSatisfy(ai -> assertThat(ai).hasValue(-1));
		}
	}

	@ParameterizedTestWithName
	@MethodSource("allPools")
	@Tag("slow")
	void raceShutdownAndAcquireInvalidate(PoolStyle configAdjuster) {
		AtomicInteger ai = new AtomicInteger();
		PoolBuilder<AtomicInteger, PoolConfig<AtomicInteger>> configBuilder = PoolBuilder
				.from(Mono.fromSupplier(() -> {
					ai.incrementAndGet();
					return ai;
				}))
				.evictionIdle(Duration.ZERO)
				.destroyHandler(resource -> Mono.fromRunnable(resource::decrementAndGet))
				.sizeBetween(0, 1);

		AtomicReference<Throwable> errorRef = new AtomicReference<>();
		for (int i = 0; i < 100_000; i++) {
			errorRef.set(null);
			InstrumentedPool<AtomicInteger> pool = configAdjuster.apply(configBuilder);

			if (i % 2 == 0) {
				RaceTestUtils.race(
						() -> pool.disposeLater().subscribe(v -> {}, errorRef::set),
						() -> pool.acquire()
						          .flatMap(PooledRef::invalidate)
						          .onErrorResume(PoolShutdownException.class, e -> Mono.empty())
						          .subscribe(v -> {}, errorRef::set)
				);
			}
			else {
				RaceTestUtils.race(
						() -> pool.acquire()
						          .flatMap(PooledRef::invalidate)
						          .onErrorResume(PoolShutdownException.class, e -> Mono.empty())
						          .subscribe(v -> {}, errorRef::set),
						() -> pool.disposeLater().subscribe(v -> {}, errorRef::set)
				);
			}
			if (errorRef.get() != null) {
				errorRef.get().printStackTrace();
			}
			assertThat(errorRef.get()).as("exception in " + configAdjuster.toString() + " iteration " + i).isNull();
		}
		assertThat(ai).as("creates and destroys stabilizes to 0").hasValue(0);
	}

	@ParameterizedTestWithName
	@MethodSource("allPools")
	@Tag("slow")
	void raceShutdownAndPreAcquiredInvalidate(PoolStyle configAdjuster) {
		AtomicInteger ai = new AtomicInteger();
		PoolBuilder<AtomicInteger, PoolConfig<AtomicInteger>> configBuilder = PoolBuilder
				.from(Mono.fromSupplier(() -> {
					ai.incrementAndGet();
					return ai;
				}))
				.evictionIdle(Duration.ZERO)
				.destroyHandler(resource -> Mono.fromRunnable(resource::decrementAndGet))
				.sizeBetween(0, 1);

		AtomicReference<Throwable> errorRef = new AtomicReference<>();
		for (int i = 0; i < 100_000; i++) {
			errorRef.set(null);
			InstrumentedPool<AtomicInteger> pool = configAdjuster.apply(configBuilder);
			PooledRef<AtomicInteger> aiRef = pool.acquire().block();

			if (i % 2 == 0) {
				RaceTestUtils.race(
						() -> pool.disposeLater().subscribe(v -> {}, errorRef::set),
						() -> aiRef.invalidate()
						           .onErrorResume(PoolShutdownException.class, e -> Mono.empty())
						           .subscribe(v -> {}, errorRef::set)
				);
			}
			else {
				RaceTestUtils.race(
						() -> aiRef.invalidate()
						           .onErrorResume(PoolShutdownException.class, e -> Mono.empty())
						           .subscribe(v -> {}, errorRef::set),
						() -> pool.disposeLater().subscribe(v -> {}, errorRef::set)
				);
			}
			if (errorRef.get() != null) {
				errorRef.get().printStackTrace();
			}
			assertThat(errorRef.get()).as("exception in " + configAdjuster.toString() + " iteration " + i).isNull();
		}
		assertThat(ai).as("creates and destroys stabilizes to 0").hasValue(0);
	}

	@ParameterizedTestWithName
	@MethodSource("allPools")
	@Tag("slow")
	void raceShutdownAndPreAcquiredReleaseWithEviction(PoolStyle configAdjuster) {
		AtomicInteger ai = new AtomicInteger();
		PoolBuilder<AtomicInteger, PoolConfig<AtomicInteger>> configBuilder = PoolBuilder
				.from(Mono.fromSupplier(() -> {
					ai.incrementAndGet();
					return ai;
				}))
				.evictionIdle(Duration.ZERO)
				.destroyHandler(resource -> Mono.fromRunnable(resource::decrementAndGet))
				.sizeBetween(0, 1);

		AtomicReference<Throwable> errorRef = new AtomicReference<>();
		for (int i = 0; i < 100_000; i++) {
			errorRef.set(null);
			InstrumentedPool<AtomicInteger> pool = configAdjuster.apply(configBuilder);
			PooledRef<AtomicInteger> aiRef = pool.acquire().block();

			if (i % 2 == 0) {
				RaceTestUtils.race(
						() -> pool.disposeLater().subscribe(v -> {}, errorRef::set),
						() -> aiRef.release()
						           .onErrorResume(PoolShutdownException.class, e -> Mono.empty())
						           .subscribe(v -> {}, errorRef::set)
				);
			}
			else {
				RaceTestUtils.race(
						() -> aiRef.release()
						           .onErrorResume(PoolShutdownException.class, e -> Mono.empty())
						           .subscribe(v -> {}, errorRef::set),
						() -> pool.disposeLater().subscribe(v -> {}, errorRef::set)
				);
			}
			if (errorRef.get() != null) {
				errorRef.get().printStackTrace();
			}
			assertThat(errorRef.get()).as("exception in " + configAdjuster.toString() + " iteration " + i).isNull();
		}
		assertThat(ai).as("creates and destroys stabilizes to 0").hasValue(0);
	}

	@ParameterizedTestWithName
	@MethodSource("allPools")
	void maxPendingZero(PoolStyle configAdjuster) {
		AtomicInteger source = new AtomicInteger();
		PoolBuilder<Integer, PoolConfig<Integer>> configBuilder = PoolBuilder
				.from(Mono.fromSupplier(source::incrementAndGet))
				.sizeBetween(1, 2)
				.maxPendingAcquire(0);

		AbstractPool<Integer> pool = configAdjuster.apply(configBuilder);

		assertThat(pool.warmup().block(Duration.ofSeconds(1))).as("warmup").isOne();

		//there is one idle resource
		assertThat(pool.acquire().block(Duration.ofSeconds(1)))
				.as("acquire on idle")
				.isNotNull()
				.returns(1 , PooledRef::poolable);

		//there is now idle resource, but still capacity
		assertThat(pool.acquire().block(Duration.ofSeconds(1)))
				.as("acquire on allocate")
				.isNotNull()
				.returns(2 , PooledRef::poolable);

		//there is now idle resource, but still capacity
		assertThatExceptionOfType(PoolAcquirePendingLimitException.class)
				.isThrownBy(() -> pool.acquire().block(Duration.ofSeconds(1)))
				.as("acquire on maxPending")
				.withMessage("No pending allowed and pool has reached allocation limit");
	}

	@ParameterizedTestWithName
	@MethodSource("allPools")
	void maxPendingOne(PoolStyle configAdjuster)
			throws InterruptedException {
		AtomicInteger source = new AtomicInteger();
		PoolBuilder<Integer, PoolConfig<Integer>> configBuilder = PoolBuilder
				.from(Mono.fromSupplier(source::incrementAndGet))
				.sizeBetween(1, 2)
				.maxPendingAcquire(1);

		AbstractPool<Integer> pool = configAdjuster.apply(configBuilder);

		assertThat(pool.warmup().block(Duration.ofSeconds(1))).as("warmup").isOne();

		//there is one idle resource
		assertThat(pool.acquire().block(Duration.ofSeconds(1)))
				.as("acquire on idle")
				.isNotNull()
				.returns(1 , PooledRef::poolable);

		//there is now idle resource, but still capacity
		assertThat(pool.acquire().block(Duration.ofSeconds(1)))
				.as("acquire on allocate")
				.isNotNull()
				.returns(2 , PooledRef::poolable);

		//there is now idle resource, no capacity, but pending 1 is allowed
		//in order to test the scenario where we've reached maxPending, this pending must not be cancelled
		CompletableFuture<PooledRef<Integer>> pendingNumberOne = pool.acquire().timeout(Duration.ofSeconds(1)).toFuture();

		//there is now idle resource, no capacity and we've reached maxPending
		assertThatExceptionOfType(PoolAcquirePendingLimitException.class)
				.isThrownBy(() -> pool.acquire().block(Duration.ofSeconds(1)))
				.as("pending acquire on maxPending")
				.withMessage("Pending acquire queue has reached its maximum size of 1");

		//post-assert the intermediate
		assertThatCode(pendingNumberOne::join)
					.as("pending 1 was parked for 1s and then timed out")
					.hasMessageStartingWith("java.util.concurrent.TimeoutException: Did not observe any item or terminal signal within 1000ms");
	}

	@ParameterizedTestWithName
	@EnumSource
	void acquireContextPropagatedToAllocator(PoolStyle style) {
		PoolBuilder<Integer, PoolConfig<Integer>> configBuilder = PoolBuilder
				.from(Mono.deferContextual(c -> Mono.just(c.getOrDefault("ifNew", 0))))
				.sizeBetween(0, 2);

		InstrumentedPool<Integer> pool = style.apply(configBuilder);

		final PooledRef<Integer> ref1 = pool.acquire()
		                                    .contextWrite(Context.of("ifNew", 1))
		                                    .block(Duration.ofSeconds(5));

		final PooledRef<Integer> ref2 = pool.acquire()
		                                    .contextWrite(Context.of("ifNew", 2))
		                                    .block(Duration.ofSeconds(5));

		assertThat(ref1.poolable()).as("atomic 1 via ref1").isEqualTo(1);
		assertThat(ref2.poolable()).as("atomic 2 via ref2").isEqualTo(2);

		ref1.release().block();
		final PooledRef<Integer> ref3 = pool.acquire()
		                                    .contextWrite(Context.of("ifNew", 3))
		                                    .block(Duration.ofSeconds(5));

		assertThat(ref3.poolable()).as("atomic 1 via ref3").isEqualTo(1);

		ref2.release().block();
		final PooledRef<Integer> ref4 = pool.acquire()
		                                    .contextWrite(Context.of("ifNew", 4))
		                                    .block(Duration.ofSeconds(5));

		assertThat(ref4.poolable()).as("atomic 2 via ref4").isEqualTo(2);
	}

	@ParameterizedTestWithName
	@EnumSource
	void warmupContextPropagatedToAllocator(PoolStyle style) {
		AtomicInteger differentiator = new AtomicInteger();
		PoolBuilder<String, PoolConfig<String>> configBuilder = PoolBuilder
				.from(Mono.deferContextual(c -> Mono.just(differentiator.incrementAndGet() + c.getOrDefault("ifNew", "noContext"))))
				.sizeBetween(2, 4);

		InstrumentedPool<String> pool = style.apply(configBuilder);

		pool.warmup()
		    .contextWrite(Context.of("ifNew", "warmup"))
		    .as(StepVerifier::create)
		    .expectNext(2)
		    .expectComplete()
		    .verify(Duration.ofSeconds(5));

		final PooledRef<String> ref1 = pool.acquire().block(Duration.ofSeconds(5));
		final PooledRef<String> ref2 = pool.acquire().block(Duration.ofSeconds(5));
		final PooledRef<String> ref3 = pool.acquire().block(Duration.ofSeconds(5));
		final PooledRef<String> ref4 = pool.acquire().block(Duration.ofSeconds(5));

		//since values are warmed up in batches and put in the idle queue, LRU-MRU has consequences
		assertThat(ref1.poolable()).as("ref1").isEqualTo(style.isLru ? "1warmup" : "2warmup");
		assertThat(ref2.poolable()).as("ref2").isEqualTo(style.isLru ? "2warmup" : "1warmup");
		assertThat(ref3.poolable()).as("ref3").isEqualTo("3noContext");
		assertThat(ref4.poolable()).as("ref4").isEqualTo("4noContext");
	}

	@ParameterizedTestWithName
	@EnumSource
	void longAllocationDoesntUpdateAcquired(PoolStyle style) {
		PoolBuilder<Integer, PoolConfig<Integer>> configBuilder = PoolBuilder
				.from(Mono.just(1).delayElement(Duration.ofSeconds(1)))
				.sizeBetween(0, 1);

		InstrumentedPool<Integer> pool = style.apply(configBuilder);

		AtomicBoolean next = new AtomicBoolean();
		AtomicBoolean error = new AtomicBoolean();
		AtomicBoolean complete = new AtomicBoolean();

		pool.acquire().subscribe(
				stringPooledRef -> next.set(true),
				throwable -> error.set(true),
				() -> complete.set(true)
		);

		assertThat(next).as("hasn't received value yet").isFalse();
		assertThat(pool.metrics().acquiredSize()).as("acquired size before allocator done").isZero();

		Awaitility.await().untilAtomic(complete, is(true));

		assertThat(error).as("error").isFalse();
		assertThat(next).as("next").isTrue();

		assertThat(pool.metrics().acquiredSize()).as("acquired size after allocator done").isOne();
	}

	private static Mono<Void> throwingFunction(String poolable) {
		if (poolable.endsWith("1")) {
			IllegalStateException e = new IllegalStateException("(expected) throwing instead of producing a Mono for " + poolable);
			StackTraceElement[] traceElements = e.getStackTrace();
			e.setStackTrace(Arrays.copyOf(traceElements, 5)); //don't fill the logs with too much stack trace
			throw e;
		}
		return Mono.empty();
	}

	@ParameterizedTestWithName
	@EnumSource
	void releaseHandlerProtectedAgainstThrowingFunction(PoolStyle style) {
		AtomicInteger count = new AtomicInteger();
		PoolBuilder<String, PoolConfig<String>> configBuilder = PoolBuilder
				.from(Mono.fromCallable(() -> "releaseHandlerCase" + count.incrementAndGet()))
				.sizeBetween(0, 1)
				.releaseHandler(CommonPoolTest::throwingFunction);

		InstrumentedPool<String> pool = style.apply(configBuilder);

		PooledRef<String> ref1 = pool.acquire().block();
		assert ref1 != null;

		//set up a concurrent acquire (blocked for now)
		AtomicReference<PooledRef<String>> acquire2 = new AtomicReference<>();
		pool.acquire().subscribe(acquire2::set);

		StepVerifier.create(ref1.release())
				.expectErrorSatisfies(t -> assertThat(t)
						.hasMessage("Couldn't apply releaseHandler, resource destroyed")
						.hasCause(new IllegalStateException("(expected) throwing instead of producing a Mono for releaseHandlerCase1")))
				.verify(Duration.ofSeconds(1));

		//assert that concurrent acquire has been unstuck, then destroy it
		assertThat(acquire2.get()).as("concurrent acquire2").isNotNull();
		acquire2.get().invalidate().block();

		//assert that next acquire will work
		PooledRef<String> ref3 = pool.acquire().block(Duration.ofSeconds(1));
		assert ref3 != null;
		assertThat(ref3.poolable()).as("poolable 3").isEqualTo("releaseHandlerCase3");
	}

	@ParameterizedTestWithName
	@EnumSource
	void destroyHandlerProtectedAgainstThrowingFunction_invalidate(PoolStyle style) {
		AtomicInteger count = new AtomicInteger();
		PoolBuilder<String, PoolConfig<String>> configBuilder = PoolBuilder
				.from(Mono.fromCallable(() -> "destroyHandlerInvalidateCase" + count.incrementAndGet()))
				.sizeBetween(0, 1)
				.destroyHandler(CommonPoolTest::throwingFunction);

		InstrumentedPool<String> pool = style.apply(configBuilder);

		PooledRef<String> ref1 = pool.acquire().block();
		assert ref1 != null;

		//set up a concurrent acquire (blocked for now)
		AtomicReference<PooledRef<String>> acquire2 = new AtomicReference<>();
		pool.acquire().subscribe(acquire2::set);

		StepVerifier.create(ref1.invalidate())
				.expectErrorSatisfies(t -> assertThat(t)
						.hasMessage("(expected) throwing instead of producing a Mono for destroyHandlerInvalidateCase1")
						.hasNoCause())
				.verify(Duration.ofSeconds(1));

		//assert that concurrent acquire has been unstuck, then destroy it
		assertThat(acquire2.get()).as("concurrent acquire2").isNotNull();
		acquire2.get().invalidate().block();

		//assert that next acquire will work
		PooledRef<String> ref3 = pool.acquire().block(Duration.ofSeconds(1));
		assert ref3 != null;
		assertThat(ref3.poolable()).as("poolable 3").isEqualTo("destroyHandlerInvalidateCase3");
	}

	@ParameterizedTestWithName
	@EnumSource
	void destroyHandlerProtectedAgainstThrowingFunction_releaseWithEviction(PoolStyle style) {
		AtomicInteger count = new AtomicInteger();
		PoolBuilder<String, PoolConfig<String>> configBuilder = PoolBuilder
				.from(Mono.fromCallable(() -> "destroyHandlerReleaseCase" + count.incrementAndGet()))
				.sizeBetween(0, 1)
				.evictionPredicate((s, ref) -> ref.acquireCount() == 1 && s.endsWith("1"))
				.destroyHandler(CommonPoolTest::throwingFunction);

		InstrumentedPool<String> pool = style.apply(configBuilder);

		PooledRef<String> ref1 = pool.acquire().block();
		assert ref1 != null;

		//set up a concurrent acquire (blocked for now)
		AtomicReference<PooledRef<String>> acquire2 = new AtomicReference<>();
		pool.acquire().subscribe(acquire2::set, Throwable::printStackTrace);

		StepVerifier.create(ref1.release())
				.expectErrorSatisfies(t -> assertThat(t)
						.hasMessage("(expected) throwing instead of producing a Mono for destroyHandlerReleaseCase1")
						.hasNoCause())
				.verify(Duration.ofSeconds(1));

		//assert that concurrent acquire has been unstuck, then destroy it
		assertThat(acquire2.get()).as("concurrent acquire2").isNotNull();
		acquire2.get().invalidate().block();

		//assert that next acquire will work
		PooledRef<String> ref3 = pool.acquire().block(Duration.ofSeconds(1));
		assert ref3 != null;
		assertThat(ref3.poolable()).as("poolable 3").isEqualTo("destroyHandlerReleaseCase3");
	}

	@ParameterizedTestWithName
	@EnumSource
	void destroyHandlerProtectedAgainstThrowingFunction_evictInBackground(PoolStyle style) {
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();
		AtomicInteger count = new AtomicInteger();
		PoolBuilder<String, PoolConfig<String>> configBuilder = PoolBuilder
				.from(Mono.fromCallable(() -> "destroyHandlerBackgroundCase" + count.incrementAndGet()))
				.sizeBetween(0, 1)
				.clock(SchedulerClock.of(vts))
				.evictInBackground(Duration.ofSeconds(4), vts)
				//we need to assert lifetime, which is precise thanks to VTS, otherwise acquire().release() would trip eviction
				.evictionPredicate((s, ref) -> ref.acquireCount() == 1 && ref.lifeTime() >= 4000)
				.destroyHandler(CommonPoolTest::throwingFunction);

		InstrumentedPool<String> pool = style.apply(configBuilder);

		//generate a first value and immediately release it back to the pool
		PooledRef<String> initialRef = pool.acquire().block(Duration.ofMillis(100));
		assert initialRef != null;
		initialRef.release().block(Duration.ofMillis(500));

		//trigger background eviction
		assertThatCode(() -> vts.advanceTimeBy(Duration.ofSeconds(10))).doesNotThrowAnyException();
		assertThat(pool.metrics().idleSize()).as("background eviction ran").isZero();

		//assert that next acquire will work
		PooledRef<String> ref2 = pool.acquire().block(Duration.ofSeconds(1));
		assert ref2 != null;
		assertThat(ref2.poolable()).as("poolable 2").isEqualTo("destroyHandlerBackgroundCase2");
	}

	@ParameterizedTestWithName
	@EnumSource
	void releaseHandlerFailsThenDestroyHandlerAlsoFails(PoolStyle style) {
		AtomicInteger count = new AtomicInteger();
		PoolBuilder<String, PoolConfig<String>> configBuilder = PoolBuilder
				.from(Mono.fromCallable(() -> "bothReleaseHandlerAndDestroyHandlerFail" + count.incrementAndGet()))
				.sizeBetween(0, 1)
				.evictionPredicate((s, ref) -> ref.acquireCount() == 1)
				.releaseHandler(CommonPoolTest::throwingFunction)
				.destroyHandler(CommonPoolTest::throwingFunction);

		InstrumentedPool<String> pool = style.apply(configBuilder);

		PooledRef<String> ref1 = pool.acquire().block();
		assert ref1 != null;

		//set up a concurrent acquire (blocked for now)
		AtomicReference<PooledRef<String>> acquire2 = new AtomicReference<>();
		pool.acquire().subscribe(acquire2::set, Throwable::printStackTrace);

		StepVerifier.create(ref1.release())
				.expectErrorSatisfies(t -> {
					assertThat(t)
							.isInstanceOf(IllegalStateException.class)
							.hasMessage("Couldn't apply releaseHandler nor destroyHandler");
					assertThat(Exceptions.unwrapMultiple(t.getCause()).stream().map(Throwable::getMessage))
							.containsExactly("(expected) throwing instead of producing a Mono for bothReleaseHandlerAndDestroyHandlerFail1",
									"(expected) throwing instead of producing a Mono for bothReleaseHandlerAndDestroyHandlerFail1");
				})
				.verify(Duration.ofSeconds(1));

		//assert that concurrent acquire has been unstuck, then destroy it
		assertThat(acquire2.get()).as("concurrent acquire2").isNotNull();
		acquire2.get().invalidate().block();

		//assert that next acquire will work
		PooledRef<String> ref3 = pool.acquire().block(Duration.ofSeconds(1));
		assert ref3 != null;
		assertThat(ref3.poolable()).as("poolable 3").isEqualTo("bothReleaseHandlerAndDestroyHandlerFail3");
	}

	@ParameterizedTestWithName
	@EnumSource
	void invalidateIgnoredAfterReleasedDoesntAffectMetrics(PoolStyle style) {
		AtomicInteger releaseCount = new AtomicInteger();
		AtomicInteger destroyCount = new AtomicInteger();
		PoolBuilder<String, PoolConfig<String>> configBuilder = PoolBuilder
				.from(Mono.fromCallable(() -> "invalidateIgnoredAfterReleased"))
				.sizeBetween(0, 1)
				.releaseHandler(it -> Mono.fromRunnable(releaseCount::incrementAndGet))
				.destroyHandler(it -> Mono.fromRunnable(destroyCount::incrementAndGet));

		InstrumentedPool<String> pool = style.apply(configBuilder);

		PooledRef<String> ref1 = pool.acquire().block();
		assert ref1 != null;
		assertThat(pool.metrics().acquiredSize()).as("acquired metric pre-release").isOne();
		assertThat(pool.metrics().allocatedSize()).as("allocated metric pre-release").isOne();
		assertThat(pool.metrics().idleSize()).as("idle metric pre-release").isZero();
		assertThat(releaseCount).as("released pre-release").hasValue(0);
		assertThat(destroyCount).as("destroyed pre-release").hasValue(0);

		ref1.release().block();

		assertThat(pool.metrics().acquiredSize()).as("acquired metric post-release").isZero();
		assertThat(pool.metrics().allocatedSize()).as("allocated metric post-release").isOne();
		assertThat(pool.metrics().idleSize()).as("idle metric post-release").isOne();
		assertThat(releaseCount).as("released post-release").hasValue(1);
		assertThat(destroyCount).as("destroyed post-release").hasValue(0);

		ref1.invalidate().block();

		assertThat(pool.metrics().acquiredSize()).as("acquired metric post-invalidate").isZero();
		assertThat(pool.metrics().allocatedSize()).as("allocated metric post-invalidate").isOne();
		assertThat(pool.metrics().idleSize()).as("idle metric post-invalidate").isOne();
		assertThat(releaseCount).as("released post-invalidate").hasValue(1);
		assertThat(destroyCount).as("destroyed post-invalidate").hasValue(0);
	}

	@ParameterizedTestWithName
	@EnumSource
	void releaseIgnoredAfterInvalidatedDoesntAffectMetrics(PoolStyle style) {
		AtomicInteger releaseCount = new AtomicInteger();
		AtomicInteger destroyCount = new AtomicInteger();
		PoolBuilder<String, PoolConfig<String>> configBuilder = PoolBuilder
				.from(Mono.fromCallable(() -> "releaseIgnoredAfterInvalidated"))
				.sizeBetween(0, 1)
				.releaseHandler(it -> Mono.fromRunnable(releaseCount::incrementAndGet))
				.destroyHandler(it -> Mono.fromRunnable(destroyCount::incrementAndGet));

		InstrumentedPool<String> pool = style.apply(configBuilder);

		PooledRef<String> ref1 = pool.acquire().block();
		assert ref1 != null;
		assertThat(pool.metrics().acquiredSize()).as("acquired metric pre-invalidate").isOne();
		assertThat(pool.metrics().allocatedSize()).as("allocated metric pre-invalidate").isOne();
		assertThat(pool.metrics().idleSize()).as("idle metric pre-invalidate").isZero();
		assertThat(releaseCount).as("released pre-invalidate").hasValue(0);
		assertThat(destroyCount).as("destroyed pre-invalidate").hasValue(0);

		ref1.invalidate().block();

		assertThat(pool.metrics().acquiredSize()).as("acquired metric post-invalidate").isZero();
		assertThat(pool.metrics().allocatedSize()).as("allocated metric post-invalidate").isZero();
		assertThat(pool.metrics().idleSize()).as("idle metric post-invalidate").isZero();
		assertThat(releaseCount).as("released post-invalidate").hasValue(0);
		assertThat(destroyCount).as("destroyed post-invalidate").hasValue(1);

		ref1.release().block();

		assertThat(pool.metrics().acquiredSize()).as("acquired metric post-release").isZero();
		assertThat(pool.metrics().allocatedSize()).as("allocated metric post-release").isZero();
		assertThat(pool.metrics().idleSize()).as("idle metric post-release").isZero();
		assertThat(releaseCount).as("released post-release").hasValue(0);
		assertThat(destroyCount).as("destroyed post-release").hasValue(1);
	}

	@ParameterizedTestWithName
	@EnumSource
	void evictingReleasedResourcesImpactsResource(PoolStyle style) {
		AtomicInteger releaseCount = new AtomicInteger();
		AtomicBoolean canEvict = new AtomicBoolean();
		PoolBuilder<AtomicBoolean, PoolConfig<AtomicBoolean>> configBuilder = PoolBuilder
			.from(Mono.fromCallable(AtomicBoolean::new))
			.sizeBetween(0, 1)
			.evictionPredicate((it, meta) -> canEvict.get())
			.releaseHandler(it -> Mono.fromRunnable(releaseCount::incrementAndGet))
			.destroyHandler(it -> Mono.fromRunnable(() -> it.set(true)));

		InstrumentedPool<AtomicBoolean> pool = style.apply(configBuilder);

		PooledRef<AtomicBoolean> ref1 = pool.acquire().block();
		ref1.release().block();

		Assumptions.assumeThat(pool).isInstanceOf(SimpleDequePool.class);
		canEvict.set(true);
		((SimpleDequePool<?>) pool).evictInBackground();

		assertThat(ref1.poolable()).as("poolable impacted by eviction").isTrue();
		assertThat(pool.metrics().acquiredSize()).as("acquired metric post-eviction").isZero();
		assertThat(pool.metrics().allocatedSize()).as("allocated metric post-eviction").isZero();
		assertThat(pool.metrics().idleSize()).as("idle metric post-eviction").isZero();
		assertThat(releaseCount).as("released post-eviction").hasValue(1);
	}

	@ParameterizedTestWithName
	@EnumSource
	void poolExposesConfig(PoolStyle style) {
		final Clock clock = Clock.systemUTC();
		PoolBuilder<String, PoolConfig<String>> configBuilder = PoolBuilder
			.from(Mono.just("test"))
			.sizeBetween(0, 123)
			.clock(clock);

		InstrumentedPool<String> pool = style.apply(configBuilder);
		PoolConfig<String> config = pool.config();

		assertThat(config.allocationStrategy().estimatePermitCount()).as("maxSize").isEqualTo(123);
		assertThat(config.clock()).as("clock").isSameAs(clock);
	}
}
