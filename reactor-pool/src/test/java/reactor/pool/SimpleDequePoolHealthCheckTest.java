/*
 * Copyright (c) 2026 VMware Inc. or its affiliates, All Rights Reserved.
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
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.scheduler.VirtualTimeScheduler;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the background async health check feature added for
 * <a href="https://github.com/reactor/reactor-pool/issues/285">reactor-pool#285</a>.
 */
class SimpleDequePoolHealthCheckTest {

	@Test
	void disabledByDefaultNeverInvokesHealthCheck() {
		AtomicInteger invocations = new AtomicInteger();
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();
		SimpleDequePool<String> pool = new SimpleDequePool<>(
				PoolBuilder.from(Mono.just("example"))
				           .sizeBetween(1, 1)
				           .healthCheck((s, meta) -> {
					           invocations.incrementAndGet();
					           return Mono.just(true);
				           })
				           .buildConfig());

		assertThat(pool.healthCheckTask).as("no task scheduled").isNotNull();
		assertThat(pool.healthCheckTask.isDisposed()).as("no-op disposable").isTrue();

		pool.warmup().block();
		vts.advanceTimeBy(Duration.ofDays(1));

		assertThat(invocations).as("health check never invoked").hasValue(0);
	}

	@Test
	void healthyResourceStaysIdleAndIsNotReallocated() {
		AtomicInteger allocations = new AtomicInteger();
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();
		SimpleDequePool<String> pool = new SimpleDequePool<>(
				PoolBuilder.from(Mono.fromCallable(() -> "resource-" + allocations.incrementAndGet()))
				           .sizeBetween(1, 1)
				           .healthCheck((s, meta) -> Mono.just(true))
				           .healthCheckInBackground(Duration.ofSeconds(10), vts)
				           .buildConfig());

		pool.warmup().block();
		assertThat(pool.idleSize()).isEqualTo(1);
		assertThat(allocations).hasValue(1);

		vts.advanceTimeBy(Duration.ofSeconds(10));

		assertThat(pool.idleSize()).as("still idle after healthy check").isEqualTo(1);
		assertThat(allocations).as("resource wasn't recreated").hasValue(1);
	}

	@Test
	void unhealthyResourceIsDestroyedAndPoolIsBackfilled() {
		AtomicInteger allocations = new AtomicInteger();
		AtomicInteger destructions = new AtomicInteger();
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();
		SimpleDequePool<String> pool = new SimpleDequePool<>(
				PoolBuilder.from(Mono.fromCallable(() -> "resource-" + allocations.incrementAndGet()))
				           .sizeBetween(1, 1)
				           .destroyHandler(s -> Mono.fromRunnable(destructions::incrementAndGet))
				           .healthCheck((s, meta) -> Mono.just(false))
				           .healthCheckInBackground(Duration.ofSeconds(10), vts)
				           .buildConfig());

		pool.warmup().block();
		assertThat(allocations).hasValue(1);

		vts.advanceTimeBy(Duration.ofSeconds(10));

		assertThat(destructions).as("unhealthy resource destroyed").hasValue(1);
		assertThat(allocations).as("pool backfilled to minimum size").hasValue(2);
		assertThat(pool.idleSize()).as("backfilled resource is idle").isEqualTo(1);
	}

	/**
	 * Port of HikariCP's {@code TestConnections#testKeepalive2}: it's not enough that internal counters/idleSize
	 * look right after an unhealthy resource is destroyed, a caller that acquires afterward must observe a
	 * genuinely different (fresh) instance, never the one that failed its health check.
	 */
	@Test
	void nextAcquireAfterUnhealthyDestroyGetsAFreshInstanceNotTheDeadOne() {
		AtomicInteger allocations = new AtomicInteger();
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();
		SimpleDequePool<String> pool = new SimpleDequePool<>(
				PoolBuilder.from(Mono.fromCallable(() -> "resource-" + allocations.incrementAndGet()))
				           .sizeBetween(1, 1)
				           .healthCheck((s, meta) -> Mono.just(false))
				           .healthCheckInBackground(Duration.ofSeconds(10), vts)
				           .buildConfig());

		pool.warmup().block();
		PooledRef<String> original = pool.acquire().block();
		assertThat(original).isNotNull();
		original.release().block();
		assertThat(original.poolable()).isEqualTo("resource-1");

		vts.advanceTimeBy(Duration.ofSeconds(10));

		PooledRef<String> replacement = pool.acquire().block();
		assertThat(replacement).isNotNull();
		assertThat(replacement.poolable())
				.as("acquirer gets the freshly backfilled resource, never the dead one")
				.isNotEqualTo(original.poolable())
				.isEqualTo("resource-2");
	}

	@Test
	void resourceUnderHealthCheckIsNotConcurrentlyAcquirable() {
		Sinks.One<Boolean> healthCheckSink = Sinks.one();
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();
		SimpleDequePool<String> pool = new SimpleDequePool<>(
				PoolBuilder.from(Mono.just("the-one-resource"))
				           .sizeBetween(1, 1)
				           .healthCheck((s, meta) -> healthCheckSink.asMono())
				           .healthCheckInBackground(Duration.ofSeconds(10), vts)
				           .buildConfig());

		pool.warmup().block();
		assertThat(pool.idleSize()).isEqualTo(1);

		//trigger the sweep: the resource is popped out of idleResources and the (still pending) check subscribed
		vts.advanceTimeBy(Duration.ofSeconds(10));
		assertThat(pool.idleSize()).as("resource removed from idle while under test").isEqualTo(0);

		AtomicReference<PooledRef<String>> acquired = new AtomicReference<>();
		pool.acquire().subscribe(acquired::set);

		assertThat(acquired).as("acquire cannot be served while resource is under health check").hasValue(null);
		assertThat(pool.pendingAcquireSize()).as("acquire is queued as pending").isEqualTo(1);

		//now let the health check resolve as healthy
		healthCheckSink.tryEmitValue(true);

		assertThat(acquired.get()).as("pending acquire is served with the resource once healthy").isNotNull();
		assertThat(acquired.get().poolable()).isEqualTo("the-one-resource");
		assertThat(pool.pendingAcquireSize()).isEqualTo(0);
	}

	@Test
	void timeoutCausesResourceToBeTreatedAsUnhealthy() {
		AtomicInteger destructions = new AtomicInteger();
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();
		SimpleDequePool<String> pool = new SimpleDequePool<>(
				PoolBuilder.from(Mono.just("example"))
				           .sizeBetween(1, 1)
				           .destroyHandler(s -> Mono.fromRunnable(destructions::incrementAndGet))
				           .healthCheck((s, meta) -> Mono.never()) //never resolves on its own
				           .healthCheckTimeout(Duration.ofSeconds(1))
				           .healthCheckInBackground(Duration.ofSeconds(10), vts)
				           .buildConfig());

		pool.warmup().block();

		vts.advanceTimeBy(Duration.ofSeconds(10));
		assertThat(destructions).as("not yet timed out").hasValue(0);

		vts.advanceTimeBy(Duration.ofSeconds(1));
		assertThat(destructions).as("destroyed after timeout").hasValue(1);
	}

	@Test
	void errorFromHealthCheckIsTreatedAsUnhealthy() {
		AtomicInteger destructions = new AtomicInteger();
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();
		SimpleDequePool<String> pool = new SimpleDequePool<>(
				PoolBuilder.from(Mono.just("example"))
				           .sizeBetween(1, 1)
				           .destroyHandler(s -> Mono.fromRunnable(destructions::incrementAndGet))
				           .healthCheck((s, meta) -> Mono.error(new IllegalStateException("boom")))
				           .healthCheckInBackground(Duration.ofSeconds(10), vts)
				           .buildConfig());

		pool.warmup().block();

		vts.advanceTimeBy(Duration.ofSeconds(10));

		assertThat(destructions).as("errored check treated as unhealthy").hasValue(1);
	}

	@Test
	void healthCheckParallelismBoundsResourcesPulledPerSweep() {
		AtomicInteger concurrentChecks = new AtomicInteger();
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();
		SimpleDequePool<String> pool = new SimpleDequePool<>(
				PoolBuilder.from(Mono.just("example"))
				           .sizeBetween(3, 3)
				           .healthCheck((s, meta) -> {
					           concurrentChecks.incrementAndGet();
					           return Mono.never(); //never resolves, so we can observe the in-flight state
				           })
				           .healthCheckParallelism(2)
				           .healthCheckInBackground(Duration.ofSeconds(10), vts)
				           .buildConfig());

		pool.warmup().block();
		assertThat(pool.idleSize()).isEqualTo(3);

		vts.advanceTimeBy(Duration.ofSeconds(10));

		assertThat(concurrentChecks).as("only up to parallelism checks started").hasValue(2);
		assertThat(pool.idleSize()).as("remaining resource still idle/available").isEqualTo(1);
	}

	/**
	 * With parallelism 1 (or more generally parallelism &lt; idle count), a sweep only checks a subset of the
	 * idle resources at a time. A healthy resource must be rotated to the opposite end of the idle deque from
	 * the one sweeps consume from, otherwise successive sweeps keep re-picking the same resource(s) and the
	 * rest of the idle set is never checked. This asserts full round-robin coverage across several sweeps,
	 * including wrap-around back to the first resource.
	 */
	@Test
	void repeatedSweepsRotateThroughAllIdleResourcesInsteadOfRecheckingTheSameOne() {
		List<String> checkedOrder = new CopyOnWriteArrayList<>();
		AtomicInteger allocations = new AtomicInteger();
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();
		SimpleDequePool<String> pool = new SimpleDequePool<>(
				PoolBuilder.from(Mono.fromCallable(() -> "resource-" + allocations.incrementAndGet()))
				           .sizeBetween(3, 3)
				           .healthCheck((s, meta) -> {
					           checkedOrder.add(s);
					           return Mono.just(true);
				           })
				           .healthCheckParallelism(1)
				           .healthCheckInBackground(Duration.ofSeconds(10), vts)
				           .buildConfig());

		pool.warmup().block();
		assertThat(pool.idleSize()).isEqualTo(3);

		//4 sweeps: one full rotation over the 3 idle resources, plus one to prove it wraps back around
		for (int i = 0; i < 4; i++) {
			vts.advanceTimeBy(Duration.ofSeconds(10));
		}

		assertThat(checkedOrder)
				.as("every idle resource gets checked in turn, then wraps back around, instead of only the first ever being revisited")
				.containsExactly("resource-1", "resource-2", "resource-3", "resource-1");
		assertThat(allocations).as("no resource was destroyed/reallocated, all checks were healthy").hasValue(3);
		assertThat(pool.idleSize()).as("all resources remain idle").isEqualTo(3);
	}

	@Test
	void sweepBacksOffWhenThereArePendingAcquires() {
		AtomicInteger invocations = new AtomicInteger();
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();
		SimpleDequePool<String> pool = new SimpleDequePool<>(
				PoolBuilder.from(Mono.just("example"))
				           .sizeBetween(1, 1)
				           .healthCheck((s, meta) -> {
					           invocations.incrementAndGet();
					           return Mono.just(true);
				           })
				           .healthCheckInBackground(Duration.ofSeconds(10), vts)
				           .buildConfig());

		pool.warmup().block();
		pool.pendingSize = 1; //simulate an in-flight pending acquire, same-package test access

		vts.advanceTimeBy(Duration.ofSeconds(10));

		assertThat(invocations).as("sweep backed off due to pool activity").hasValue(0);
		assertThat(pool.idleSize()).as("idle resource untouched").isEqualTo(1);
	}

	@Test
	void disposeLaterCancelsHealthCheckTask() {
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();
		SimpleDequePool<String> pool = new SimpleDequePool<>(
				PoolBuilder.from(Mono.just("example"))
				           .healthCheck((s, meta) -> Mono.just(true))
				           .healthCheckInBackground(Duration.ofSeconds(10), vts)
				           .buildConfig());

		assertThat(pool.healthCheckTask).isNotNull();
		assertThat(pool.healthCheckTask.isDisposed()).isFalse();

		pool.disposeLater().block();

		assertThat(pool.healthCheckTask.isDisposed()).as("background task cancelled on dispose").isTrue();
	}

	/**
	 * A resource pulled out of {@code idleResources} for an in-flight background health check is not tracked
	 * anywhere else. If the {@link PoolConfig#healthCheck()} publisher never emits (the documented default has
	 * no timeout) and {@code disposeLater()} is called while the check is still pending, the resource must
	 * still be destroyed and its allocation permit returned - it must not be silently leaked.
	 */
	@Test
	void disposeLaterDestroysResourceStuckInAnInFlightHealthCheck() {
		AtomicInteger destructions = new AtomicInteger();
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();
		SimpleDequePool<String> pool = new SimpleDequePool<>(
				PoolBuilder.from(Mono.just("example"))
				           .sizeBetween(1, 1)
				           .destroyHandler(s -> Mono.fromRunnable(destructions::incrementAndGet))
				           .healthCheck((s, meta) -> Mono.never()) //never resolves, no timeout configured
				           .healthCheckInBackground(Duration.ofSeconds(10), vts)
				           .buildConfig());

		pool.warmup().block();
		vts.advanceTimeBy(Duration.ofSeconds(10));
		assertThat(pool.idleSize()).as("resource pulled out for the (hanging) check").isEqualTo(0);

		pool.disposeLater().block();

		assertThat(destructions)
				.as("resource stuck in an in-flight health check is still destroyed on dispose, not leaked")
				.hasValue(1);
	}

	/**
	 * Disposing an in-flight sweep's subscription runs its {@code doFinally} synchronously, which used to call
	 * {@code scheduleHealthCheck()} unconditionally - re-arming a brand new {@code healthCheckTask} right after
	 * {@code disposeLater()} had just disposed the previous one. That leaves the pool with a live, un-disposed
	 * background task after shutdown.
	 */
	@Test
	void disposingAnInFlightSweepDoesNotRescheduleAfterDispose() {
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();
		SimpleDequePool<String> pool = new SimpleDequePool<>(
				PoolBuilder.from(Mono.just("example"))
				           .sizeBetween(1, 1)
				           .healthCheck((s, meta) -> Mono.never()) //never resolves: sweep stays in flight
				           .healthCheckInBackground(Duration.ofSeconds(10), vts)
				           .buildConfig());

		pool.warmup().block();
		vts.advanceTimeBy(Duration.ofSeconds(10));
		assertThat(pool.healthCheckSweepSubscription).as("sweep is in flight").isNotNull();

		pool.disposeLater().block();

		assertThat(pool.healthCheckTask)
				.as("no new task scheduled behind disposeLater()'s back")
				.isNotNull();
		assertThat(pool.healthCheckTask.isDisposed())
				.as("the task disposeLater() disposed must stay disposed, not be replaced by a fresh scheduled one")
				.isTrue();
	}

	/**
	 * {@code healthCheckInBackground()}'s early backoff (pool activity / lost the WIP race) and empty-candidate
	 * paths also call {@code scheduleHealthCheck()}, without themselves checking disposal - if a tick happens
	 * to reach either path in the (narrow, real) window where the pool has just been disposed, they'd re-arm a
	 * task the same way the doFinally path used to. The guard belongs inside {@code scheduleHealthCheck()}
	 * itself so every caller - present or future - is covered unconditionally, not just the doFinally one.
	 */
	@Test
	void scheduleHealthCheckNeverArmsANewTaskOnceDisposed() {
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();
		SimpleDequePool<String> pool = new SimpleDequePool<>(
				PoolBuilder.from(Mono.just("example"))
				           .healthCheckInBackground(Duration.ofSeconds(10), vts)
				           .buildConfig());

		pool.disposeLater().block();
		assertThat(pool.healthCheckTask.isDisposed()).as("disposed by disposeLater()").isTrue();

		//simulates any caller (backoff/empty-candidate paths, doFinally, or a future one) reaching
		//scheduleHealthCheck() after the pool is already disposed
		pool.scheduleHealthCheck();

		assertThat(pool.healthCheckTask)
				.as("still the same already-disposed task disposeLater() left behind")
				.isNotNull();
		assertThat(pool.healthCheckTask.isDisposed())
				.as("scheduleHealthCheck() must refuse to arm a new task once the pool is disposed, regardless of caller")
				.isTrue();
	}

	/**
	 * {@code healthCheckSweepCandidates} must only ever contain refs whose check hasn't resolved yet. If one
	 * candidate in a batch resolves healthy (and gets reoffered, then acquired by a live borrower) while another
	 * candidate from the *same* batch is still pending, {@code disposeLater()} must not destroy the acquired one
	 * just because the batch as a whole isn't finished - that would destroy a resource out from under the
	 * borrower currently holding it, without their knowledge.
	 */
	@Test
	void disposeDoesNotDestroyAnAlreadyAcquiredResourceFromTheSameHealthCheckBatch() {
		List<String> destroyedPoolables = new CopyOnWriteArrayList<>();
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();
		SimpleDequePool<String> pool = new SimpleDequePool<>(
				PoolBuilder.from(Mono.fromCallable(new AtomicInteger()::incrementAndGet).map(n -> "resource-" + n))
				           .sizeBetween(2, 2)
				           .destroyHandler(s -> Mono.fromRunnable(() -> destroyedPoolables.add(s)))
				           //resource-1 resolves immediately as healthy, resource-2 hangs forever: both are
				           //part of the same sweep batch (healthCheckParallelism(2))
				           .healthCheck((s, meta) -> "resource-1".equals(s) ? Mono.just(true) : Mono.never())
				           .healthCheckParallelism(2)
				           .healthCheckInBackground(Duration.ofSeconds(10), vts)
				           .buildConfig());

		pool.warmup().block();
		vts.advanceTimeBy(Duration.ofSeconds(10));

		//resource-1's check already resolved and was reoffered; resource-2's check is still pending
		assertThat(pool.idleSize()).as("resource-1 reoffered, resource-2 still checked out").isEqualTo(1);

		PooledRef<String> acquired = pool.acquire().block();
		assertThat(acquired).isNotNull();
		assertThat(acquired.poolable()).as("the only idle resource is the one that already resolved").isEqualTo("resource-1");

		pool.disposeLater().block();

		assertThat(destroyedPoolables)
				.as("acquired resource must not be destroyed while still held by its borrower")
				.doesNotContain("resource-1");
		assertThat(destroyedPoolables)
				.as("the genuinely still-hanging resource is destroyed by dispose as usual")
				.containsExactly("resource-2");

		acquired.release().block();
		assertThat(destroyedPoolables)
				.as("once released (pool already disposed), the resource is cleaned up through the normal release path")
				.contains("resource-1");
	}

	/**
	 * Background eviction ({@code evictInBackground()}) and the background health check sweep
	 * ({@code healthCheckInBackground()}) both mutate {@code idleResources}/{@code idleSize}. Without sharing the
	 * pool's {@code WIP} exclusion guard, a ref could be polled by the health check sweep at the same time
	 * eviction's iterator is independently destroying it: both sides would {@code decrementIdle()} for what is
	 * really a single removal, and the health check could later re-offer a ref that eviction already destroyed.
	 * <p>
	 * Runs both reapers from separate threads, synchronized to start together via a {@link CyclicBarrier} to
	 * maximize contention on the same set of idle refs, repeated over many iterations. Regardless of which
	 * reaper "wins" a given ref on a given run (the outcome is legitimately nondeterministic), the invariants
	 * below must always hold.
	 */
	@Test
	void concurrentEvictionAndHealthCheckDoNotCorruptIdleAccounting() throws Exception {
		int iterations = 300;
		int poolSize = 4;
		ExecutorService executor = Executors.newFixedThreadPool(2);
		try {
			for (int i = 0; i < iterations; i++) {
				AtomicInteger destroyCount = new AtomicInteger();
				List<String> destroyedPoolables = new CopyOnWriteArrayList<>();
				SimpleDequePool<String> pool = new SimpleDequePool<>(
						PoolBuilder.from(Mono.fromCallable(() -> "resource-" + System.nanoTime()))
						           .sizeBetween(poolSize, poolSize)
						           //evict everything: maximizes contention against the health check sweep below
						           .evictionPredicate((s, meta) -> true)
						           //always healthy: any ref the sweep grabs gets re-offered, contending with eviction
						           .healthCheck((s, meta) -> Mono.just(true))
						           .healthCheckParallelism(poolSize)
						           .destroyHandler(s -> Mono.fromRunnable(() -> {
							           destroyCount.incrementAndGet();
							           destroyedPoolables.add(s);
						           }))
						           .buildConfig());
				pool.warmup().block();
				assertThat(pool.idleSize()).isEqualTo(poolSize);

				CyclicBarrier barrier = new CyclicBarrier(2);
				Runnable runEviction = () -> {
					awaitBarrier(barrier);
					pool.evictInBackground();
				};
				Runnable runHealthCheck = () -> {
					awaitBarrier(barrier);
					pool.healthCheckInBackground();
				};

				Future<?> evictionFuture = executor.submit(runEviction);
				Future<?> healthCheckFuture = executor.submit(runHealthCheck);
				evictionFuture.get(5, TimeUnit.SECONDS);
				healthCheckFuture.get(5, TimeUnit.SECONDS);

				int finalIdleSize = pool.idleSize();
				int finalDestroyCount = destroyCount.get();

				assertThat(finalIdleSize).as("iteration %d: idleSize never goes negative", i).isGreaterThanOrEqualTo(0);
				assertThat(finalIdleSize + finalDestroyCount)
						.as("iteration %d: every resource is either still idle or destroyed exactly once, no leaks/no double-counting", i)
						.isEqualTo(poolSize);
				assertThat(destroyedPoolables)
						.as("iteration %d: no resource destroyed more than once", i)
						.doesNotHaveDuplicates();

				Deque<SimpleDequePool.QueuePooledRef<String>> remainingIdle = pool.idleResources;
				assertThat(remainingIdle).isNotNull();
				for (SimpleDequePool.QueuePooledRef<String> ref : remainingIdle) {
					assertThat(destroyedPoolables)
							.as("iteration %d: an already-destroyed resource must never reappear as idle/available", i)
							.doesNotContain(ref.poolable);
				}

				pool.disposeLater().block();
			}
		}
		finally {
			executor.shutdownNow();
		}
	}

	/**
	 * Sweep registration ({@code healthCheckSweepCandidates}/{@code healthCheckSweepSubscription}) is not
	 * linearized with {@code disposeLater()}: candidates are popped out of {@code idleResources} before those
	 * fields are set. If {@code disposeLater()} runs its entire snapshot-and-cleanup pass (reads
	 * {@code healthCheckSweepCandidates} as still null, detaches an {@code idleResources} that no longer
	 * contains the already-popped candidates) inside that gap, the popped refs would never be found by either
	 * cleanup path and leak forever if their check never resolves - since a second {@code disposeLater()} call
	 * is a no-op once the pool is already terminated.
	 * <p>
	 * Runs {@code healthCheckInBackground()} and {@code disposeLater()} from separate threads, synchronized via
	 * a {@link CyclicBarrier} to maximize the chance of {@code disposeLater()} landing inside the registration
	 * gap, repeated over many iterations. Every resource must always end up destroyed exactly once - regardless
	 * of which thread "wins" - never left permanently unaccounted for.
	 */
	@Test
	void disposeDuringSweepRegistrationNeverLeaksTheCandidates() throws Exception {
		int iterations = 2000;
		int poolSize = 4;
		ExecutorService executor = Executors.newFixedThreadPool(2);
		try {
			for (int i = 0; i < iterations; i++) {
				List<String> destroyedPoolables = new CopyOnWriteArrayList<>();
				SimpleDequePool<String> pool = new SimpleDequePool<>(
						PoolBuilder.from(Mono.fromCallable(() -> "resource-" + System.nanoTime()))
						           .sizeBetween(poolSize, poolSize)
						           //never resolves: maximizes the time a leaked ref would stay undestroyed,
						           //and keeps healthCheckSweepSubscription alive/cancellable for longer
						           .healthCheck((s, meta) -> Mono.never())
						           .healthCheckParallelism(poolSize)
						           .destroyHandler(s -> Mono.fromRunnable(() -> destroyedPoolables.add(s)))
						           .buildConfig());
				pool.warmup().block();
				assertThat(pool.idleSize()).isEqualTo(poolSize);

				CyclicBarrier barrier = new CyclicBarrier(2);
				Runnable runHealthCheck = () -> {
					awaitBarrier(barrier);
					pool.healthCheckInBackground();
				};
				Runnable runDispose = () -> {
					awaitBarrier(barrier);
					pool.disposeLater().block();
				};

				Future<?> healthCheckFuture = executor.submit(runHealthCheck);
				Future<?> disposeFuture = executor.submit(runDispose);
				healthCheckFuture.get(5, TimeUnit.SECONDS);
				disposeFuture.get(5, TimeUnit.SECONDS);

				assertThat(destroyedPoolables)
						.as("iteration %d: every resource destroyed exactly once, none leaked by racing sweep registration against dispose", i)
						.hasSize(poolSize)
						.doesNotHaveDuplicates();
			}
		}
		finally {
			executor.shutdownNow();
		}
	}

	private static void awaitBarrier(CyclicBarrier barrier) {
		try {
			barrier.await(5, TimeUnit.SECONDS);
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
