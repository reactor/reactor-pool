/*
 * Copyright (c) 2021 VMware Inc. or its affiliates, All Rights Reserved.
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;
import reactor.pool.InstrumentedPool;
import reactor.pool.Pool;
import reactor.pool.PoolShutdownException;
import reactor.pool.PooledRef;
import reactor.pool.PooledRefMetadata;
import reactor.util.Logger;
import reactor.util.Loggers;

/**
 * A decorating {@link InstrumentedPool} that adds the capacity to  {@link #gracefulShutdown(Duration) gracefully shut down} the pool.
 * Apply to any {@link InstrumentedPool} via the {@link #decorate(InstrumentedPool)} factory method.
 * <p>
 * Adds the {@link #getOriginalPool()}, {@link #gracefulShutdown(Duration)}, {@link #isGracefullyShuttingDown()} and {@link #isInGracePeriod()} methods.
 *
 * @author Simon Baslé
 */
public final class GracefulShutdownInstrumentedPool<T> implements InstrumentedPool<T> {

	private static final Logger LOGGER = Loggers.getLogger(GracefulShutdownInstrumentedPool.class);

	public static <T> GracefulShutdownInstrumentedPool<T> decorate(InstrumentedPool<T> originalPool) {
		return new GracefulShutdownInstrumentedPool<>(originalPool);
	}

	private final InstrumentedPool<T> originalPool;
	private final AtomicLong          acquireTracker;
	private final AtomicInteger       isGracefulShutdown;

	private final Sinks.Empty<Void> gracefulNotifier;

	private Disposable timeout;

	GracefulShutdownInstrumentedPool(InstrumentedPool<T> originalPool) {
		this.originalPool = originalPool;
		this.acquireTracker = new AtomicLong();
		this.isGracefulShutdown = new AtomicInteger();
		this.gracefulNotifier = Sinks.empty();

		//worst case scenario, if releases end up disposing this one instead of the real timeout one,
		//then the CAS inside the timeout task will prevent double shutdown anyway.
		this.timeout = Disposables.single();
	}

	/**
	 * Return the original pool.
	 * @return the original {@link InstrumentedPool}
	 */
	public InstrumentedPool<T> getOriginalPool() {
		return this.originalPool;
	}

	@Override
	public Mono<PooledRef<T>> acquire() {
		if (isGracefulShutdown.get() > 0) {
			return Mono.error(new PoolShutdownException("The pool is being gracefully shut down and won't accept new acquire calls"));
		}
		else {
			return Mono.defer(() -> {
				acquireTracker.incrementAndGet();
				return originalPool
					.acquire()
					//accommodate for the fact that the underlying pool might reject the acquire itself, or it could be cancelled
					.doFinally(st -> {
						if (st == SignalType.ON_ERROR || st == SignalType.CANCEL) {
							acquireTracker.decrementAndGet();
						}
					})
					//wrap the PooledRef so that we detect releases
					.map(GracefulRef::new);
			});
		}
	}

	@Override
	public Mono<PooledRef<T>> acquire(Duration timeout) {
		if (isGracefulShutdown.get() > 0) {
			return Mono.error(new PoolShutdownException("The pool is being gracefully shut down and won't accept new acquire calls"));
		}
		else {
			return Mono.defer(() -> {
				acquireTracker.incrementAndGet();
				return originalPool
					.acquire(timeout)
					//accommodate for the fact that the underlying pool might reject the acquire itself, or it could be cancelled
					.doFinally(st -> {
						if (st == SignalType.ON_ERROR || st == SignalType.CANCEL) {
							acquireTracker.decrementAndGet();
						}
					})
					//wrap the PooledRef so that we detect releases
					.map(GracefulRef::new);
			});
		}
	}

	/**
	 * Trigger a "graceful shutdown" of the pool, with a grace period timeout.
	 * From there on, calls to {@link Pool#acquire()} and {@link Pool#acquire(Duration)} will
	 * fail fast with a {@link PoolShutdownException}.
	 * However, for the provided {@link Duration}, pending acquires will get a chance to be served.
	 * <p>
	 * If the wrapper detects that all pending acquires are either {@link PooledRef#release() released}
	 * or {@link PooledRef#invalidate() invalidated}, the returned {@link Mono} will complete successfully.
	 * It will do so after having internally called and waited for the original pool's {@link Pool#disposeLater()} method,
	 * effectively shutting down the pool for good.
	 * <p>
	 * If the timeout triggers before that, the returned {@link Mono} will also trigger the {@link Pool#disposeLater()} method,
	 * but will terminate by emitting a {@link TimeoutException}. Since it means that at that point some pending acquire are
	 * still registered, these are terminated with a {@link PoolShutdownException} by the {@link #disposeLater()} method.
	 * <p>
	 * Note that the rejection of new acquires and the grace timer start immediately, irrespective of subscription to the
	 * returned {@link Mono}. Subsequent calls return the same {@link Mono}, effectively getting notifications from the first graceful shutdown
	 * call and ignoring subsequently provided timeouts.
	 *
	 * @param gracefulTimeout the maximum {@link Duration} for graceful shutdown before full shutdown is forced (resolution: ms)
	 *
	 * @return a {@link Mono} representing the current graceful shutdown of the pool
	 *
	 * @see #disposeLater()
	 */
	public Mono<Void> gracefulShutdown(final Duration gracefulTimeout) {
		if (isGracefulShutdown.compareAndSet(0, 1)) {
			//implement a timer that will trigger if not all released within the provided Duration
			timeout = Schedulers.boundedElastic()
				.schedule(() -> {
						if (isGracefulShutdown.compareAndSet(1, 2)) {
							//pending acquires haven't yet all been released, timing out
							originalPool
								.disposeLater()
								.subscribeOn(Schedulers.boundedElastic())
								.doFinally(st -> gracefulNotifier.emitError(
									new TimeoutException("Pool has forcefully shut down after graceful timeout of " + gracefulTimeout),
									Sinks.EmitFailureHandler.FAIL_FAST))
								.subscribe(v -> {
									},
									timedOutError -> LOGGER.warn("Error during the graceful shutdown upon graceful timeout", timedOutError));
						}
					},
					gracefulTimeout.toMillis(),
					TimeUnit.MILLISECONDS
				);
			//from there on, acquire() calls will get rejected
		}
		return gracefulNotifier.asMono();
	}

	/**
	 * Check if the {@link #gracefulShutdown(Duration)} has been invoked.
	 *
	 * @return true if the pool is in the process of shutting down gracefully, or has already done so
	 */
	public boolean isGracefullyShuttingDown() {
		return isGracefulShutdown.get() > 0;
	}

	/**
	 * Check if the {@link #gracefulShutdown(Duration)} has been invoked but there are still
	 * pending acquire and the grace period hasn't timed out.
	 * <p>
	 * If {@link #isGracefullyShuttingDown()} returns true but this method returns false,
	 * it means that the pool is now at least in the process of shutting down completely via
	 * {@link #disposeLater()} (or has already done so).
	 *
	 * @return true if the graceful shutdown is still within the grace period, false otherwise
	 */
	public boolean isInGracePeriod() {
		return isGracefulShutdown.get() == 1;
	}

	private Mono<Void> tryGracefulDone() {
		if (isGracefulShutdown.compareAndSet(1, 2)) {
			//the timeout hasn't come into play. cancel it for good measure
			timeout.dispose();
			return originalPool.disposeLater()
				.doFinally(st -> gracefulNotifier.emitEmpty(Sinks.EmitFailureHandler.FAIL_FAST));
		}
		return Mono.empty();
	}

	@Override
	public PoolMetrics metrics() {
		return originalPool.metrics();
	}

	@Override
	public Mono<Integer> warmup() {
		return originalPool.warmup();
	}

	@Override
	public Mono<Void> disposeLater() {
		return originalPool.disposeLater();
	}


	private class GracefulRef implements PooledRef<T> {

		final PooledRef<T> originalRef;

		public GracefulRef(PooledRef<T> originalRef) {
			this.originalRef = originalRef;
		}

		@Override
		public T poolable() {
			return originalRef.poolable();
		}

		@Override
		public PooledRefMetadata metadata() {
			return originalRef.metadata();
		}

		@Override
		public Mono<Void> invalidate() {
			return Mono.defer(() -> {
				long remaining = acquireTracker.decrementAndGet();
				if (remaining > 0) {
					return originalRef.invalidate();
				}
				else if (remaining == 0) {
					return originalRef.invalidate()
						.then(Mono.defer(GracefulShutdownInstrumentedPool.this::tryGracefulDone));
				}
				else {
					return Mono.empty();
				}
			});
		}

		@Override
		public Mono<Void> release() {
			return Mono.defer(() -> {
				long remaining = acquireTracker.decrementAndGet();
				if (remaining > 0) {
					return originalRef.release();
				}
				else if (remaining == 0) {
					return originalRef.release()
						.then(Mono.defer(GracefulShutdownInstrumentedPool.this::tryGracefulDone));
				}
				else {
					return Mono.empty();
				}
			});
		}
	}
}
