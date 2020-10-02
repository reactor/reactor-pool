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
import java.util.Deque;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.Loggers;
import reactor.util.annotation.Nullable;

/**
 * The {@link SimpleDequePool} is based on {@link Deque} for idle resources and pending {@link Pool#acquire()} Monos,
 * allowing both to be ordered either LIFO or FIFO.
 * It uses non-blocking drain loops to deliver resources to borrowers, which means that a resource could
 * be handed off on any of the following {@link Thread threads}:
 * <ul>
 *     <li>any thread on which a resource was last allocated</li>
 *     <li>any thread on which a resource was recently released</li>
 *     <li>any thread on which an {@link Pool#acquire()} {@link Mono} was subscribed</li>
 * </ul>
 * For a more deterministic approach, the {@link PoolBuilder#acquisitionScheduler(Scheduler)} property of the builder can be used.
 *
 * @author Simon Baslé
 */
public class SimpleDequePool<POOLABLE> extends AbstractPool<POOLABLE> {

	@SuppressWarnings("rawtypes")
	private static final ConcurrentLinkedDeque TERMINATED = new ConcurrentLinkedDeque();

	final boolean idleResourceLeastRecentlyUsed;
	final boolean pendingBorrowerFirstInFirstServed;

	volatile Deque<QueuePooledRef<POOLABLE>> idleResources;
	@SuppressWarnings("rawtypes")
	protected static final AtomicReferenceFieldUpdater<SimpleDequePool, Deque> IDLE_RESOURCES =
			AtomicReferenceFieldUpdater.newUpdater(SimpleDequePool.class, Deque.class, "idleResources");

	volatile             int                                        acquired;
	@SuppressWarnings("rawtypes")
	private static final AtomicIntegerFieldUpdater<SimpleDequePool> ACQUIRED =
			AtomicIntegerFieldUpdater.newUpdater(SimpleDequePool.class, "acquired");

	volatile             int                                        wip;
	@SuppressWarnings("rawtypes")
	private static final AtomicIntegerFieldUpdater<SimpleDequePool> WIP =
			AtomicIntegerFieldUpdater.newUpdater(SimpleDequePool.class, "wip");

	volatile             ConcurrentLinkedDeque<Borrower<POOLABLE>> pending;
	@SuppressWarnings("rawtypes")
	private static final AtomicReferenceFieldUpdater<SimpleDequePool, ConcurrentLinkedDeque> PENDING =
			AtomicReferenceFieldUpdater.newUpdater(SimpleDequePool.class, ConcurrentLinkedDeque.class, "pending");

	SimpleDequePool(PoolConfig<POOLABLE> poolConfig, boolean pendingBorrowerFirstInFirstServed) {
		super(poolConfig, Loggers.getLogger(SimpleDequePool.class));
		this.idleResourceLeastRecentlyUsed = poolConfig.reuseIdleResourcesInLruOrder();
		this.pendingBorrowerFirstInFirstServed = pendingBorrowerFirstInFirstServed;
		this.pending = new ConcurrentLinkedDeque<>(); //unbounded
		this.idleResources = new ConcurrentLinkedDeque<>();
	}

	@Override
	public Mono<PooledRef<POOLABLE>> acquire() {
		return new QueueBorrowerMono<>(this,
				Duration.ZERO); //the mono is unknown to the pool until requested
	}

	@Override
	public Mono<PooledRef<POOLABLE>> acquire(Duration timeout) {
		return new QueueBorrowerMono<>(this,
				timeout); //the mono is unknown to the pool until requested
	}

	@Override
	public int acquiredSize() {
		return acquired;
	}

	@Override
	public Mono<Void> disposeLater() {
		return Mono.defer(() -> {
			@SuppressWarnings("unchecked") ConcurrentLinkedDeque<Borrower<POOLABLE>> q =
					PENDING.getAndSet(this, TERMINATED);
			if (q != TERMINATED) {
				Borrower<POOLABLE> p;
				while ((p = q.pollFirst()) != null) {
					p.fail(new PoolShutdownException());
				}

				@SuppressWarnings("unchecked")
				Queue<QueuePooledRef<POOLABLE>> e =
						IDLE_RESOURCES.getAndSet(this, null);
				if (e != null) {
					Mono<Void> destroyMonos = Mono.empty();
					while (!e.isEmpty()) {
						QueuePooledRef<POOLABLE> ref = e.poll();
						if (ref.markInvalidate()) {
							destroyMonos = destroyMonos.and(destroyPoolable(ref));
						}
					}
					return destroyMonos;
				}
			}
			return Mono.empty();
		});
	}

	@Override
	public int idleSize() {
		Queue<?> e = IDLE_RESOURCES.get(this);
		return e == null ? 0 : e.size();
	}

	@Override
	public Mono<Integer> warmup() {
		if (poolConfig.allocationStrategy()
		              .permitMinimum() > 0) {
			return Mono.defer(() -> {
				int initSize = poolConfig.allocationStrategy()
				                         .getPermits(0);
				@SuppressWarnings({ "unchecked", "rawtypes" }) //rawtypes added since javac actually complains
				Mono<POOLABLE>[] allWarmups = new Mono[initSize];
				for (int i = 0; i < initSize; i++) {
					long start = clock.millis();
					allWarmups[i] = poolConfig.allocator()
					                          .doOnNext(p -> {
						                          metricsRecorder.recordAllocationSuccessAndLatency(
								                          clock.millis() - start);
						                          //the pool slot won't access this pool instance until after it has been constructed
						                          this.idleResources.offerLast(createSlot(p));
					                          })
					                          .doOnError(e -> {
						                          metricsRecorder.recordAllocationFailureAndLatency(
								                          clock.millis() - start);
						                          poolConfig.allocationStrategy()
						                                    .returnPermits(1);
					                          });
				}
				return Flux.concat(allWarmups)
				           .reduce(0, (count, p) -> count + 1);
			});
		}
		else {
			return Mono.just(0);
		}
	}

	@Override
	void cancelAcquire(Borrower<POOLABLE> borrower) {
		if (!isDisposed()) { //ignore pool disposed
			ConcurrentLinkedDeque<Borrower<POOLABLE>> q = this.pending;
			if (q.remove(borrower)) {
				PENDING_COUNT.decrementAndGet(this);
			}
		}
	}

	QueuePooledRef<POOLABLE> createSlot(POOLABLE element) {
		return new QueuePooledRef<>(this, element);
	}

	@Override
	void doAcquire(Borrower<POOLABLE> borrower) {
		if (isDisposed()) {
			borrower.fail(new PoolShutdownException());
			return;
		}

		pendingOffer(borrower);
		drain();
	}

	void drain() {
		if (WIP.getAndIncrement(this) == 0) {
			drainLoop();
		}
	}

	private void drainLoop() {
		for (; ; ) {
			@SuppressWarnings("unchecked")
			Deque<QueuePooledRef<POOLABLE>> irq = IDLE_RESOURCES.get(this);
			if (irq != null) {
				int availableCount = irq.size();
				int pendingCount = PENDING_COUNT.get(this);
				int estimatedPermitCount = poolConfig.allocationStrategy()
				                                     .estimatePermitCount();

				if (availableCount == 0) {
					if (pendingCount > 0 && estimatedPermitCount > 0) {
						final Borrower<POOLABLE> borrower = pendingPoll(); //shouldn't be null
						if (borrower == null) {
							continue;
						}
						ACQUIRED.incrementAndGet(this);
						int permits = poolConfig.allocationStrategy()
						                        .getPermits(1);
						if (borrower.get() || permits == 0) {
							ACQUIRED.decrementAndGet(this);
							continue;
						}
						borrower.stopPendingCountdown();
						long start = clock.millis();
						Mono<POOLABLE> allocator;
						Scheduler s = poolConfig.acquisitionScheduler();
						if (s != Schedulers.immediate()) {
							allocator = poolConfig.allocator()
							                      .publishOn(s);
						}
						else {
							allocator = poolConfig.allocator();
						}
						Mono<POOLABLE> primary = allocator.doOnEach(sig -> {
							if (sig.isOnNext()) {
								POOLABLE newInstance = sig.get();
								metricsRecorder.recordAllocationSuccessAndLatency(clock.millis() - start);
								borrower.deliver(createSlot(newInstance));
							}
							else if (sig.isOnError()) {
								metricsRecorder.recordAllocationFailureAndLatency(clock.millis() - start);
								ACQUIRED.decrementAndGet(this);
								poolConfig.allocationStrategy()
								          .returnPermits(1);
								borrower.fail(sig.getThrowable());
								drain();
							}
						});

						int toWarmup = permits - 1;
						if (toWarmup < 1) {
							primary.subscribe(alreadyPropagated -> { }, alreadyPropagatedOrLogged -> { });
						}
						else {
							logger.debug("should warm up {} extra resources", toWarmup);
							final long startWarmupIteration = clock.millis();
							final Mono<Void> warmup = Flux
									.range(1, toWarmup)
									.flatMap(i -> allocator
											.doOnNext(
													poolable -> {
														logger.debug("warmed up extra resource {}/{}", i, toWarmup);
														metricsRecorder.recordAllocationSuccessAndLatency(
																clock.millis() - startWarmupIteration);
														irq.offer(new QueuePooledRef<>(this, poolable));
														drain();
													})
											.onErrorResume(
													warmupError -> {
														logger.debug("failed to warm up extra resource {}/{}: {}", i, toWarmup,
																warmupError.toString());
														metricsRecorder.recordAllocationFailureAndLatency(
																clock.millis() - startWarmupIteration);
														//we return permits in case of warmup failure, but shouldn't further decrement ACQUIRED
														poolConfig.allocationStrategy().returnPermits(1);
														drain();
														return Mono.empty();
													}))
									.then();

							//we ignore errors from primary (already propagated), which allows us to attempt
							// the warmup in all cases. individual warmup failures decrement the permit and are logged
							primary.onErrorResume(ignore -> Mono.empty())
							       .thenMany(warmup)
							       //all errors and values are either propagated or logged, nothing to do
							       .subscribe(alreadyPropagated -> { }, alreadyPropagatedOrLogged -> { });
						}
					}
				}
				else if (pendingCount > 0) {
					if (isDisposed()) {
						continue;
					}
					//there are objects ready and unclaimed in the pool + a pending
					QueuePooledRef<POOLABLE> slot = idleResourceLeastRecentlyUsed ? irq.pollFirst() : irq.pollLast();
					if (slot == null) {
						continue;
					}

					if (poolConfig.evictionPredicate()
					              .test(slot.poolable, slot) && slot.markInvalidate()) {
						destroyPoolable(slot).subscribe(null,
								error -> drain(),
								this::drain);
						continue;
					}

					//there is a party currently pending acquiring
					Borrower<POOLABLE> inner = pendingPoll();
					if (inner == null) {
						if (!isDisposed()) {
							//put back at the same end
							if (idleResourceLeastRecentlyUsed) {
								irq.offerFirst(slot);
							}
							else {
								irq.offerLast(slot);
							}
						}
						continue;
					}
					inner.stopPendingCountdown();
					ACQUIRED.incrementAndGet(this);
					poolConfig.acquisitionScheduler()
					          .schedule(() -> inner.deliver(slot));
				}
			}

			if (WIP.decrementAndGet(this) == 0) {
				break;
			}
		}
	}

	@Override
	boolean elementOffer(POOLABLE element) {
		@SuppressWarnings("unchecked")
		Deque<QueuePooledRef<POOLABLE>> irq = IDLE_RESOURCES.get(this);
		if (irq == null) {
			return false;
		}
		return irq.offerLast(createSlot(element));
	}

	@SuppressWarnings("WeakerAccess")
	final void maybeRecycleAndDrain(QueuePooledRef<POOLABLE> poolSlot) {
		if (!isDisposed()) {
			if (!poolConfig.evictionPredicate()
			               .test(poolSlot.poolable, poolSlot)) {
				metricsRecorder.recordRecycled();
				@SuppressWarnings("unchecked")
				Deque<QueuePooledRef<POOLABLE>> irq = IDLE_RESOURCES.get(this);
				if (irq != null) {
					QueuePooledRef<POOLABLE> slot = recycleSlot(poolSlot);
					irq.offerLast(slot);
					drain();
					if (isDisposed() && slot.markInvalidate()) {
						destroyPoolable(slot).subscribe(); //TODO manage errors?
					}
					return;
				}
			}
		}
		if (poolSlot.markInvalidate()) {
			destroyPoolable(poolSlot).subscribe(null,
					e -> drain(),
					this::drain); //TODO manage errors?
		}
	}

	/**
	 * @param pending a new {@link reactor.pool.AbstractPool.Borrower} to register as pending
	 *
	 * @return true if the pool had capacity to register this new pending
	 */
	boolean pendingOffer(Borrower<POOLABLE> pending) {
		int maxPending = poolConfig.maxPending();
		for (; ; ) {
			int currentPending = PENDING_COUNT.get(this);
			if (maxPending >= 0 && currentPending == maxPending) {
				//best effort: check for idle and capacity
				Deque<QueuePooledRef<POOLABLE>> ir = this.idleResources;
				if (ir.isEmpty() && poolConfig.allocationStrategy().estimatePermitCount() == 0) {
					//fail fast. differentiate slightly special case of maxPending == 0
					if (maxPending == 0) {
						pending.fail(new PoolAcquirePendingLimitException(0, "No pending allowed and pool has reached allocation limit"));
					}
					else {
						pending.fail(new PoolAcquirePendingLimitException(maxPending));
					}
					return false;
				}
			}

			if (PENDING_COUNT.compareAndSet(this,
					currentPending,
					currentPending + 1)) {
				this.pending.offerLast(pending); //unbounded
				return true;
			}
		}
	}

	/**
	 * @return the next {@link reactor.pool.AbstractPool.Borrower} to serve
	 */
	@Nullable
	Borrower<POOLABLE> pendingPoll() {
		Deque<Borrower<POOLABLE>> pq = pending;
		Borrower<POOLABLE> b = this.pendingBorrowerFirstInFirstServed ?
				pq.pollFirst() :
				pq.pollLast();
		if (b != null) {
			PENDING_COUNT.decrementAndGet(this);
		}
		return b;
	}

	QueuePooledRef<POOLABLE> recycleSlot(
			QueuePooledRef<POOLABLE> slot) {
		return new QueuePooledRef<>(slot);
	}

	@Override
	public boolean isDisposed() {
		return PENDING.get(this) == TERMINATED;
	}




	static final class QueuePooledRef<T> extends AbstractPooledRef<T> {

		final SimpleDequePool<T> pool;

		QueuePooledRef(SimpleDequePool<T> pool, T poolable) {
			super(poolable, pool.metricsRecorder, pool.clock);
			this.pool = pool;
		}

		QueuePooledRef(QueuePooledRef<T> oldRef) {
			super(oldRef);
			this.pool = oldRef.pool;
		}

		@Override
		public Mono<Void> invalidate() {
			return Mono.defer(() -> {
				if (markInvalidate()) {
					//immediately clean up state
					ACQUIRED.decrementAndGet(pool);
					return pool.destroyPoolable(this)
					           .then(Mono.fromRunnable(pool::drain));
				}
				else {
					return Mono.empty();
				}
			});
		}

		@Override
		public Mono<Void> release() {
			return Mono.defer(() -> {
				if (STATE.get(this) == STATE_RELEASED) {
					return Mono.empty();
				}

				if (pool.isDisposed()) {
					ACQUIRED.decrementAndGet(pool); //immediately clean up state
					if (markInvalidate()) {
						return pool.destroyPoolable(this);
					}
					else {
						return Mono.empty();
					}
				}

				Publisher<Void> cleaner;
				try {
					cleaner = pool.poolConfig.releaseHandler()
					                         .apply(poolable);
				}
				catch (Throwable e) {
					ACQUIRED.decrementAndGet(pool); //immediately clean up state
					markReleased();
					return Mono.error(new IllegalStateException(
							"Couldn't apply cleaner function",
							e));
				}
				//the PoolRecyclerMono will wrap the cleaning Mono returned by the Function and perform state updates
				return new QueuePoolRecyclerMono<>(cleaner, this);
			});
		}
	}



	static final class QueueBorrowerMono<T> extends Mono<PooledRef<T>> {

		final SimpleDequePool<T> parent;
		final Duration           acquireTimeout;

		QueueBorrowerMono(SimpleDequePool<T> pool, Duration acquireTimeout) {
			this.parent = pool;
			this.acquireTimeout = acquireTimeout;
		}

		@Override
		public void subscribe(CoreSubscriber<? super PooledRef<T>> actual) {
			Objects.requireNonNull(actual, "subscribing with null");
			Borrower<T> borrower = new Borrower<>(actual, parent, acquireTimeout);
			actual.onSubscribe(borrower);
		}
	}

	private static final class QueuePoolRecyclerInner<T>
			implements CoreSubscriber<Void>, Scannable, Subscription {

		final CoreSubscriber<? super Void> actual;
		final SimpleDequePool<T>           pool;

		//poolable can be checked for null to protect against protocol errors
		QueuePooledRef<T> pooledRef;
		Subscription                      upstream;
		long                              start;

		//once protects against multiple requests
		volatile     int once;
		QueuePoolRecyclerInner(CoreSubscriber<? super Void> actual,
				QueuePooledRef<T> pooledRef) {
			this.actual = actual;
			this.pooledRef = Objects.requireNonNull(pooledRef, "pooledRef");
			this.pool = pooledRef.pool;
		}

		@Override
		public void cancel() {
			//NO-OP, once requested, release cannot be cancelled
		}

		@Override
		public void onComplete() {
			QueuePooledRef<T> slot = pooledRef;
			pooledRef = null;
			if (slot == null) {
				return;
			}

			//some operators might immediately produce without request (eg. fromRunnable)
			// we decrement ACQUIRED EXACTLY ONCE to indicate that the poolable was released by the user
			if (ONCE.compareAndSet(this, 0, 1)) {
				ACQUIRED.decrementAndGet(pool);
			}

			pool.metricsRecorder.recordResetLatency(pool.clock.millis() - start);

			pool.maybeRecycleAndDrain(slot);
			actual.onComplete();
		}

		@Override
		public void onError(Throwable throwable) {
			QueuePooledRef<T> slot = pooledRef;
			pooledRef = null;
			if (slot == null) {
				Operators.onErrorDropped(throwable, actual.currentContext());
				return;
			}

			//some operators might immediately produce without request (eg. fromRunnable)
			// we decrement ACQUIRED EXACTLY ONCE to indicate that the poolable was released by the user
			if (ONCE.compareAndSet(this, 0, 1)) {
				ACQUIRED.decrementAndGet(pool);
			}

			//TODO should we separate reset errors?
			pool.metricsRecorder.recordResetLatency(pool.clock.millis() - start);

			if (slot.markInvalidate()) {
				pool.destroyPoolable(slot)
				    .subscribe(null, null, pool::drain); //TODO manage errors?
			}

			actual.onError(throwable);
		}

		@Override
		public void onNext(Void o) {
			//N/A
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(upstream, s)) {
				this.upstream = s;
				actual.onSubscribe(this);
				this.start = pool.clock.millis();
			}
		}

		@Override
		public void request(long l) {
			if (Operators.validate(l)) {
				upstream.request(l);
				// we decrement ACQUIRED EXACTLY ONCE to indicate that the poolable was released by the user
				if (ONCE.compareAndSet(this, 0, 1)) {
					ACQUIRED.decrementAndGet(pool);
				}
			}
		}

		@Override
		@SuppressWarnings("rawtypes")
		public Object scanUnsafe(Scannable.Attr key) {
			if (key == Attr.ACTUAL) {
				return actual;
			}
			if (key == Attr.PARENT) {
				return upstream;
			}
			if (key == Attr.CANCELLED) {
				return false;
			}
			if (key == Attr.TERMINATED) {
				return pooledRef == null;
			}
			if (key == Attr.BUFFERED) {
				return (pooledRef == null) ? 0 : 1;
			}
			return null;
		}
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<QueuePoolRecyclerInner> ONCE =
				AtomicIntegerFieldUpdater.newUpdater(QueuePoolRecyclerInner.class, "once");
	}

	private static final class QueuePoolRecyclerMono<T> extends Mono<Void>
			implements Scannable {

		final Publisher<Void>                                    source;
		final AtomicReference<QueuePooledRef<T>> slotRef;

		QueuePoolRecyclerMono(Publisher<Void> source, QueuePooledRef<T> poolSlot) {
			this.source = source;
			this.slotRef = new AtomicReference<>(poolSlot);
		}

		@Override
		@Nullable
		@SuppressWarnings("rawtypes")
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PREFETCH) {
				return Integer.MAX_VALUE;
			}
			if (key == Attr.PARENT) {
				return source;
			}
			return null;
		}

		@Override
		public void subscribe(CoreSubscriber<? super Void> actual) {
			QueuePooledRef<T> slot = slotRef.getAndSet(null);
			if (slot == null || !slot.markReleased()) {
				Operators.complete(actual);
			}
			else {
				QueuePoolRecyclerInner<T> qpr =
						new QueuePoolRecyclerInner<>(actual, slot);
				source.subscribe(qpr);
			}
		}
	}
}
