/*
 * Copyright (c) 2018-2024 VMware Inc. or its affiliates, All Rights Reserved.
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
import java.util.Iterator;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiPredicate;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.Exceptions;
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

	volatile Deque<QueuePooledRef<POOLABLE>> idleResources;
	@SuppressWarnings("rawtypes")
	protected static final AtomicReferenceFieldUpdater<SimpleDequePool, Deque> IDLE_RESOURCES =
			AtomicReferenceFieldUpdater.newUpdater(SimpleDequePool.class, Deque.class, "idleResources");

	volatile             int                                        acquired;
	@SuppressWarnings("rawtypes")
	private static final AtomicIntegerFieldUpdater<SimpleDequePool> ACQUIRED =
			AtomicIntegerFieldUpdater.newUpdater(SimpleDequePool.class, "acquired");

	//TODO rework into a STATE that uses 31 bits for wip guard and 1 bit for TERMINATED, make queues final and not volatile
	volatile             int                                        wip;
	@SuppressWarnings("rawtypes")
	private static final AtomicIntegerFieldUpdater<SimpleDequePool> WIP =
			AtomicIntegerFieldUpdater.newUpdater(SimpleDequePool.class, "wip");

	volatile             ConcurrentLinkedDeque<Borrower<POOLABLE>> pending;
	@SuppressWarnings("rawtypes")
	private static final AtomicReferenceFieldUpdater<SimpleDequePool, ConcurrentLinkedDeque> PENDING =
			AtomicReferenceFieldUpdater.newUpdater(SimpleDequePool.class, ConcurrentLinkedDeque.class, "pending");

	volatile int pendingSize;
	@SuppressWarnings("rawtypes")
	private static final AtomicIntegerFieldUpdater<SimpleDequePool> PENDING_SIZE =
		AtomicIntegerFieldUpdater.newUpdater(SimpleDequePool.class, "pendingSize");

	volatile int idleSize;
	@SuppressWarnings("rawtypes")
	private static final AtomicIntegerFieldUpdater<SimpleDequePool> IDLE_SIZE =
		AtomicIntegerFieldUpdater.newUpdater(SimpleDequePool.class, "idleSize");

	Disposable evictionTask;

	SimpleDequePool(PoolConfig<POOLABLE> poolConfig) {
		super(poolConfig, Loggers.getLogger(SimpleDequePool.class));
		this.idleResourceLeastRecentlyUsed = poolConfig.reuseIdleResourcesInLruOrder();
		this.pending = new ConcurrentLinkedDeque<>(); //unbounded
		this.idleResources = new ConcurrentLinkedDeque<>();
		recordInteractionTimestamp();

		scheduleEviction();
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
	public boolean transferBorrowersFrom(InstrumentedPool<POOLABLE> fromPool) {
		SimpleDequePool<POOLABLE> other = (SimpleDequePool<POOLABLE>) fromPool;

		if (!other.isDisposed()) {
			ConcurrentLinkedDeque<Borrower<POOLABLE>> q = other.PENDING.get(other);
			if (q == TERMINATED) {
				return false;
			}
			Borrower<POOLABLE> b = other.pendingPoll(q);
			if (b != null && !b.get()) {
				// TODO check race conditions when timer expires or subscription is cancelled concurrently !
				b.setPool(this);
				doAcquire(b);
				return true;
			}
		}

		return false;
	}

	@Override
	public int acquiredSize() {
		return acquired;
	}


	void scheduleEviction() {
		if (!poolConfig.evictInBackgroundInterval().isZero()) {
			long nanosEvictionInterval = poolConfig.evictInBackgroundInterval().toNanos();
			this.evictionTask = poolConfig.evictInBackgroundScheduler().schedule(this::evictInBackground, nanosEvictionInterval, TimeUnit.NANOSECONDS);
		}
		else {
			this.evictionTask = Disposables.disposed();
		}
	}

	void evictInBackground() {
		@SuppressWarnings("unchecked")
		Queue<QueuePooledRef<POOLABLE>> e = IDLE_RESOURCES.get(this);
		if (e == null) {
			//no need to schedule the task again, pool has been disposed
			return;
		}

		if (WIP.getAndIncrement(this) == 0) {
			if (pendingSize == 0) {
				BiPredicate<POOLABLE, PooledRefMetadata> evictionPredicate = poolConfig.evictionPredicate();
				//only one evictInBackground can enter here, and it won vs `drain` calls
				//let's "purge" the pool
				Iterator<QueuePooledRef<POOLABLE>> iterator = e.iterator();
				while (iterator.hasNext()) {
					QueuePooledRef<POOLABLE> pooledRef = iterator.next();
					if (evictionPredicate.test(pooledRef.poolable, pooledRef)) {
						//note that usually a manually released ref will put a new ref in the queue, so we expect refs to be pristine here
						if (pooledRef.markDestroy()) {
							recordInteractionTimestamp();
							iterator.remove();
							decrementIdle();
							destroyPoolable(pooledRef).subscribe(v -> {},
									destroyError -> this.logger.warn("Error while destroying resource in background eviction:", destroyError));
						}
					}
				}
			}
			//at the end if there are racing drain calls, go into the drainLoop
			if (WIP.decrementAndGet(this) > 0) {
				drainLoop();
			}
		}
		//schedule the next iteration
		scheduleEviction();
	}

	@Override
	public Mono<Void> disposeLater() {
		return Mono.defer(() -> {
			recordInteractionTimestamp();

			//TODO mark the termination differently and handle Borrower.fail inside drainLoop
			//TODO also IDLE_RESOURCE
			//to make it truly MPSC

			@SuppressWarnings("unchecked") ConcurrentLinkedDeque<Borrower<POOLABLE>> q =
					PENDING.getAndSet(this, TERMINATED);
			if (q != TERMINATED) {
				//stop reaper thread
				this.evictionTask.dispose();

				Borrower<POOLABLE> p;
				while ((p = q.pollFirst()) != null) {
					p.fail(new PoolShutdownException());
				}
				//unlike IDLE_SIZE below, there should be no competing decrementAndGet, so safe to set to 0 in one go
				PENDING_SIZE.set(this, 0);

				@SuppressWarnings("unchecked")
				Queue<QueuePooledRef<POOLABLE>> e =
						IDLE_RESOURCES.getAndSet(this, null);
				if (e != null) {
					Mono<Void> destroyMonos = Mono.empty();
					while (!e.isEmpty()) {
						QueuePooledRef<POOLABLE> ref = e.poll();
						if (ref.markDestroy()) {
							decrementIdle();
							destroyMonos = destroyMonos.and(destroyPoolable(ref));
						}
					}
					//we're not setting IDLE_SIZE to zero here on purpose, as competing
					//markDestroy might happen concurrently which would bring it below 0
					return destroyMonos;
				}
			}
			return Mono.empty(); //TODO subsequent calls misleading: destroying might still be in progress yet second subscriber sees immediate termination
		});
	}

	@Override
	public int idleSize() {
		return idleSize;
	}

	@Override
	public Mono<Integer> warmup() {
		if (poolConfig.allocationStrategy()
		              .permitMinimum() > 0) {
			return Mono.deferContextual(ctx -> {
				recordInteractionTimestamp();
				int initSize = poolConfig.allocationStrategy()
				                         .getPermits(0);
				if (initSize <= 0) {
					return Mono.just(0);
				}
				@SuppressWarnings({ "unchecked", "rawtypes" }) //rawtypes added since javac actually complains
				Mono<POOLABLE>[] allWarmups = new Mono[initSize];
				for (int i = 0; i < initSize; i++) {
					long start = clock.millis();
					allWarmups[i] = poolConfig.allocator()
					                          .contextWrite(ctx)
					                          .doOnNext(p -> {
						                          metricsRecorder.recordAllocationSuccessAndLatency(
								                          clock.millis() - start);
						                          //the pool slot won't access this pool instance until after it has been constructed
												  incrementIdle();
						                          this.idleResources.offerLast(createSlot(p));
					                          })
					                          .doOnError(e -> {
						                          metricsRecorder.recordAllocationFailureAndLatency(
								                          clock.millis() - start);
						                          poolConfig.allocationStrategy()
						                                    .returnPermits(1);
					                          });
				}
				// merge will eagerly subscribe to all warmups from the current thread, but
				// the parallelism can be controlled from configuration.
				int mergeConcurrency = Math.min(poolConfig.allocationStrategy().warmupParallelism(), allWarmups.length);
				return Flux.merge(Flux.fromArray(allWarmups), mergeConcurrency)
				           .reduce(0, (count, p) -> count + 1);
			});
		}
		else {
			return Mono.just(0);
		}
	}

	void decrementIdle() {
		if (IDLE_SIZE.decrementAndGet(this) < 0) {
			System.err.println("unexpected decrement below 0");
		}
	}

	void incrementIdle() {
		if (IDLE_SIZE.incrementAndGet(this) == 0) {
			System.err.println("unexpected increment from below 0");
		}
	}

	@Override
	void cancelAcquire(Borrower<POOLABLE> borrower) {
		if (!isDisposed()) { //ignore pool disposed
			ConcurrentLinkedDeque<Borrower<POOLABLE>> q = this.pending;
			if (q.remove(borrower)) {
				PENDING_SIZE.decrementAndGet(this);
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

	boolean drain() {
		if (WIP.getAndIncrement(this) == 0) {
			drainLoop();
			return true;
		}
		return false;
	}

	private void drainLoop() {
		//we mark the interaction timestamp twice (beginning and end) rather than continuously on each iteration
		recordInteractionTimestamp();
		int maxPending = poolConfig.maxPending();

		for (;;) {
			@SuppressWarnings("unchecked")
			Deque<QueuePooledRef<POOLABLE>> resources = IDLE_RESOURCES.get(this);
			@SuppressWarnings("unchecked")
			ConcurrentLinkedDeque<Borrower<POOLABLE>> borrowers = PENDING.get(this);
			if (resources == null || borrowers == TERMINATED) {
				//null queue indicates a terminated pool
				return;
			}

			int borrowersCount = pendingSize;
			int resourcesCount = idleSize;

			if (borrowersCount == 0) {
				/*=========================================*
				 * No Pending: Nothing to do *
				 *=========================================*/
				//TODO in 0.2.x we might want to warm up here too
			}
			else {
				if (resourcesCount > 0) {
					/*===================================================*
					 * MATCH: one PENDING Borrower can get IDLE resource *
					 *===================================================*/
					//get the resource
					QueuePooledRef<POOLABLE> slot = idleResourceLeastRecentlyUsed ? resources.pollFirst() : resources.pollLast();
					if (slot == null) {
						continue;
					}
					decrementIdle();
					//check it is still valid
					if (poolConfig.evictionPredicate().test(slot.poolable, slot)) {
						if (slot.markDestroy()) {
							destroyPoolable(slot).subscribe(null,
									error -> drain(),
									this::drain);
						}
						continue;
					}
					Borrower<POOLABLE> borrower = pendingPoll(borrowers);
					if (borrower == null) {
						if (idleResourceLeastRecentlyUsed) {
							resources.offerFirst(slot);
						}
						else {
							resources.offerLast(slot);
						}
						incrementIdle();
						//we expect to detect a disposed pool in the next round
						continue;
					}
					if (isDisposed()) {
						slot.invalidate().subscribe(); //TODO double check this is enough
						borrower.fail(new PoolShutdownException());
						return;
					}
					borrower.stopPendingCountdown(true);
					ACQUIRED.incrementAndGet(this);
					poolConfig.acquisitionScheduler()
					          .schedule(() -> borrower.deliver(slot));
				}
				else {
					/*==================================*
					 * One Borrower, but NO RESOURCE... *
					 *==================================*/
					// Can we allocate more?
					int permits = poolConfig.allocationStrategy().getPermits(1);
					if (permits <= 0) {
						/*==========================*
						 * ... and CANNOT ALLOCATE  => MAX PENDING ENFORCING *
						 *==========================*/
						//we don't have idle resource nor allocation permit
						//we look at the borrowers and cull those that are above the maxPending limit (using pollLast!)
						if (maxPending >= 0) {
							borrowersCount = pendingSize;
							int toCull = borrowersCount - maxPending;
							for (int i = 0; i < toCull; i++) {
								Borrower<POOLABLE> extraneous = pendingPoll(borrowers);
								if (extraneous != null) {
									//fail fast. differentiate slightly special case of maxPending == 0
									if (maxPending == 0) {
										extraneous.fail(new PoolAcquirePendingLimitException(0, "No pending allowed and pool has reached allocation limit"));
									}
									else {
										extraneous.fail(new PoolAcquirePendingLimitException(maxPending));
									}
								}
							}
						}
					}
					else {
						/*=======================*
						 * ... and CAN ALLOCATE  => Subscribe to allocator + Warmup *
						 *=======================*/
						Borrower<POOLABLE> borrower = pendingPoll(borrowers);
						if (borrower == null) {
							continue; //we expect to detect pool is shut down in next round
						}
						if (isDisposed()) {
							borrower.fail(new PoolShutdownException());
							return;
						}
						borrower.stopPendingCountdown(true);
						long start = clock.millis();
						Mono<POOLABLE> allocator = allocatorWithScheduler();

						Mono<POOLABLE> primary = allocator.doOnEach(sig -> {
							if (sig.isOnNext()) {
								POOLABLE newInstance = sig.get();
								assert newInstance != null;
								ACQUIRED.incrementAndGet(this);
								metricsRecorder.recordAllocationSuccessAndLatency(clock.millis() - start);
								borrower.deliver(createSlot(newInstance));
							}
							else if (sig.isOnError()) {
								Throwable error = sig.getThrowable();
								assert error != null;
								metricsRecorder.recordAllocationFailureAndLatency(clock.millis() - start);
								poolConfig.allocationStrategy()
								          .returnPermits(1);
								borrower.fail(error);
							}
						}).contextWrite(borrower.currentContext());

						if (permits == 1) {
							//subscribe to the primary, which will directly feed to the borrower
							primary.subscribe(alreadyPropagated -> { }, alreadyPropagatedOrLogged -> drain(), this::drain);
						}
						else {
							/*=============================================*
							 * (warm up in sequence to primary allocation) *
							 *=============================================*/
							int toWarmup = permits - 1;
							logger.debug("should warm up {} extra resources", toWarmup);

							final long startWarmupIteration = clock.millis();
							// flatMap will eagerly subscribe to the allocator from the current thread, but the concurrency
							// can be controlled from configuration
							final int mergeConcurrency = Math.min(poolConfig.allocationStrategy().warmupParallelism(), toWarmup + 1);
							Flux.range(1, toWarmup)
									.map(i -> warmupMono(i, toWarmup, startWarmupIteration, allocator).doOnSuccess(__ -> drain()))
									.startWith(primary.doOnSuccess(__ -> drain()).onErrorComplete().then())
									.flatMap(Function.identity(), mergeConcurrency, 1) // since we dont store anything the inner buffer can be simplified
									.onErrorResume(e -> Mono.empty())
									.subscribe(aVoid -> { }, alreadyPropagatedOrLogged -> drain(), this::drain);
						}
					}
				}
			}

			if (WIP.decrementAndGet(this) == 0) {
				recordInteractionTimestamp();
				break;
			}
		}
	}

	private Mono<POOLABLE> allocatorWithScheduler() {
		Scheduler s = poolConfig.acquisitionScheduler();
		if (s != Schedulers.immediate()) {
			return poolConfig.allocator().publishOn(s);
		}
		return poolConfig.allocator();
	}

	Mono<Void> warmupMono(int index, int max, long startWarmupIteration, Mono<POOLABLE> allocator) {
		return allocator.flatMap(poolable -> {
			logger.debug("warmed up extra resource {}/{}", index, max);
			metricsRecorder.recordAllocationSuccessAndLatency(
					clock.millis() - startWarmupIteration);
			if (!elementOffer(poolable)) {
				//destroyPoolable will correctly return permit and won't decrement ACQUIRED, unlike invalidate
				//BUT: it requires a PoolRef that is marked as invalidated
				QueuePooledRef<POOLABLE> tempRef = createSlot(poolable);
				tempRef.markDestroy();
				return destroyPoolable(tempRef);
			}
			return Mono.empty();
		}).onErrorResume(warmupError -> {
			logger.debug("failed to warm up extra resource {}/{}: {}", index, max,
					warmupError.toString());
			metricsRecorder.recordAllocationFailureAndLatency(
					clock.millis() - startWarmupIteration);
			//we return permits in case of warmup failure, but shouldn't further decrement ACQUIRED
			poolConfig.allocationStrategy().returnPermits(1);
			return Mono.empty();
		});
		//draining will be triggered again at the end of the warmup execution
	}

	@Override
	public int pendingAcquireSize() {
		return pendingSize;
	}

	@Override
	boolean elementOffer(POOLABLE element) {
		@SuppressWarnings("unchecked")
		Deque<QueuePooledRef<POOLABLE>> irq = IDLE_RESOURCES.get(this);
		if (irq == null) {
			return false;
		}
		if (irq.offer(createSlot(element))) {
			incrementIdle();
			poolConfig.resourceManager().resourceAvailable();
			return true;
		}
		return false;
	}

	@SuppressWarnings("WeakerAccess")
	final void maybeRecycleAndDrain(QueuePooledRef<POOLABLE> poolSlot, CoreSubscriber<? super Void> actual) {
		if (!isDisposed()) {
			recordInteractionTimestamp();
			if (!poolConfig.evictionPredicate()
			               .test(poolSlot.poolable, poolSlot)) {
				metricsRecorder.recordRecycled();
				@SuppressWarnings("unchecked")
				Deque<QueuePooledRef<POOLABLE>> irq = IDLE_RESOURCES.get(this);
				if (irq != null) {
					QueuePooledRef<POOLABLE> slot = recycleSlot(poolSlot);
					boolean addedIdle = irq.offerLast(slot);
					if (addedIdle) {
						incrementIdle();
					}
					actual.onComplete();
					boolean drained = drain();
					if (isDisposed() && slot.markDestroy()) {
						if (addedIdle) {
							decrementIdle();
						}
						destroyPoolable(slot).subscribe(); //TODO manage errors?
					} else if (drained && addedIdle && hasAvailableResources()) {
						poolConfig.resourceManager().resourceAvailable();
					}
					return;
				}
			}
		}
		//if we destroy instead of recycle, be sure to propagate destroy handler errors
		//to the release() subscriber
		if (poolSlot.markDestroy()) {
			destroyPoolable(poolSlot).subscribe(null,
					e -> {
						actual.onError(e);
						this.drain();
					},
					() -> {
						actual.onComplete();
						this.drain();
					}); //TODO manage errors?
		}
		else {
			actual.onComplete(); //already invalidated
		}
	}

	/**
	 * @param pending a new {@link reactor.pool.AbstractPool.Borrower} to add to the queue and later either serve or consider pending
	 */
	void pendingOffer(Borrower<POOLABLE> pending) {
		int maxPending = poolConfig.maxPending();
		ConcurrentLinkedDeque<Borrower<POOLABLE>> pendingQueue = this.pending;
		if (pendingQueue == TERMINATED) {
			return;
		}
		int postOffer = pendingSize;
		if (pendingQueue.offerLast(pending)) {
			postOffer = PENDING_SIZE.incrementAndGet(this);
		}

		if (WIP.getAndIncrement(this) == 0) {
			if (maxPending >= 0 && postOffer > maxPending && idleSize == 0 && poolConfig.allocationStrategy().estimatePermitCount() == 0) {
				//fail fast. differentiate slightly special case of maxPending == 0
				Borrower<POOLABLE> toCull = pendingQueue.pollLast();
				if (toCull != null) {
					PENDING_SIZE.decrementAndGet(this);
					if (maxPending == 0) {
						toCull.fail(new PoolAcquirePendingLimitException(0, "No pending allowed and pool has reached allocation limit"));
					}
					else {
						toCull.fail(new PoolAcquirePendingLimitException(maxPending));
					}
				}

				//we've managed the object, but let's drain loop in case there was another parallel interaction
				if (WIP.decrementAndGet(this) > 0) {
					drainLoop();
				}
				return;
			}

			//at this point, the pending is expected to be matched against a resource
			//let's invoke the drain loop (since we've won the WIP race)
			drainLoop();
		} //else we've lost the WIP race, but the pending has been enqueued
	}

	/**
	 * @return the next {@link reactor.pool.AbstractPool.Borrower} to serve
	 */
	@Nullable
	Borrower<POOLABLE> pendingPoll(Deque<Borrower<POOLABLE>> borrowers) {
		Borrower<POOLABLE> b = borrowers.pollFirst();
		if (b != null) {
			PENDING_SIZE.decrementAndGet(this);
		}
		return b;
	}

	QueuePooledRef<POOLABLE> recycleSlot(QueuePooledRef<POOLABLE> slot) {
		return new QueuePooledRef<>(slot);
	}

	@Override
	public boolean isDisposed() {
		return PENDING.get(this) == TERMINATED || IDLE_RESOURCES.get(this) == null;
	}




	static final class QueuePooledRef<T> extends AbstractPooledRef<T> {

		final SimpleDequePool<T> pool;

		//TODO we can probably have a Mono<Void> field to always return the same mono to invalidate/release calls
		//TODO we should also be able to collapse QueuePoolRecyclerMono and QueuePoolRecyclerInner (since it is intended to be idempotent)

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
				if (markSoftInvalidate()) {
					//immediately clean up state
					ACQUIRED.decrementAndGet(pool);
					return pool.destroyPoolable(this)
							//when invalidating, be sure to trigger drain even if there was an error
							.doFinally(st -> pool.drain());
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
					if (markDestroy()) {
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
				catch (Throwable releaseError) {
					ACQUIRED.decrementAndGet(pool); //immediately clean up state
					if (markDestroy()) {
						return pool.destroyPoolable(this)
								.onErrorResume(destroyError ->
									Mono.error(new IllegalStateException("Couldn't apply releaseHandler nor destroyHandler",
											Exceptions.multiple(releaseError, destroyError)))
								)
								.then(Mono.<Void>error(new IllegalStateException("Couldn't apply releaseHandler, resource destroyed", releaseError)))
								.doFinally(st -> pool.drain());
					}
					return Mono.error(new IllegalStateException("Couldn't apply releaseHandler", releaseError)); //should be a very rare corner case
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

			pool.maybeRecycleAndDrain(slot, actual);
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

			if (slot.markDestroy()) {
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
				this.start = pool.clock.millis();
				this.upstream = s;
				actual.onSubscribe(this);
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
