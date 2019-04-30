package reactor.pool;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;

import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.annotation.Nullable;
import reactor.util.concurrent.Queues;

/**
 * A Pool of Pools
 *
 * @author Stephane Maldini
 */
final class PartitionedPool<KEY, POOLABLE> extends ConfigurablePool<POOLABLE>
		implements Function<KEY, InstrumentedPool<POOLABLE>> {

	final Function<? super KEY, ? extends InstrumentedPool<POOLABLE>> poolFactory;
	final Function<? super POOLABLE, ? extends KEY>                   poolKeyFactory;

	final Queue<Acquisition<KEY, POOLABLE>> pending;

	volatile Map<KEY, InstrumentedPool<POOLABLE>> pools;
	volatile int                                  wip;

	PartitionedPool(PoolConfiguration<POOLABLE> poolConfig,
			Function<? super KEY, ? extends InstrumentedPool<POOLABLE>> poolFactory,
			Function<? super POOLABLE, ? extends KEY> poolKeyFactory) {
		super(poolConfig);
		this.poolKeyFactory = poolKeyFactory;
		this.poolFactory = poolFactory;
//		this.pending = Queues.get(poolConfig.maxPending).get();
		//TODO use MPSC ArrayQueue once in reactor-core
		this.pending = Queues.<Acquisition<KEY, POOLABLE>>unboundedMultiproducer().get();
		POOLS.lazySet(this, new ConcurrentHashMap<>(8));
	}

	@Override
	public Mono<PooledRef<POOLABLE>> acquire() {
		return acquire(Duration.ZERO);
	}

	@Override
	public Mono<PooledRef<POOLABLE>> acquire(Duration timeout) {
		return new MonoAcquire<>(this, timeout);
	}

	@Override
	public InstrumentedPool<POOLABLE> apply(KEY key) {
		InstrumentedPool<POOLABLE> pool = poolFactory.apply(key);
		//TODO record partition creation metric
		if (log.isDebugEnabled()) {
			log.debug("Pool partition for key '{}' has been created: ", key, pool);
		}
		return pool;
	}

	@Override
	public void dispose() {
		Map<KEY, InstrumentedPool<POOLABLE>> pools = terminate();

		if (pools == TERMINATED) {
			return;
		}

		for (InstrumentedPool<POOLABLE> pool : pools.values()) {
			pool.dispose();
		}

		drain();
	}

	@Override
	public int idleSize() {
		return pools.values()
		            .stream()
		            .mapToInt(p -> p.metrics()
		                            .idleSize())
		            .sum();
	}

	@Override
	public boolean isDisposed() {
		return pools == TERMINATED;
	}

	@Override
	public int pendingAcquireSize() {
		return pending.size();
	}

	@Override
	public String toString() {
		return "PartitionedPool{" + "pools=" + pools + '}';
	}

	void drain() {
		if (WIP.getAndIncrement(this) != 0) {
			return;
		}

		int missed = 1;
		Acquisition<KEY, POOLABLE> acquisition;


		for (; ; ) {
			Map<KEY, InstrumentedPool<POOLABLE>> pools = this.pools;
			boolean terminated = pools == TERMINATED;

			if (terminated) {
				while ((acquisition = pending.poll()) != null) {
					acquisition.actual.onError(DisposedError.INSTANCE);
				}
				return;
			}

			//get permits only if queue has pending
			if (pending.isEmpty() ||
					poolConfig.allocationStrategy.getPermits(1) == 0) {
				missed = WIP.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
				continue;
			}

			acquisition = pending.peek();


			//cannot be null due to pending.isEmpty above and non concurrent poll
			KEY key = acquisition.key;

			InstrumentedPool<POOLABLE> pool = computeOrFindAvailablePool(pools, key);

			if (pool == null) {
				poolConfig.allocationStrategy.returnPermits(1);
			}
			else {
				pending.poll();

				pool.acquire()
				    .map(pr -> new PartitionedPoolRef<>(this, pr, key))
				    .subscribe(acquisition.actual);
			}

			missed = WIP.addAndGet(this, -missed);
			if (missed == 0) {
				break;
			}
		}
	}

	@Nullable
	InstrumentedPool<POOLABLE> computeOrFindAvailablePool(Map<KEY, InstrumentedPool<POOLABLE>> pools, KEY key) {
		//TODO should cache in partitionedPool
		@SuppressWarnings("unchecked") InstrumentedPool<POOLABLE>[] findLast =
				(InstrumentedPool<POOLABLE>[]) new InstrumentedPool[1];

		int idle = pools.values()
		                .stream()
		                .mapToInt(p -> {
			                int i = p.metrics().idleSize();
			                if (i != 0) {
				                findLast[0] = p;
			                }
			                return i;
		                })
		                .sum();

		if (poolConfig.allocationStrategy.permitMaximum() - idle == 0) {
			return pools.getOrDefault(key, findLast[0]);
		}
		else {
			//TODO limit number of partitions?
			return pools.computeIfAbsent(key, this);
		}
	}

	@SuppressWarnings("unchecked")
	Map<KEY, InstrumentedPool<POOLABLE>> terminate() {
		return POOLS.getAndSet(this, TERMINATED);
	}

	static final class PartitionedPoolRef<KEY, POOLABLE> implements PooledRef<POOLABLE>, Runnable {

		final PooledRef<POOLABLE>            pooledRef;
		final KEY                            key;
		final PartitionedPool<KEY, POOLABLE> parent;

		PartitionedPoolRef(PartitionedPool<KEY, POOLABLE> parent, PooledRef<POOLABLE> pooledRef, KEY key) {
			this.pooledRef = pooledRef;
			this.key = key;
			this.parent = parent;
		}

		@Override
		public Mono<Void> invalidate() {
			return pooledRef.invalidate()
			                .doAfterTerminate(this);
		}

		@Override
		public PooledRefMetadata metadata() {
			return pooledRef.metadata();
		}

		@Override
		public POOLABLE poolable() {
			return pooledRef.poolable();
		}

		@Override
		public Mono<Void> release() {
			return pooledRef.release()
			                .doAfterTerminate(this);
		}

		@Override
		public void run() {
			parent.poolConfig.allocationStrategy.returnPermits(1);
			parent.drain();
		}

		@Override
		public String toString() {
			return "PartitionedPoolRef{" + "pooledRef=" + pooledRef + ", key=" + key + ", parent=" + parent + '}';
		}
	}

	static final class Acquisition<KEY, POOLABLE> {

		final CoreSubscriber<? super PooledRef<POOLABLE>> actual;
		final KEY                                         key;

		Acquisition(CoreSubscriber<? super PooledRef<POOLABLE>> actual, KEY key) {
			this.actual = actual;
			this.key = key;
		}
	}

	static final class MonoAcquire<KEY, POOLABLE> extends Mono<PooledRef<POOLABLE>> {

		final Duration                       acquireTimeout;
		final PartitionedPool<KEY, POOLABLE> parent;

		MonoAcquire(PartitionedPool<KEY, POOLABLE> parent, Duration acquireTimeout) {
			this.acquireTimeout = acquireTimeout;
			this.parent = parent;
		}

		@Override
		public void subscribe(CoreSubscriber<? super PooledRef<POOLABLE>> actual) {
			if (parent.isDisposed()) {
				Operators.error(actual, DisposedError.INSTANCE);
				return;
			}

			//TODO pass key by context or by argument?
			KEY key = Objects.requireNonNull(parent.poolKeyFactory.apply(null), "key must be defined");

			if (parent.poolConfig.allocationStrategy.getPermits(1) == 1) {
				Map<KEY, InstrumentedPool<POOLABLE>> pools = parent.pools;
				if (pools == TERMINATED) {
					return;
				}

				InstrumentedPool<POOLABLE> pool = parent.computeOrFindAvailablePool(pools, key);

				if (pool == null) {
					parent.poolConfig.allocationStrategy.returnPermits(1);
				}
				else {
					pool.acquire()
					    .map(pr -> new PartitionedPoolRef<>(parent, pr, key))
					    .subscribe(actual);

					return;
				}
			}

			//pending acquire
			if (!parent.pending.offer(new Acquisition<>(actual, key))) {
				Operators.error(actual, Exceptions.failWithOverflow("Too much pending subscribers"));
				return;
			}
			parent.drain();
		}
	}

	static final class DisposedError extends RuntimeException {

		DisposedError() {
			super("Cannot acquire from a disposed pool");
		}

		static final DisposedError INSTANCE = new DisposedError();

	}

	static final Logger                                            log        =
			Loggers.getLogger(PartitionedPool.class);
	@SuppressWarnings("rawtypes")
	static final Map                                               TERMINATED = Collections.emptyMap();
	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<PartitionedPool, Map> POOLS      =
			AtomicReferenceFieldUpdater.newUpdater(PartitionedPool.class, Map.class, "pools");

	static final AtomicIntegerFieldUpdater<PartitionedPool> WIP =
			AtomicIntegerFieldUpdater.newUpdater(PartitionedPool.class, "wip");

	static <POOLABLE> PartitionedPool<Long, POOLABLE> partitionByThread(AbstractPool.DefaultPoolConfig<POOLABLE> poolConfig) {
		PoolConfiguration<POOLABLE> defaultPoolConfig = new PoolConfiguration<>(poolConfig.allocator,
				poolConfig.initialSize,
				poolConfig.allocationStrategy,
				poolConfig.maxPending,
				poolConfig.destroyHandler,
				poolConfig.releaseHandler,
				poolConfig.evictionPredicate,
				poolConfig.acquisitionScheduler,
				poolConfig.metricsRecorder);


		return new PartitionedPool<>(defaultPoolConfig,
				key -> {

					AbstractPool.DefaultPoolConfig<POOLABLE> partitionConfig =
							new AbstractPool.DefaultPoolConfig<>(poolConfig.allocator,
									0,
									//FIXME make sure we dont block on constructor
//						poolConfig.allocationStrategy,
									new AllocationStrategies.UnboundedAllocationStrategy(),
									poolConfig.maxPending,
									poolConfig.destroyHandler,
									poolConfig.releaseHandler,
									poolConfig.evictionPredicate,
									poolConfig.acquisitionScheduler,
									poolConfig.metricsRecorder,
									poolConfig.isLifo);

					if (poolConfig.isLifo) {
						return new SimpleLifoPool<>(partitionConfig);
					}
					return new SimpleFifoPool<>(partitionConfig);
				},
				poolable -> Thread.currentThread()
				                  .getId());
	}

}
