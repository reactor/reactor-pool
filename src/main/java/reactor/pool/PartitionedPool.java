package reactor.pool;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;

import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.util.Logger;
import reactor.util.Loggers;

/**
 * A Pool of Pools
 *
 * @author Stephane Maldini
 */
final class PartitionedPool<KEY, POOLABLE> extends ConfigurablePool<POOLABLE>
		implements Function<KEY, InstrumentedPool<POOLABLE>> {

	final Function<? super KEY, ? extends InstrumentedPool<POOLABLE>> poolFactory;
	final Function<? super POOLABLE, ? extends KEY>                   poolKeyFactory;

	volatile Map<KEY, InstrumentedPool<POOLABLE>> pools;

	PartitionedPool(PoolConfiguration<POOLABLE> poolConfig,
			Function<? super KEY, ? extends InstrumentedPool<POOLABLE>> poolFactory,
			Function<? super POOLABLE, ? extends KEY> poolKeyFactory) {
		super(poolConfig);
		this.poolKeyFactory = poolKeyFactory;
		this.poolFactory = poolFactory;
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
		for (InstrumentedPool<POOLABLE> pool : terminate().values()) {
			pool.dispose();
		}
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
		return pools.values()
		            .stream()
		            .mapToInt(p -> p.metrics()
		                            .pendingAcquireSize())
		            .sum();
	}

	@SuppressWarnings("unchecked")
	Map<KEY, InstrumentedPool<POOLABLE>> terminate() {
		return POOLS.getAndSet(this, TERMINATED);
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

			if (parent.poolConfig.allocationStrategy.getPermits(1) != 1) {

				return;
			}

			//TODO pass key by context or by argument?
			KEY key = Objects.requireNonNull(parent.poolKeyFactory.apply(null), "key must be defined");
			try {
				//TODO limit number of partitions?
				InstrumentedPool<POOLABLE> pool = parent.pools.computeIfAbsent(key, parent);
				pool.acquire(acquireTimeout)
				    .subscribe(actual);
			}
			catch (Throwable err) {
				Exceptions.throwIfFatal(err);
				if (err.getClass()
				       .equals(UnsupportedOperationException.class) && parent.pools == TERMINATED) {
					Operators.error(actual, DisposedError.INSTANCE);
				}
				Operators.error(actual, err);
			}
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

		AbstractPool.DefaultPoolConfig<POOLABLE> partitionConfig =
				new AbstractPool.DefaultPoolConfig<>(poolConfig.allocator,
						0,
						//FIXME make sure we dont block on constructor
//						poolConfig.allocationStrategy,
						new AllocationStrategies.UnboundedAllocationStrategy(),
						poolConfig.maxPending,
						poolConfig.destroyHandler,
						poolable ->
							Mono.fromDirect(poolConfig.releaseHandler.apply(poolable))
							    .doOnSuccess(v -> poolConfig.allocationStrategy.returnPermits(1)),
						poolConfig.evictionPredicate,
						poolConfig.acquisitionScheduler,
						poolConfig.metricsRecorder,
						poolConfig.isLifo);

		return new PartitionedPool<>(defaultPoolConfig,
				poolConfig.isLifo ? key -> new SimpleLifoPool<>(partitionConfig) :
						key -> new SimpleFifoPool<>(partitionConfig),
				poolable -> Thread.currentThread()
				                  .getId());
	}

}
