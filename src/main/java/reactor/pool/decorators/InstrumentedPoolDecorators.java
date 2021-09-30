package reactor.pool.decorators;

import java.time.Duration;
import java.util.function.Function;

import reactor.pool.InstrumentedPool;

/**
 * Utility class to expose various {@link InstrumentedPool} decorators, which can also be used
 * via {@link reactor.pool.PoolBuilder#buildPoolAndDecorateWith(Function)}.
 *
 * @author Simon Baslé
 */
public final class InstrumentedPoolDecorators {

	/**
	 * Decorate the pool with the capacity to {@link GracefulShutdownInstrumentedPool gracefully shutdown},
	 * via {@link GracefulShutdownInstrumentedPool#disposeGracefully(Duration)}.
	 *
	 * @param pool the original pool
	 * @param <T> the type of resources in the pool
	 * @return the decorated pool which can now gracefully shutdown via additional methods
	 * @see GracefulShutdownInstrumentedPool
	 */
	public static <T> GracefulShutdownInstrumentedPool<T> gracefulShutdown(InstrumentedPool<T> pool) {
		return new GracefulShutdownInstrumentedPool<>(pool);
	}

	private InstrumentedPoolDecorators() { }

}
