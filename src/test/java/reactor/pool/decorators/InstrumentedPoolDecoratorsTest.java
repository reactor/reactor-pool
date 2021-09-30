package reactor.pool.decorators;

import org.junit.jupiter.api.Test;

import reactor.core.publisher.Mono;
import reactor.pool.InstrumentedPool;
import reactor.pool.PoolBuilder;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Simon Baslé
 */
class InstrumentedPoolDecoratorsTest {

	@Test
	void decorateGracefulAsPartOfBuilder() {
		InstrumentedPool<String> pool = PoolBuilder.from(Mono.just("foo"))
			.sizeBetween(0, 5)
			.buildPoolAndDecorateWith(InstrumentedPoolDecorators::gracefulShutdown);

		assertThat(pool).isExactlyInstanceOf(GracefulShutdownInstrumentedPool.class);
	}

}