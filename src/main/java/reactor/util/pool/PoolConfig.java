package reactor.util.pool;

import reactor.core.publisher.Mono;

import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * @author Simon Baslé
 */
public interface PoolConfig<P> {

    Mono<P> allocator();
    Predicate<P> validator();
    Consumer<P> cleaner();

    int minSize();
    int maxSize();
}
