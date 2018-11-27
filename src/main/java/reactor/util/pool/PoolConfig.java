package reactor.util.pool;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import java.util.function.Function;
import java.util.function.Predicate;

/**
 * @author Simon Baslé
 */
public interface PoolConfig<P> {

    Mono<P> allocator();
    Predicate<P> validator();
    Function<P, Mono<Void>> cleaner();

    int minSize();
    int maxSize();

    Scheduler deliveryScheduler();
}
