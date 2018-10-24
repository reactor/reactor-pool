package reactor.util.pool;

import reactor.core.publisher.Mono;

/**
 * @author Simon Baslé
 */
public interface Pool<POOLABLE> {

    /**
     * Borrow a {@code POOLABLE} from the pool upon subscription. The resulting {@link Mono} emits the {@code POOLABLE}
     * as it becomes available. Cancelling the {@link org.reactivestreams.Subscription} before the {@code POOLABLE} has
     * been emitted will either avoid object borrowing entirely or will result in immediate {@link PoolConfig#cleaner()} release}
     * of the {@code POOLABLE}.
     *
     * @return a {@link Mono}, each subscription to which represents the act of borrowing a pooled object
     */
    Mono<POOLABLE> borrow();

    /**
     * Release the {@code POOLABLE} back to the pool.
     * @param poolable the {@code POOLABLE} to be released back to the pool
     */
    void release(POOLABLE poolable);

}
