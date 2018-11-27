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
     * Release the {@code POOLABLE} back to the pool, asynchronously.
     *
     * @param poolable the {@code POOLABLE} to be released back to the pool
     * @return a {@link Mono} that will complete empty when the object has been released. In case of an error the object
     * is always discarded.
     */
    Mono<Void> release(POOLABLE poolable);

    /**
     * Release the {@code POOLABLE} back to the pool and block for the termination of the release.
     * In case of an error the object is always discarded.
     *
     * @param poolable the {@code POOLABLE} to be released back to the pool
     */
    void releaseSync(POOLABLE poolable);


}
