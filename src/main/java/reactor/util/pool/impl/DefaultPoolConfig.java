package reactor.util.pool.impl;

import reactor.core.publisher.Mono;
import reactor.util.pool.PoolConfig;

import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * @author Simon Baslé
 */
//package private for test extendability
class DefaultPoolConfig<POOLABLE> implements PoolConfig<POOLABLE> {

    private final int minSize;
    private final int maxSize;
    private final Mono<POOLABLE> allocator;
    private final Consumer<POOLABLE> cleaner;
    private final Predicate<POOLABLE> validator;

    DefaultPoolConfig(int minSize, int maxSize, Mono<POOLABLE> allocator,
                      Consumer<POOLABLE> cleaner,
                      Predicate<POOLABLE> validator) {
        this.minSize = minSize;
        this.maxSize = maxSize;

        this.allocator = allocator;
        this.cleaner = cleaner;
        this.validator = validator;
    }

    @Override
    public Mono<POOLABLE> allocator() {
        return this.allocator;
    }

    @Override
    public Consumer<POOLABLE> cleaner() {
        return this.cleaner;
    }

    @Override
    public Predicate<POOLABLE> validator() {
        return this.validator;
    }

    @Override
    public int minSize() {
        return this.minSize;
    }

    @Override
    public int maxSize() {
        return this.maxSize;
    }
}
