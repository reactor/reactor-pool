package reactor.util.pool.impl;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.pool.PoolConfig;

import java.util.function.Function;
import java.util.function.Predicate;

/**
 * @author Simon Baslé
 */
//package private for test extendability
class DefaultPoolConfig<POOLABLE> implements PoolConfig<POOLABLE> {

    private final int minSize;
    private final int maxSize;
    private final Mono<POOLABLE> allocator;
    private final Function<POOLABLE, Mono<Void>> cleaner;
    private final Predicate<POOLABLE> validator;
    private final Scheduler deliveryScheduler;

    DefaultPoolConfig(int minSize, int maxSize, Mono<POOLABLE> allocator,
                      Function<POOLABLE, Mono<Void>> cleaner,
                      Predicate<POOLABLE> validator) {
        this(minSize, maxSize, allocator, cleaner, validator, Schedulers.immediate());
    }

    DefaultPoolConfig(int minSize, int maxSize, Mono<POOLABLE> allocator,
                      Function<POOLABLE, Mono<Void>> cleaner,
                      Predicate<POOLABLE> validator,
                      Scheduler deliveryScheduler) {
        this.minSize = minSize;
        this.maxSize = maxSize;

        this.allocator = allocator;
        this.cleaner = cleaner;
        this.validator = validator;

        this.deliveryScheduler = deliveryScheduler;
    }

    @Override
    public Mono<POOLABLE> allocator() {
        return this.allocator;
    }

    @Override
    public Function<POOLABLE, Mono<Void>> cleaner() {
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

    @Override
    public Scheduler deliveryScheduler() {
        return this.deliveryScheduler;
    }
}
