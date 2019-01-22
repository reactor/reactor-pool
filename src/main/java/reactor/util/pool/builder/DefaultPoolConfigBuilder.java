/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.util.pool.builder;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.util.annotation.Nullable;
import reactor.util.pool.api.Pool;
import reactor.util.pool.api.PoolConfig;
import reactor.util.pool.api.PooledRef;

import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A builder for a {@link DefaultPoolConfig}.
 * @author Simon Baslé
 */
final class DefaultPoolConfigBuilder<T> implements PoolBuilder.RecyclingStep<T>,
        PoolBuilder.FirstPredicateStep<T>,
        PoolBuilder.OtherPredicateStep<T>,
        PoolBuilder.AfterPredicateStep<T>,
        PoolBuilder.SizeStep<T> {

    final Mono<T> allocator;

    //FIXME see how to integrate optional aspect of cleaner, destroyer (or remove notion of builder entirely)
    @Nullable
    Function<T, Mono<Void>> cleaner;
    @Nullable
    Function<T, Mono<Void>> destroyer = null;
    @Nullable
    Predicate<PooledRef<T>> evictionPredicate;
    @Nullable
    Scheduler scheduler = null;

    DefaultPoolConfigBuilder(Mono<T> allocator) {
        this.allocator = allocator;
    }

    @Override
    public PoolBuilder.FirstPredicateStep<T> recycleWith(Function<T, Mono<Void>> cleaner) {
        this.cleaner = cleaner;
        return this;
    }

    @Override
    public PoolBuilder.FirstPredicateStep<T> recycleAndDestroyWith(Function<T, Mono<Void>> cleaner, Function<T, Mono<Void>> destroyer) {
        this.cleaner = cleaner;
        this.destroyer = destroyer;
        return this;
    }

    @Override
    public PoolBuilder.FirstPredicateStep<T> destroyWith(Function<T, Mono<Void>> destroyer) {
        this.destroyer = destroyer;
        return this;
    }

    @Override
    public PoolBuilder.OtherPredicateStep<T> unlessPoolableMatches(Predicate<? super T> poolablePredicate) {
        this .evictionPredicate = slot -> poolablePredicate.test(slot.poolable());
        return this;
    }

    @Override
    public PoolBuilder.OtherPredicateStep<T> unlessRefMatches(Predicate<? super PooledRef<? super T>> refPredicate) {
        this.evictionPredicate = refPredicate::test;
        return this;
    }

    @Override
    public PoolBuilder.OtherPredicateStep<T> orPoolableMatches(Predicate<? super T> poolablePredicate) {
        assert this.evictionPredicate != null;
        final Predicate<PooledRef<T>> oldPredicate = this.evictionPredicate;
        this.evictionPredicate = slot -> oldPredicate.test(slot) || poolablePredicate.test(slot.poolable());
        return this;
    }

    @Override
    public PoolBuilder.OtherPredicateStep<T> orRefMatches(Predicate<? super PooledRef<? super T>> refPredicate) {
        assert this.evictionPredicate != null;
        final Predicate<PooledRef<T>> oldPredicate = this.evictionPredicate;
        this.evictionPredicate = slot -> oldPredicate.test(slot) || refPredicate.test(slot);
        return this;
    }

    @Override
    public PoolBuilder.SizeStep<T> publishOn(Scheduler scheduler) {
        this.scheduler = scheduler;
        return this;
    }

    @Override
    public PoolBuilder<T> allocatingBetween(int minSize, int maxSize) {
        return new FinalStep<>(new DefaultPoolConfig<>(minSize, maxSize, allocator, cleaner, destroyer, evictionPredicate, scheduler));
    }

    @Override
    public PoolBuilder<T> allocatingMax(int maxSize) {
        return new FinalStep<>(new DefaultPoolConfig<>(0, maxSize, allocator, cleaner, destroyer, evictionPredicate, scheduler));
    }

    static final class FinalStep<T> implements PoolBuilder<T> {

        final DefaultPoolConfig<T> config;

        FinalStep(DefaultPoolConfig<T> config) {
            this.config = config;
        }

        @Override
        public PoolConfig<T> toConfig() {
            return this.config;
        }

        @Override
        public Pool<T> buildQueuePool() {
            return new QueuePool<>(config);
        }
    }
}
