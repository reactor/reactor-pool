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
import reactor.util.pool.api.PoolSlot;

import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A builder for a {@link DefaultPoolConfig}.
 * @author Simon Baslé
 */
final class DefaultPoolConfigBuilder<T> implements PoolBuilder.RecyclingStep<T>, PoolBuilder.FirstPredicateStep<T>,
        PoolBuilder.OtherPredicateStep<T>, PoolBuilder.AfterPredicateStep<T>, PoolBuilder.SizeStep<T> {

    final Mono<T> allocator;

    Function<T, Mono<Void>> cleaner;
    @Nullable
    Predicate<? super PoolSlot<T>> evictionPredicate;
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
    public PoolBuilder.OtherPredicateStep<T> unlessPoolableMatches(Predicate<? super T> poolablePredicate) {
        this .evictionPredicate = slot -> poolablePredicate.test(slot.poolable());
        return this;
    }

    @Override
    public PoolBuilder.OtherPredicateStep<T> unlessSlotMatches(Predicate<? super PoolSlot<T>> slotPredicate) {
        this.evictionPredicate = slotPredicate;
        return this;
    }

    @Override
    public PoolBuilder.OtherPredicateStep<T> orPoolableMatches(Predicate<? super T> poolablePredicate) {
        //FIXME
        return this;
    }

    @Override
    public PoolBuilder.OtherPredicateStep<T> orSlotMatches(Predicate<? super PoolSlot<T>> slotPredicate) {
        //FIXME
        return this;
    }

    @Override
    public PoolBuilder.OtherPredicateStep<T> andPoolableMatches(Predicate<? super T> poolablePredicate) {
        //FIXME
        return this;
    }

    @Override
    public PoolBuilder.OtherPredicateStep<T> andSlotMatches(Predicate<? super PoolSlot<T>> slotPredicate) {
        //FIXME
        return this;
    }

    @Override
    public PoolBuilder.SizeStep<T> publishOn(Scheduler scheduler) {
        this.scheduler = scheduler;
        return this;
    }

    @Override
    public PoolBuilder<T> allocatingBetween(int minSize, int maxSize) {
        return new FinalStep<>(new DefaultPoolConfig<>(minSize, maxSize, allocator, cleaner, evictionPredicate, scheduler));
    }

    @Override
    public PoolBuilder<T> allocatingMax(int maxSize) {
        return new FinalStep<>(new DefaultPoolConfig<>(0, maxSize, allocator, cleaner, evictionPredicate, scheduler));
    }

    static final class FinalStep<T> implements PoolBuilder<T> {

        final DefaultPoolConfig<T> config;

        FinalStep(DefaultPoolConfig<T> config) {
            this.config = config;
        }

        @Override
        public Pool<T> buildQueuePool() {
            return new QueuePool<>(config);
        }
    }
}
