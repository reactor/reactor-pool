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
import reactor.core.scheduler.Schedulers;
import reactor.util.pool.api.Pool;
import reactor.util.pool.api.PoolConfig;
import reactor.util.pool.api.PoolSlot;

import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Builder for {@link PoolConfig} and {@link Pool}.
 *
 * @author Simon Baslé
 */
public interface PoolBuilder<POOLABLE> {

    /**
     * Build a {@link PoolConfig} instead of a {@link Pool}.
     *
     * @return a {@link PoolConfig} as defined by this builders
     */
    PoolConfig<POOLABLE> toConfig();

    /**
     * Build a MPSC-Queue based {@link Pool}, configured with the {@link PoolConfig}
     * defined by this builder.
     *
     * @return a MPSC-Queue-based {@link Pool}
     */
    Pool<POOLABLE> buildQueuePool();

    /**
     * Start building a {@link Pool} by describing how new objects are to be asynchronously allocated.
     * Note that the {@link Mono} {@code allocator} should NEVER block its thread (thus adapting from blocking code,
     * eg. a constructor, via {@link Mono#fromCallable(Callable)} should be augmented with {@link Mono#publishOn(Scheduler)}).
     *
     * @param allocator the asynchronous creator of poolable resources.
     * @param <T> the type of resource created and recycled by this {@link Pool}
     * @return the next step in building a {@link Pool}
     */
    static <T> RecyclingStep<T> allocatingWith(Mono<T> allocator) {
        return new DefaultPoolConfigBuilder<>(allocator);
    }

    /**
     * Builder step for mandatory recycler/clearner
     * @param <T>
     */
    interface RecyclingStep<T> {

        /**
         * Continue building the {@link Pool} by providing a {@code cleaner}, a recycling {@link Function} that will
         * be applied whenever an object is released, producing a {@link Mono Mono&lt;Void&gt;} that will asynchronously
         * recycle the object. AFTER the Mono has terminated, the object will be returned back to the pool...
         *
         * @param cleaner the {@link Function} supplying the recycling {@link Mono}
         * @return the next step in building a {@link Pool}
         */
        FirstPredicateStep<T> recycleWith(Function<T, Mono<Void>> cleaner);
    }

    /**
     * Builder step for mandatory eviction strategies / predicate that prevents recycling
     * @param <T>
     */
    interface FirstPredicateStep<T> {

        /**
         * The object is to be recycled and released back to the {@link Pool} <strong>unless</strong> it matches the
         * given {@link Predicate}.
         * <p>
         * Use {@link OtherPredicateStep#orPoolableMatches(Predicate)} and {@link OtherPredicateStep#orSlotMatches(Predicate)}
         * to combine multiple predicates (OR). Use {@link Predicate#and(Predicate)} if you need to group multiple conditions (AND).
         *
         * @param poolablePredicate a {@link Predicate} on the {@link PoolSlot} holding the object, {@literal true}
         *                          meaning it should prevent recycling.
         * @return the next step in building a {@link Pool}
         */
        OtherPredicateStep<T> unlessPoolableMatches(Predicate<? super T> poolablePredicate);

        /**
         * The object is to be recycled and released back to the {@link Pool} <strong>unless</strong> it matches the
         * given {@link Predicate}, as applied to the {@link PoolSlot} holding it.
         * See {@link reactor.util.pool.api.EvictionStrategies} for pre-made {@link PoolSlot} predicates.
         * <p>
         * Use {@link OtherPredicateStep#orPoolableMatches(Predicate)} and {@link OtherPredicateStep#orSlotMatches(Predicate)}
         * to combine multiple predicates (OR). Use {@link Predicate#and(Predicate)} if you need to group multiple conditions (AND).
         *
         * @param slotPredicate a {@link Predicate} on the {@link PoolSlot} holding the object, {@literal true}
         *                          meaning it should prevent recycling.
         * @return the next step in building a {@link Pool}
         */
        OtherPredicateStep<T> unlessSlotMatches(Predicate<? super PoolSlot<? super T>> slotPredicate);
    }

    /**
     * Builder step for additional optional conditions on the recycling {@link Predicate}. These can be omitted
     * in favor of either configuring a Scheduler for the {@link Pool} or finalizing the configuration by defining the
     * {@link Pool} sizing (min and max allocated objects).
     * @param <T>
     */
    interface OtherPredicateStep<T> extends AfterPredicateStep<T> {

        /**
         * The object is to be recycled and released back to the {@link Pool} <strong>unless</strong> any of the
         * previously set predicate match OR it matches the given {@link Predicate}.
         * <p>
         * Use {@link #orSlotMatches(Predicate)} to combine multiple predicates (OR). Use {@link Predicate#and(Predicate)}
         * if you need to group multiple conditions (AND).
         *
         * @param poolablePredicate a {@link Predicate} on the {@link PoolSlot} holding the object, {@literal true}
         *                          meaning it should prevent recycling.
         * @return the next step in building a {@link Pool}
         */
        OtherPredicateStep<T> orPoolableMatches(Predicate<? super T> poolablePredicate);

        /**
         * The object is to be recycled and released back to the {@link Pool} <strong>unless</strong> any of the
         * previously set predicates match OR the {@link PoolSlot} which holds it matches the given {@link Predicate}.
         * See {@link reactor.util.pool.api.EvictionStrategies} for pre-made {@link PoolSlot} predicates.
         * <p>
         * Use {@link #orPoolableMatches(Predicate)} to combine multiple predicates (OR). Use {@link Predicate#and(Predicate)}
         * if you need to group multiple conditions (AND).
         *
         * @param slotPredicate a {@link Predicate} on the pooled object, {@literal true} meaning it should prevent recycling.
         * @return the next step in building a {@link Pool}
         */
        OtherPredicateStep<T> orSlotMatches(Predicate<? super PoolSlot<? super T>> slotPredicate);

    }

    /**
     * Builder step for optional configuration of a Scheduler for the {@link Pool}.
     * Also allows direct final configuration of the {@link Pool} by defining its sizing (min and max allocated objects).
     */
    interface AfterPredicateStep<T> extends SizeStep<T> {

        /**
         * Define a {@link Scheduler} onto which the pooled elements will be published once they become available,
         * when borrowing from the {@link Pool} (either via {@link Pool#acquire()} or {@link Pool#borrow(Function)}).
         *
         * @param scheduler the {@link Scheduler} to use. Default is {@link Schedulers#immediate()}.
         * @return the last step in building a {@link Pool}
         */
        SizeStep<T> publishOn(Scheduler scheduler);
    }

    /**
     * Final step in configuring the {@link Pool}, defining its sizing (min and max allocated objects).
     * @param <T>
     */
    interface SizeStep<T> {

        /**
         * Ensure there are always at least {@code minSize} usable objects in the {@link Pool}, and at most {@code maxSize}
         * (both inclusive). The {@code minSize} drives initial pre-allocation of objects when building the {@link Pool}.
         *
         * @param minSize the minimum number of allocated objects to keep in the {@link Pool} (inclusive, borrowed + available)
         * @param maxSize the maximum number of allocated objects to keep in the {@link Pool} (inclusive, borrowed + available)
         * @return a {@link PoolBuilder} allowing to build a {@link Pool} from this configuration
         */
        PoolBuilder<T> allocatingBetween(int minSize, int maxSize);

        /**
         * Ensure there are always at most {@code maxSize} usable objects in the {@link Pool}.
         *
         * @param maxSize the maximum number of allocated objects to keep in the {@link Pool} (inclusive, borrowed + available)
         * @return a {@link PoolBuilder} allowing to build a {@link Pool} from this configuration
         */
        PoolBuilder<T> allocatingMax(int maxSize);
    }
}
