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
import reactor.util.annotation.Nullable;
import reactor.util.pool.api.PoolConfig;
import reactor.util.pool.api.PooledRef;

import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A default {@link PoolConfig}.
 *
 * @author Simon Baslé
 */
class DefaultPoolConfig<POOLABLE> implements PoolConfig<POOLABLE> {

    static final Function<?, Mono<Void>> NO_OP = it -> Mono.empty();

    private final int initialSize;
    private final int maxSize;
    private final Mono<POOLABLE> allocator;
    private final Function<POOLABLE, Mono<Void>> resetFactory;
    private final Function<POOLABLE, Mono<Void>> destroyFactory;
    private final Predicate<PooledRef<POOLABLE>> evictionPredicate;
    private final Scheduler deliveryScheduler;

    DefaultPoolConfig(int initialSize, int maxSize, Mono<POOLABLE> allocator,
                      @Nullable Function<POOLABLE, Mono<Void>> resetFactory,
                      @Nullable Function<POOLABLE, Mono<Void>> destroyFactory,
                      @Nullable Predicate<PooledRef<POOLABLE>> evictionPredicate,
                      @Nullable Scheduler deliveryScheduler) {
        this.initialSize = initialSize;
        this.maxSize = maxSize;

        @SuppressWarnings("unchecked")
        Function<POOLABLE, Mono<Void>> noOp = (Function<POOLABLE, Mono<Void>>) NO_OP;

        this.allocator = allocator;
        this.resetFactory = resetFactory == null ? noOp : resetFactory;
        this.destroyFactory = destroyFactory == null ? noOp : destroyFactory;

        this.evictionPredicate = evictionPredicate == null ? slot -> false : evictionPredicate;
        this.deliveryScheduler = deliveryScheduler == null ? Schedulers.immediate() : deliveryScheduler;
    }

    @Override
    public Mono<POOLABLE> allocator() {
        return this.allocator;
    }

    @Override
    public Function<POOLABLE, Mono<Void>> resetResource() {
        return this.resetFactory;
    }

    @Override
    public Function<POOLABLE, Mono<Void>> destroyResource() {
        return this.destroyFactory;
    }

    @Override
    public Predicate<PooledRef<POOLABLE>> evictionPredicate() {
        return this.evictionPredicate;
    }

    @Override
    public int initialSize() {
        return this.initialSize;
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
