/*
 * Copyright (c) 2011-2019 Pivotal Software Inc, All Rights Reserved.
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

package reactor.util.pool.api;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.annotation.Nullable;
import reactor.util.pool.metrics.MetricsRecorder;
import reactor.util.pool.metrics.NoOpMetricsRecorder;

import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A default {@link PoolConfig}.
 *
 * @author Simon Baslé
 */
class DefaultPoolConfig<POOLABLE> implements PoolConfig<POOLABLE> {

    private final Mono<POOLABLE> allocator;
    private final int initialSize;
    private final AllocationStrategy allocationStrategy;
    private final Function<POOLABLE, Mono<Void>> resetFactory;
    private final Function<POOLABLE, Mono<Void>> destroyFactory;
    private final Predicate<PooledRef<POOLABLE>> evictionPredicate;
    private final Scheduler deliveryScheduler;
    private final MetricsRecorder metricsRecorder;

    DefaultPoolConfig(Mono<POOLABLE> allocator,
                      int initialSize,
                      @Nullable AllocationStrategy allocationStrategy,
                      @Nullable Function<POOLABLE, Mono<Void>> resetFactory,
                      @Nullable Function<POOLABLE, Mono<Void>> destroyFactory,
                      @Nullable Predicate<PooledRef<POOLABLE>> evictionPredicate,
                      @Nullable Scheduler deliveryScheduler,
                      @Nullable MetricsRecorder metricsRecorder) {
        this.allocator = allocator;
        this.initialSize = initialSize < 0 ? 0 : initialSize;
        this.allocationStrategy = allocationStrategy == null ? AllocationStrategies.unbounded() : allocationStrategy;

        @SuppressWarnings("unchecked")
        Function<POOLABLE, Mono<Void>> noOp = (Function<POOLABLE, Mono<Void>>) NO_OP_FACTORY;

        this.resetFactory = resetFactory == null ? noOp : resetFactory;
        this.destroyFactory = destroyFactory == null ? noOp : destroyFactory;

        this.evictionPredicate = evictionPredicate == null ? slot -> false : evictionPredicate;
        this.deliveryScheduler = deliveryScheduler == null ? Schedulers.immediate() : deliveryScheduler;
        this.metricsRecorder = metricsRecorder == null ? NoOpMetricsRecorder.INSTANCE : metricsRecorder;
    }

    @Override
    public Mono<POOLABLE> allocator() {
        return this.allocator;
    }

    @Override
    public AllocationStrategy allocationStrategy() {
        return this.allocationStrategy;
    }

    @Override
    public int initialSize() {
        return this.initialSize;
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
    public Scheduler deliveryScheduler() {
        return this.deliveryScheduler;
    }

    @Override
    public MetricsRecorder metricsRecorder() {
        return this.metricsRecorder;
    }
}
