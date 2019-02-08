/*
 * Copyright (c) 2018-Present Pivotal Software Inc, All Rights Reserved.
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
package reactor.pool;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.pool.impl.PoolConfigBuilder;
import reactor.pool.metrics.NoOpPoolMetricsRecorder;
import reactor.pool.metrics.PoolMetricsRecorder;

import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Configuration for a {@link Pool}. Can be built using {@link PoolConfigBuilder#allocateWith(Mono) PoolConfigBuilder}.
 *
 * @author Simon Baslé
 */
public interface PoolConfig<P> {

    Function<?, Mono<Void>> NO_OP_FACTORY = it -> Mono.empty();

    /**
     * The asynchronous factory that produces new resources.
     *
     * @return a {@link Mono} representing the creation of a resource
     */
    Mono<P> allocator();

    /**
     * Defines a strategy / limit for the number of pooled object to allocate.
     *
     * @return the {@link AllocationStrategy} for the pool
     */
    AllocationStrategy allocationStrategy();

    /**
     * @return the minimum number of objects a {@link Pool} should create at initialization.
     */
    int initialSize();

    /**
     * A {@link Predicate} that checks if a resource should be disposed ({@code true}) or is still in a valid state
     * for recycling. This is primarily applied when a resource is released, to check whether or not it can immediately
     * be recycled, but could also be applied during an acquire attempt (detecting eg. idle resources) or by a background
     * reaping process.
     *
     * @return A {@link Predicate} that returns true if the {@link PooledRef} should be destroyed instead of used
     */
    Predicate<PooledRef<P>> evictionPredicate();

    /**
     * When a resource is {@link PooledRef#release() released}, defines a mechanism of resetting any lingering state of
     * the resource in order for it to become usable again. The {@link #evictionPredicate()} is applied AFTER this reset.
     * <p>
     * For example, a buffer could have a readerIndex and writerIndex that need to be flipped back to zero.
     *
     * @return a {@link Function} representing the asynchronous reset mechanism for a given resource
     */
    Function<P, Mono<Void>> resetResource();

    /**
     * Defines a mechanism of resource destruction, cleaning up state and OS resources it could maintain (eg. off-heap
     * objects, file handles, socket connections, etc...).
     * <p>
     * For example, a database connection could need to cleanly sever the connection link by sending a message to the database.
     *
     * @return a {@link Function} representing the asynchronous destroy mechanism for a given resource
     */
    Function<P, Mono<Void>> destroyResource();

    /**
     * The {@link Scheduler} on which the {@link Pool} should publish resources, independently of which thread called
     * {@link Pool#acquire()} or {@link PooledRef#release()} or on which thread the {@link #allocator()} produced new
     * resources.
     * <p>
     * Use {@link Schedulers#immediate()} if determinism is less important than staying on the same threads.
     *
     * @return a {@link Scheduler} on which to publish resources
     */
    Scheduler deliveryScheduler();

    /**
     * The {@link PoolMetricsRecorder} to use to collect instrumentation data of the {@link Pool}
     * implementations.
     * <p>
     * Defaults to {@link NoOpPoolMetricsRecorder}
     *
     * @return the {@link PoolMetricsRecorder} to use
     */
    PoolMetricsRecorder metricsRecorder();
}
