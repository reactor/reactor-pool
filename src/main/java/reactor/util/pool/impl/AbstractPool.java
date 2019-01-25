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

package reactor.util.pool.impl;

import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.util.Logger;
import reactor.util.annotation.Nullable;
import reactor.util.pool.api.AllocationStrategy;
import reactor.util.pool.api.Pool;
import reactor.util.pool.api.PoolConfig;
import reactor.util.pool.metrics.MetricsRecorder;

import java.io.Closeable;
import java.io.IOException;
import java.util.function.Function;

/**
 * An abstract base version of a {@link Pool}, mutualizing small amounts of code and allowing to build common
 * related classes like {@link AbstractPooledRef} or {@link Borrower}.
 *
 * @author Simon Baslé
 */
abstract class AbstractPool<POOLABLE> implements Pool<POOLABLE> {

    //A pool should be rare enough that having instance loggers should be ok
    //This helps with testability of some methods that for now mainly log
    final Logger logger;

    final PoolConfig<POOLABLE> poolConfig;

    protected final MetricsRecorder metricsRecorder;


    AbstractPool(PoolConfig<POOLABLE> poolConfig, Logger logger) {
        this.poolConfig = poolConfig;
        this.logger = logger;
        this.metricsRecorder = poolConfig.metricsRecorder();
    }

    @SuppressWarnings("WeakerAccess")
    void defaultDestroy(@Nullable POOLABLE poolable) {
        if (poolable instanceof Disposable) {
            ((Disposable) poolable).dispose();
        }
        else if (poolable instanceof Closeable) {
            try {
                ((Closeable) poolable).close();
            } catch (IOException e) {
                logger.trace("Failure while discarding a released Poolable that is Closeable, could not close", e);
            }
        }
        //TODO anything else to throw away the Poolable?
    }

    /**
     * Apply the configured destroyFactory to get the destroy {@link Mono} AND return a permit to the {@link AllocationStrategy},
     * which assumes that the {@link Mono} will always be subscribed.
     *
     * @param poolable the poolable that is not part of the live set
     * @return the destroy {@link Mono}, which MUST be subscribed immediately
     */
    Mono<Void> destroyPoolable(@Nullable POOLABLE poolable) {
        poolConfig.allocationStrategy().returnPermit();
        long start = System.nanoTime();
        Function<POOLABLE, Mono<Void>> factory = poolConfig.destroyResource();
        Mono<Void> base;
        if (factory == PoolConfig.NO_OP_FACTORY) {
            base = Mono.fromRunnable(() -> defaultDestroy(poolable));
        }
        else {
            base = factory.apply(poolable);
        }

        if (metricsRecorder != null) {
            return base.doFinally(fin -> metricsRecorder.recordDestroyLatency(System.nanoTime() - start));
        }
        else {
            return base;
        }
    }

}
