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

package reactor.util.pool.api;

import reactor.core.publisher.Mono;

import java.util.function.Function;

/**
 * An abstraction over an object in a {@link Pool}, which holds the underlying {@code POOLABLE} object and allows one to
 * manually {@link #release()} it to the pool or {@link #invalidate()} it. Since the {@link PooledRef} provides a few
 * additional information about its lifecyle, like its {@link #timeSinceAllocation()} and the number of times it has been
 * {@link #acquireCount() acquired}, the {@link Pool} may optionally use that information to automatically invalidate
 * the object, and provide a simplified acquire mechanism where the {@link PooledRef} is not directly exposed (see
 * {@link Pool#acquireInScope(Function)} vs {@link Pool#acquire()}).
 *
 * @author Simon Baslé
 */
public interface PooledRef<POOLABLE> {

    /**
     * Returns the wrapped {@code POOLABLE}. This method is not thread-safe and the poolable should NEVER be accessed
     * concurrently.
     * <p>
     * The reference is retained by the {@link PooledRef} but the object might be in an intermediate or invalid state
     * when one of the {@link #release()} or {@link #invalidate()} methods have been previously called.
     *
     * @return the poolable
     */
    POOLABLE poolable();

    /**
     * Return a {@link Mono} that, once subscribed, will release the {@code POOLABLE} back to the pool asynchronously.
     * <p>
     * This method is idempotent (subsequent subscriptions to the same Mono, or re-invocations of the method
     * are NO-OP).
     *
     * @return a {@link Mono} that will complete empty when the object has been released. In case of an error the object
     * is always discarded.
     */
    Mono<Void> release();

    /**
     * Return a {@link Mono} that triggers the asynchronous invalidation of the {@code POOLABLE} when subscribed.
     * The object is always discarded by the pool and not further reused.
     * <p>
     * This is useful when the unhealthy state of the resource (or lack of re-usability) is detected through the usage of
     * the resource, as opposed to its exposed state.
     */
    Mono<Void> invalidate();

    /**
     * Return the number of times the underlying pooled object has been used by consumers of the {@link Pool}, via
     * either of {@link Pool#acquire()} or {@link Pool#acquireInScope(Function)}. The first time an object is allocated, this
     * method returns {@literal 1}, so the number of times it has been "recycled" can be deduced as {@code acquireCount() - 1}.
     *
     * @return the number of times this object has been used by consumers of the pool
     */
    int acquireCount();

    /**
     * Returns the age of the {@link PooledRef}: the wall-clock time (in milliseconds) since which the underlying object
     * has been allocated.
     *
     * @return the wall-clock age (time since allocation) of the underlying object in milliseconds
     */
    long timeSinceAllocation();

}
