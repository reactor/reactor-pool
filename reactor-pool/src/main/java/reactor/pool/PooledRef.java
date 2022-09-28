/*
 * Copyright (c) 2018-2021 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.pool;

import reactor.core.publisher.Mono;

import java.util.function.Function;

/**
 * An abstraction over an object in a {@link Pool}, which holds the underlying {@code POOLABLE} object and allows one to
 * manually {@link #release()} it to the pool or {@link #invalidate()} it. Through its {@link #metadata()},
 * the {@link PooledRef} provides a few additional information about its lifecycle, like its age and the number of times
 * it has been acquired, so the {@link Pool} may optionally use that information to automatically invalidate
 * the object, and provide a simplified acquire mechanism where the {@link PooledRef} is not directly exposed (see
 * {@link Pool#withPoolable(Function)} vs {@link Pool#acquire()}).
 *
 * @author Simon Baslé
 */
public interface PooledRef<POOLABLE> {

    // == Data ==

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
     * Returns a {@link PooledRefMetadata} object that holds more information about the <strong>reference</strong> (rather
     * than the {@link #poolable()}), like how many times it was acquired, its age (time since allocation), etc...
     * <p>
     * Note that if {@link #release()} or {@link #invalidate()} have been invoked, this method might return stale information.
     *
     * @return the {@link PooledRefMetadata} for this {@link PooledRef}
     */
    PooledRefMetadata metadata();

    // == Actions ==

    /**
     * Return a {@link Mono} that triggers the asynchronous invalidation of the {@code POOLABLE} when subscribed.
     * The object is always discarded by the pool and not further reused.
     * <p>
     * This is useful when the unhealthy state of the resource (or lack of re-usability) is detected through the usage of
     * the resource, as opposed to its exposed state.
     * <p>
     * Note that if {@link #release()} has been invoked, this method should be no-op.
     */
    Mono<Void> invalidate();

    /**
     * Return a {@link Mono} that, once subscribed, will release the {@code POOLABLE} back to the pool asynchronously.
     * Once released, a reference is considered out of the control of the user and shouldn't be usable anymore.
     * It is discouraged to use the underlying {@link #poolable()} at this point, {@link #metadata()} might become stale,
     * and usage of both {@link #invalidate()} and release() should be no-op.
     * <p>
     * This method is idempotent (subsequent subscriptions to the same Mono, or re-invocations of the method
     * are NO-OP). It is also no-op if {@link #invalidate()} has previously been invoked.
     *
     * @return a {@link Mono} that will complete empty when the object has been released. In case of an error the object
     * is always discarded
     */
    Mono<Void> release();

}
