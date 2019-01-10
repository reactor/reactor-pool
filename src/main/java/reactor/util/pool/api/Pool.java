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

import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Function;

/**
 * A reactive pool of objects.
 * @author Simon Baslé
 */
public interface Pool<POOLABLE> extends Disposable {

    /**
     * Acquire a {@code POOLABLE}: borrow it from the pool upon subscription and become responsible for its release.
     * The object is wrapped in a {@link PoolSlot} which can be used for manually releasing the object back to the pool
     * or invalidating it.
     * <p>
     * The resulting {@link Mono} emits the {@link PoolSlot} as the {@code POOLABLE} becomes available. Cancelling the
     * {@link org.reactivestreams.Subscription} before the {@code POOLABLE} has been emitted will either avoid object
     * borrowing entirely or will result in immediate {@link PoolConfig#cleaner() cleanup} of the {@code POOLABLE}.
     * It is the responsibility of the caller to release the poolable object via the {@link PoolSlot} when the object
     * is not used anymore.
     *
     * @return a {@link Mono}, each subscription to which represents the act of borrowing a pooled object and manually
     * managing its lifecycle from there
     * @see #borrowInScope(Function)
     */
    Mono<PoolSlot<POOLABLE>> borrow();

    /**
     * Borrow a {@code POOLABLE} object from the pool upon subscription and declaratively use it, automatically releasing
     * the object back to the pool once the derived usage pipeline terminates or is cancelled.
     * <p>
     * The {@link Mono} provided to the {@link Function} emits the {@code POOLABLE} as it becomes available. Cancelling
     * the {@link org.reactivestreams.Subscription} before the {@code POOLABLE} has been emitted will either avoid object
     * borrowing entirely or will result in immediate {@link PoolConfig#cleaner() cleanup} of the {@code POOLABLE}.
     * <p>
     * The {@link Function} is a declarative way of implementing a processing pipeline on top of the poolable object,
     * at the end of which the object is automatically released.
     *
     * @param processingFunction the {@link Function} to apply to the {@link Mono} delivering the POOLABLE to instantiate
     *                           and trigger a processing pipeline around it.
     * @return a {@link Flux}, each subscription to which represents the act of borrowing a pooled object, processing it
     * as declared in {@code processingFunction} and automatically releasing it.
     * @see #borrow()
     */
    default <V> Flux<V> borrowInScope(Function<Mono<POOLABLE>, Publisher<V>> processingFunction) {
        return Flux.usingWhen(borrow(),
                slot -> processingFunction.apply(Mono.justOrEmpty(slot.poolable())),
                PoolSlot::releaseMono,
                PoolSlot::releaseMono);
    }

}
