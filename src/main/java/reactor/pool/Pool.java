/*
 * Copyright (c) 2018-Present Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.pool;

import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.function.Function;

/**
 * A reactive pool of objects.
 * @author Simon Baslé
 */
public interface Pool<POOLABLE> extends Disposable {

    /**
     * Manually acquire a {@code POOLABLE} from the pool upon subscription and become responsible for its release.
     * The object is wrapped in a {@link PooledRef} which can be used for manually releasing the object back to the pool
     * or invalidating it. As a result, you MUST maintain a reference to it throughout the code that makes use of the
     * underlying resource.
     * <p>
     * This is typically the case when one needs to wrap the actual resource into a decorator version, where the reference
     * to the {@link PooledRef} can be stored. On the other hand, if the resource and its usage directly expose reactive
     * APIs, you might want to prefer to use {@link #acquireInScope(Function)}.
     * <p>
     * The resulting {@link Mono} emits the {@link PooledRef} as the {@code POOLABLE} becomes available. Cancelling the
     * {@link org.reactivestreams.Subscription} before the {@code POOLABLE} has been emitted will either avoid object
     * acquisition entirely or will translate to a {@link PooledRef#release() release} of the {@code POOLABLE}.
     * Once the resource is emitted though, it is the responsibility of the caller to release the poolable object via
     * the {@link PooledRef} {@link PooledRef#release() release methods} when the resource is not used anymore
     * (directly OR indirectly, eg. the results from multiple statements derived from a DB connection type of resource
     * have all been processed).
     *
     * @return a {@link Mono}, each subscription to which represents an individual act of acquiring a pooled object and
     * manually managing its lifecycle from there on
     * @see #acquireInScope(Function)
     */
    Mono<PooledRef<POOLABLE>> acquire();

    /**
     * Manually acquire a {@code POOLABLE} from the pool upon subscription and become responsible for its release.
     * The provided {@link Duration} acts as a timeout that only applies if the acquisition is added to the pending
     * queue, i.e. there is no idle resource and no new resource can be created currently, so one needs to wait
     * for a release before a resource can be delivered. For a timeout that covers both this pending case and the
     * time it would take to allocate a new resource, simply apply the {@link Mono#timeout(Duration)} operator to
     * the returned Mono. For a timeout that only applies to resource allocation, build the pool with the standard
     * {@link Mono#timeout(Duration)} operator chained to the {@link PoolBuilder#from(Publisher) allocator}.
     * <p>
     * The object is wrapped in a {@link PooledRef} which can be used for manually releasing the object back to the pool
     * or invalidating it. As a result, you MUST maintain a reference to it throughout the code that makes use of the
     * underlying resource.
     * <p>
     * This is typically the case when one needs to wrap the actual resource into a decorator version, where the reference
     * to the {@link PooledRef} can be stored. On the other hand, if the resource and its usage directly expose reactive
     * APIs, you might want to prefer to use {@link #acquireInScope(Function)}.
     * <p>
     * The resulting {@link Mono} emits the {@link PooledRef} as the {@code POOLABLE} becomes available. Cancelling the
     * {@link org.reactivestreams.Subscription} before the {@code POOLABLE} has been emitted will either avoid object
     * acquisition entirely or will translate to a {@link PooledRef#release() release} of the {@code POOLABLE}.
     * Once the resource is emitted though, it is the responsibility of the caller to release the poolable object via
     * the {@link PooledRef} {@link PooledRef#release() release methods} when the resource is not used anymore
     * (directly OR indirectly, eg. the results from multiple statements derived from a DB connection type of resource
     * have all been processed).
     *
     * @return a {@link Mono}, each subscription to which represents an individual act of acquiring a pooled object and
     * manually managing its lifecycle from there on
     * @see #acquireInScope(Function)
     */
    Mono<PooledRef<POOLABLE>> acquire(Duration timeout);

    /**
     * Acquire a {@code POOLABLE} object from the pool upon subscription and declaratively use it, automatically releasing
     * the object back to the pool once the derived usage pipeline terminates or is cancelled. This acquire-use-and-release
     * scope is represented by a user provided {@link Function}.
     * <p>
     * This is typically useful when the resource (and its usage patterns) directly involve reactive APIs that can be
     * composed within the {@link Function} scope.
     * <p>
     * The {@link Mono} provided to the {@link Function} emits the {@code POOLABLE} as it becomes available. Cancelling
     * the {@link org.reactivestreams.Subscription} before the {@code POOLABLE} has been emitted will either avoid object
     * acquisition entirely or will translate to a {@link PooledRef#release() release} of the {@code POOLABLE}.
     *
     * @param scopeFunction the {@link Function} to apply to the {@link Mono} delivering the POOLABLE to instantiate and
     *                      trigger a processing pipeline around it.
     * @return a {@link Flux}, each subscription to which represents an individual act of acquiring a pooled object,
     * processing it as declared in {@code scopeFunction} and automatically releasing it.
     * @see #acquire()
     */
    default <V> Flux<V> acquireInScope(Function<Mono<POOLABLE>, Publisher<V>> scopeFunction) {
        return Flux.usingWhen(acquire(),
                slot -> scopeFunction.apply(Mono.justOrEmpty(slot.poolable())),
                PooledRef::release,
                PooledRef::release);
    }

}
