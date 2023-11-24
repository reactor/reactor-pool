/*
 * Copyright (c) 2018-2024 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.time.Duration;
import java.util.function.Function;

import org.reactivestreams.Publisher;

import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * A reactive pool of objects.
 * @author Simon Baslé
 */
public interface Pool<POOLABLE> extends Disposable {

	/**
	 * Warms up the {@link Pool}, if needed. This typically instructs the pool to check for a minimum size and allocate
	 * necessary objects when the minimum is not reached. The resulting {@link Mono} emits the number of extra resources
	 * it created as a result of the {@link PoolBuilder#sizeBetween(int, int) allocation minimum}.
	 * <p>
	 * Note that no work nor allocation is performed until the {@link Mono} is subscribed to.
	 * <p>
	 * Implementations MAY include more behavior, but there is no restriction on the way this method is called by users
	 * (it should be possible to call it at any time, as many times as needed or not at all).
	 *
	 * @apiNote this API is intended to easily reach the minimum allocated size (see {@link PoolBuilder#sizeBetween(int, int)})
	 * without paying that cost on the first {@link #acquire()}. However, implementors should also consider creating
	 * the extra resources needed to honor that minimum during the acquire, as one cannot rely on the user calling
	 * warmup() consistently.
	 *
	 * @return a cold {@link Mono} that triggers resource warmup and emits the number of warmed up resources
	 */
	Mono<Integer> warmup();

	/**
	 * Manually acquire a {@code POOLABLE} from the pool upon subscription and become responsible for its release.
	 * The object is wrapped in a {@link PooledRef} which can be used for manually releasing the object back to the pool
	 * or invalidating it. As a result, you MUST maintain a reference to it throughout the code that makes use of the
	 * underlying resource.
	 * <p>
	 * This is typically the case when one needs to wrap the actual resource into a decorator version, where the reference
	 * to the {@link PooledRef} can be stored. On the other hand, if the resource and its usage directly expose reactive
	 * APIs, you might want to prefer to use {@link #withPoolable(Function)}.
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
	 * @see #withPoolable(Function)
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
	 * APIs, you might want to prefer to use {@link #withPoolable(Function)}.
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
	 * @see #withPoolable(Function)
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
	 *                      trigger a processing pipeline around it
	 * @return a {@link Flux}, each subscription to which represents an individual act of acquiring a pooled object,
	 * processing it as declared in {@code scopeFunction} and automatically releasing it
	 * @see #acquire()
	 */
	default <V> Flux<V> withPoolable(Function<POOLABLE, Publisher<V>> scopeFunction) {
		return Flux.usingWhen(
				acquire(),
				slot -> { POOLABLE poolable = slot.poolable();
				if (poolable == null) return Mono.empty();
				return scopeFunction.apply(poolable);
				},
				PooledRef::release,
				(ref, error) -> ref.release(),
				PooledRef::release);
	}

	/**
	 * Return the pool's {@link PoolConfig configuration}.
	 * @implNote Starting at 1.0.x, implementors MUST return a configuration.
	 *
	 * @return the {@link PoolConfig}
	 */
	PoolConfig<POOLABLE> config();

	/**
	 * Shutdown the pool by:
	 * <ul>
	 *     <li>
	 *         notifying every acquire still pending that the pool has been shut down,
	 *         via a {@link RuntimeException}
	 *     </li>
	 *     <li>
	 *         releasing each pooled resource, according to the release handler defined in
	 *         the {@link PoolBuilder}
	 *     </li>
	 * </ul>
	 * This imperative style method returns once every release handler has been started in
	 * step 2, but doesn't necessarily block until full completion of said releases.
	 * For a blocking alternative, use {@link #disposeLater()} and {@link Mono#block()}.
	 * <p>
	 * By default this is implemented as {@code .disposeLater().subscribe()}. As a result
	 * failures during release could be swallowed.
	 */
	@Override
	default void dispose() {
		disposeLater().subscribe();
	}

	/**
	 * Returns a {@link Mono} that represents a lazy asynchronous shutdown of this {@link Pool}.
	 * Shutdown doesn't happen until the {@link Mono} is {@link Mono#subscribe() subscribed}.
	 * Otherwise, it performs the same steps as in the imperative counterpart, {@link #dispose()}.
	 * <p>
	 * If the pool has been already shut down, returns {@link Mono#empty()}. Completion of
	 * the {@link Mono} indicates completion of the shutdown process.
	 *
	 * @return a Mono triggering the shutdown of the pool once subscribed.
	 */
	Mono<Void> disposeLater();

	/**
	 * Transfer some pending borrowers from another pool into this pool.
	 *
	 * @param from another pool to steal resources from
	 * @return true if some borrowers have been moved from <code>from</code> into this pool instance
	 */
	default boolean transferBorrowersFrom(InstrumentedPool<POOLABLE> from) {
		return false;
	}
}
