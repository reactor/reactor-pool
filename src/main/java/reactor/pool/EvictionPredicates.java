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

import java.time.Duration;
import java.util.function.Predicate;

/**
 * {@link EvictionPredicates} are {@link Predicate} on a {@link PooledRef} that return {@literal true} if the object held
 * by the {@link PooledRef} should be discarded instead of recycled when released back to the {@link Pool}.
 *
 * @author Simon Baslé
 */
//TODO should be provided by Builder methods directly ?
final class EvictionPredicates {

    /**
     * Return a {@link Predicate} that matches {@link PooledRef} of resources that were created more than {@code ttl}
     * milliseconds ago (inclusive, the ttl being represented as a {@link Duration} but converted to ms resolution).
     * Such objects are to be discarded instead of recycled when released back to the {@link Pool}.
     *
     * @param ttl the {@link Duration} after which an object should not be recycled (resolution: ms)
     * @return the allocation ttl eviction strategy
     */
    //TODO do we need that for a start
    static <T> Predicate<PooledRef<T>> agedMoreThan(Duration ttl) {
        return slot -> slot instanceof PooledRefMetrics && ((PooledRefMetrics)slot).timeSinceAllocation() >= ttl.toMillis();
    }

    /**
     * Return a {@link Predicate} that matches {@link PooledRef} of resources that have been idle (ie released and
     * available in the {@link Pool}) for more than the {@code ttl} {@link Duration} (inclusive).
     * Such a predicate could be used to evict too idle objects when next encountered by an {@link Pool#acquire()}.
     *
     * @param ttl the {@link Duration} after which an object should not be passed to a borrower, but destroyed (resolution: ms)
     * @return the idle ttl eviction strategy
     */
    static <T> Predicate<PooledRef<T>> idleMoreThan(Duration ttl) {
        return slot -> slot instanceof PooledRefMetrics && ((PooledRefMetrics)slot).timeSinceRelease() >= ttl.toMillis();
    }

    /**
     * Return a {@link Predicate} that matches {@link PooledRef} which acquire counter is greater than or equal to the
     * {@code acquireMaxInclusive} int. Such objects are to be destroyed instead of recycled when released back to the
     * {@link Pool}.
     *
     * @param acquireMaxInclusive the number of acquires after which an object should not be recycled
     * @return the acquireMax eviction strategy
     */
    //TODO do we need that for a start
    static <T> Predicate<PooledRef<T>> acquiredMoreThan(int acquireMaxInclusive) {
        return slot -> slot instanceof PooledRefMetrics && ((PooledRefMetrics)slot).acquireCount() >= acquireMaxInclusive;
    }

    static <T> Predicate<PooledRef<T>> poolableMatches(Predicate<? super T> poolablePredicate) {
        return slot -> poolablePredicate.test(slot.poolable());
    }
}
