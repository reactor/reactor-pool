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

import java.time.Duration;
import java.util.function.Predicate;

/**
 * {@link EvictionStrategies} are {@link Predicate} on a {@link PooledRef} that return {@literal true} if the object held
 * by the {@link PooledRef} should be discarded instead of recycled when released back to the {@link Pool}.
 *
 * @author Simon Baslé
 */
public final class EvictionStrategies {

    /**
     * Return a {@link Predicate} that matches {@link PooledRef} of resources that were created at a time before the
     * {@code ttl} {@link Duration}.
     * Such objects are to be discarded instead of recycled when released back to the {@link Pool}.
     *
     * @param ttl the {@link Duration} after which an object should not be recycled (resolution: ms)
     * @return the ttl eviction strategy
     */
    public static Predicate<PooledRef<?>> agedMoreThan(Duration ttl) {
        return slot -> slot.age() >= ttl.toMillis();
    }

    /**
     * Return a {@link Predicate} that matches {@link PooledRef} which acquire counter is greater than or equal to the
     * {@code acquireMaxInclusive} int. Such objects are to be destroyed instead of recycled when released back to the
     * {@link Pool}.
     *
     * @param acquireMaxInclusive the number of acquires after which an object should not be recycled
     * @return the acquireMax eviction strategy
     */
    public static Predicate<PooledRef<?>> acquired(int acquireMaxInclusive) {
        return slot -> slot.acquireCount() >= acquireMaxInclusive;
    }
}
