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
import java.util.function.BiPredicate;
import java.util.function.Predicate;

/**
 * {@link EvictionPredicates} are {@link Predicate} on a {@link PooledRef} that return {@literal true} if the object held
 * by the {@link PooledRef} should be discarded instead of recycled when released back to the {@link Pool}.
 *
 * @author Simon Baslé
 */
abstract class EvictionPredicates {

    private EvictionPredicates(){}

    /**
     * Return a {@link Predicate} that matches {@link PooledRef} of resources that have been idle (ie released and
     * available in the {@link Pool}) for more than the {@code ttl} {@link Duration} (inclusive).
     * Such a predicate could be used to evict too idle objects when next encountered by an {@link Pool#acquire()}.
     *
     * @param ttl the {@link Duration} after which an object should not be passed to a borrower, but destroyed (resolution: ms)
     * @return the idle ttl eviction strategy
     */
    static <T> BiPredicate<T, PoolableMetrics> idleMoreThan(Duration ttl) {
        return (it, metrics) -> metrics.idleTime() >= ttl.toMillis();
    }
}
