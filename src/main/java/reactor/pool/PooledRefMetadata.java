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

import java.util.function.Function;

/**
 * Metadata about a {@link PooledRef}, which includes monotonic metrics like its age
 * ({@link #lifeTime()}), number of times it was reused ({@link #acquireCount()}), as well
 * as contextually changing metrics like the duration for which it has been idle in a pool
 * ({@link #idleTime()}).
 *
 * @author Simon Baslé
 */
public interface PooledRefMetadata {

	/**
	 * Return the number of times the underlying pooled object has been used by consumers of the {@link Pool}, via
	 * either of {@link Pool#acquire()} or {@link Pool#acquireInScope(Function)}. The first time an object is allocated, this
	 * method returns {@literal 1}, so the number of times it has been "recycled" can be deduced as {@code acquireCount() - 1}.
	 *
	 * @return the number of times this object has been used by consumers of the pool
	 */
	int acquireCount();

	/**
	 * Returns the wall-clock number of milliseconds since the reference was last released (or allocated, if it was
	 * never released). Can be used on resources that are not currently acquired to detect idle resources.
	 * A {@link PooledRef} that is currently acquired is required to return {@literal 0L}.
	 *
	 * @return the wall-clock number of milliseconds since the reference was last released (or allocated, if it was never released)
	 */
	/*
	 * Design notes:
	 * This can be useful to do active idle eviction (eg. some loadbalancers will terminate a TCP connection unilaterally after x minutes).
	 *
	 * The evictionPredicate from the PoolConfig can look at this time even in the release phase, because it MUST be reset
	 * to 0L before the application of the recycler function and evictionPredicate itself, so it will always look "fresh".
	 *
	 * Eviction can happen when an acquire() encounters an available element that is detected as idle.
	 * It could then either:
	 *   - only remove that element and call the allocator
	 * OR
	 *   - continuously loop until it finds a valid available element, only calling the allocator when it ends up finding no valid element
	 *
	 * Another possibility is to use a reaper thread that actively removes idle resources from the available set (but that would need some more synchronization).s
	 */
	long idleTime();

	/**
	 * Returns the age of the {@link PooledRef}: the wall-clock time (in milliseconds) since which the underlying object
	 * has been allocated.
	 *
	 * @return the wall-clock age (time since allocation) of the underlying object in milliseconds
	 */
	long lifeTime();
}
