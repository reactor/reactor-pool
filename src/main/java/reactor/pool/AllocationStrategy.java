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

import reactor.pool.util.AllocationStrategies;

/**
 * A strategy guiding the {@link Pool} on whether or not it is possible to invoke the resource allocator.
 * <p>
 * See {@link AllocationStrategies} for pre-made strategies.
 *
 * @author Simon Baslé
 */
public interface AllocationStrategy {

    /**
     * Try to get the permission to allocate a {@code desired} positive number of new resources. Returns the permissible
     * number of resources which MUST be created (otherwise the internal live counter of the strategy might be off).
     * This permissible number might be zero. Once a resource is discarded from the pool, it must
     * update the strategy using {@link #returnPermits(int)} (which can happen in batches or with value {@literal 1}).
     *
     * @param desired the desired number of new resources
     * @return the acceptable number of new resources, might be zero
     */
    int getPermits(int desired);

    /**
     * Best-effort peek at the state of the strategy which indicates roughly how many more resources can currently be
     * allocated. Should be paired with {@link #getPermits(int)} for an atomic permission.
     *
     * @return an ESTIMATED count of how many more resources can currently be allocated
     */
    int estimatePermitCount();

    /**
     * Update the strategy to indicate that N resources were discarded from the {@link Pool}, potentially leaving space
     * for N new ones to be allocated. Users MUST ensure that this method isn't called with a value greater than the
     * number of held permits it has.
     */
    void returnPermits(int returned);
}
