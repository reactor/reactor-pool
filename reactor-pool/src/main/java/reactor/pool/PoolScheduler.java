/*
 * Copyright (c) 2024 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.List;

/**
 * A Pool that can schedule resource acquisition among multiple sub pools,
 * each managing a portion of resources. Resource acquisitions will
 * be concurrently distributed across sub pools using sub pool executors, in a work stealing style.
 */
public interface PoolScheduler<T> extends InstrumentedPool<T> {
    /**
     * Get the number of borrowers steal count (only if the Scheduler supports work stealing).
     *
     * @return the number of Pool steal count, or -1
     */
    long stealCount();

    /**
     * Returns the number of sub pools managed this this scheduler.
     * @return the number of sub pools managed this this scheduler
     */
    List<InstrumentedPool<T>> getPools();
}
