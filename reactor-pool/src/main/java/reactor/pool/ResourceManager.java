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

/**
 * A resource manager utilized by concrete Pool implementations. This manager enables Pools to interact
 * with its pool scheduler, if there is one enabled.
 * Pools can access their resource manager via the {@link PoolConfig#resourceManager()} method.
 */
public interface ResourceManager {
    /**
     * Notifies the pool scheduler that some resources can be acquired from the current pool because either certain
     * resources are currently estimated to be idle or available for allocation.
     */
    void resourceAvailable();
}
