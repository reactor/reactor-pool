/*
 * Copyright (c) 2011-2019 Pivotal Software Inc, All Rights Reserved.
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

package reactor.util.pool.metrics;

/**
 * An interface representing ways for {@link reactor.util.pool.api.Pool} to collect instrumentation data.
 * Some methods are pool-implementation specific.
 * <p>
 * Additionally wraps the concept of a clock, with {@link #now()} to get the current time with milliseconds
 * resolution and {@link #measureTime(long)} to get the elapsed time.
 *
 * @author Simon Baslé
 */
public interface MetricsRecorder {

    /**
     * Get a starting time with milliseconds resolution.
     */
    long now();

    /**
     * Get the elapsed time in milliseconds between {@link #now()} and the given starting time.
     *
     * @param startTimeMillis the starting time initially obtained via {@link #now()}
     * @return the elapsed time in milliseconds
     */
    long measureTime(long startTimeMillis);

    /**
     * Record a latency for successful allocation. Implies incrementing an allocation success counter as well.
     * @param latencyMs the latency in milliseconds
     */
    void recordAllocationSuccessAndLatency(long latencyMs);

    /**
     * Record a latency for failed allocation. Implies incrementing an allocation failure counter as well.
     * @param latencyMs the latency in milliseconds
     */
    void recordAllocationFailureAndLatency(long latencyMs);

    /**
     * Record a latency for resetting a resource to a reusable state. Implies incrementing a counter as well.
     * @param latencyMs the latency in milliseconds
     */
    void recordResetLatency(long latencyMs);

    /**
     * Record a latency for destroying a resource. Implies incrementing a counter as well.
     * @param latencyMs the latency in milliseconds
     */
    void recordDestroyLatency(long latencyMs);

    /**
     * Record the fact that a resource was recycled, ie it was reset and tested for reuse.
     */
    void recordRecycled();

}
