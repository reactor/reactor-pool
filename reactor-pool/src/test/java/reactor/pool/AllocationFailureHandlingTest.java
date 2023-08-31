/*
 * Copyright (c) 2020-2023 VMware Inc. or its affiliates, All Rights Reserved.
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

import org.assertj.core.api.WithAssertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.concurrent.atomic.AtomicBoolean;

public class AllocationFailureHandlingTest implements WithAssertions {
    @Test
    void test() {
        final AtomicBoolean canAllocateResource = new AtomicBoolean(true);
        final Mono<String> allocator = Mono.defer(() ->
            canAllocateResource.get() ?
                Mono.just("value") :
                Mono.error(new IllegalStateException("Can't allocate"))
        );

        final InstrumentedPool<String> pool = PoolBuilder
            .from(allocator)
            .maxPendingAcquireUnbounded()
            .sizeBetween(10, 10) // Spring Boot R2DBC connections pool default
            .buildPool();

        // New empty pool. No resources allocated yet, but has min-size (10) permits
        assertThat(pool.config().allocationStrategy().estimatePermitCount()).isEqualTo(10);
        assertThat(pool.metrics().idleSize()).isEqualTo(0);

        // Try to acquire one resource. This should trigger pool "warmup" to min-size of resources
        StepVerifier.create(pool.acquire().flatMap(PooledRef::release)).verifyComplete();
        assertThat(pool.config().allocationStrategy().estimatePermitCount()).isEqualTo(0);
        assertThat(pool.metrics().idleSize()).isEqualTo(10);

        // Now allocator will return errors (simulating inaccessible DB server for R2DBC connections pool)
        canAllocateResource.set(false);

        // We have 10 allocated resources in the pool, but they are not valid anymore, so invalidate them
        StepVerifier.create(Flux.range(0, 10).concatMap(ignore -> pool.acquire().flatMap(PooledRef::invalidate)))
            .verifyComplete();
        assertThat(pool.metrics().idleSize()).isEqualTo(0);
        assertThat(pool.config().allocationStrategy().estimatePermitCount()).isEqualTo(10);

        // Now we have empty pool, so it should be warmed up again but allocator still not working
        StepVerifier.create(pool.acquire()).verifyError();
        assertThat(pool.metrics().idleSize()).isEqualTo(0);
        assertThat(pool.config().allocationStrategy().estimatePermitCount()).isEqualTo(10); // Oops!
    }
}
