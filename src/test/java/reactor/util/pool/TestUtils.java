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

package reactor.util.pool;

import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.util.pool.api.AllocationStrategies;
import reactor.util.pool.api.PoolConfig;
import reactor.util.pool.api.PoolConfigBuilder;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * @author Simon Baslé
 */
public class TestUtils {

    public static final class PoolableTest implements Disposable {

        private static AtomicInteger defaultId = new AtomicInteger();

        public int usedUp;
        public int discarded;
        public final int id;

        public PoolableTest() {
            this(defaultId.incrementAndGet());
        }

        public PoolableTest(int id) {
            this.id = id;
            this.usedUp = 0;
        }

        void clean() {
            this.usedUp++;
        }

        public boolean isHealthy() {
            return usedUp < 2;
        }

        @Override
        public void dispose() {
            discarded++;
        }

        @Override
        public boolean isDisposed() {
            return discarded > 0;
        }

        @Override
        public String toString() {
            return "PoolableTest{id=" + id + ", used=" + usedUp + "}";
        }
    }

    public static final PoolConfig<PoolableTest> poolableTestConfig(int minSize, int maxSize, Mono<PoolableTest> allocator) {
        return PoolConfigBuilder.allocateWith(allocator)
                .initialSizeOf(minSize)
                .witAllocationLimit(AllocationStrategies.allocatingMax(maxSize))
                .resetResourcesWith(pt -> Mono.fromRunnable(pt::clean))
                .evictionPredicate(slot -> !slot.poolable().isHealthy())
                .buildConfig();
    }

    public static final PoolConfig<PoolableTest> poolableTestConfig(int minSize, int maxSize, Mono<PoolableTest> allocator, Scheduler deliveryScheduler) {
        return PoolConfigBuilder.allocateWith(allocator)
                .initialSizeOf(minSize)
                .witAllocationLimit(AllocationStrategies.allocatingMax(maxSize))
                .resetResourcesWith(pt -> Mono.fromRunnable(pt::clean))
                .evictionPredicate(slot -> !slot.poolable().isHealthy())
                .deliveryScheduler(deliveryScheduler)
                .buildConfig();
    }

    public static final PoolConfig<PoolableTest> poolableTestConfig(int minSize, int maxSize, Mono<PoolableTest> allocator,
                                                                    Consumer<? super PoolableTest> additionalCleaner) {
        return PoolConfigBuilder.allocateWith(allocator)
                .initialSizeOf(minSize)
                .witAllocationLimit(AllocationStrategies.allocatingMax(maxSize))
                .resetResourcesWith(poolableTest -> Mono.fromRunnable(() -> {
                    poolableTest.clean();
                    additionalCleaner.accept(poolableTest);
                }))
                .evictionPredicate(slot -> !slot.poolable().isHealthy())
                .buildConfig();
    }
}
