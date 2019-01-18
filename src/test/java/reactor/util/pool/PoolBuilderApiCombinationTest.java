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

package reactor.util.pool;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.pool.api.PoolConfig;
import reactor.util.pool.api.PooledRef;
import reactor.util.pool.builder.PoolBuilder;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static reactor.util.pool.api.EvictionStrategies.agedMoreThan;
import static reactor.util.pool.api.EvictionStrategies.acquired;

//FIXME test each pool configuration combination?
//intentionally one package lower than necessary to ensure correct visibility of APIs vs package-private implementations
class PoolBuilderApiCombinationTest {

    /**
     * Fake {@link PooledRef} for tests, either wrapping value+timeSinceAllocation+acquireCount or just a
     * value (in which case timeSinceAllocation defaults to 12s and acquireCount to 1000)
     * @param <T>
     */
    final class TestPooledRef<T> implements PooledRef<T> {

        final T poolable;
        final long age;
        final int acquireCount;

        TestPooledRef(T poolable, long age, int acquireCount) {
            this.poolable = poolable;
            this.age = age;
            this.acquireCount = acquireCount;
        }

        TestPooledRef(T poolable) {
            this(poolable, 12_000, 1000);
        }

        @Override
        public T poolable() {
            return poolable;
        }

        @Override
        public Mono<Void> release() {
            return Mono.empty();
        }

        @Override
        public Mono<Void> invalidate() {
            return Mono.empty();
        }

        @Override
        public int acquireCount() {
            return acquireCount;
        }

        @Override
        public long timeSinceAllocation() {
            return age;
        }

        @Override
        public String toString() {
            return "TestPooledRef{" +
                    "poolable=" + poolable +
                    ", timeSinceAllocation=" + age +
                    ", acquireCount=" + acquireCount +
                    '}';
        }
    }

    @Test
    @DisplayName("PoolablePredicate AllocatingBetween")
    void builderCombinationMinimal1() {
        final PoolConfig<List<String>> config = PoolBuilder.allocatingWith(Mono.<List<String>>fromCallable(ArrayList::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessPoolableMatches(list -> list.size() >= 2)
                .allocatingBetween(1, 10)
                .toConfig();

        assertThat(config.initialSize()).as("initialSize").isEqualTo(1);
        assertThat(config.maxSize()).as("maxSize").isEqualTo(10);
        assertThat(config.deliveryScheduler()).as("scheduler").isSameAs(Schedulers.immediate());
        assertThat(config.evictionPredicate()).as("evictionPredicate")
                .rejects(new TestPooledRef<>(Collections.emptyList()))
                .rejects(new TestPooledRef<>(Collections.singletonList("A")))
                .accepts(new TestPooledRef<>(Arrays.asList("A", "B")))
                .accepts(new TestPooledRef<>(Arrays.asList("A", "B", "C")));
    }

    @Test
    @DisplayName("PoolablePredicate AllocatingMax")
    void builderCombinationMinimal2() {
        final PoolConfig<List<String>> config = PoolBuilder.allocatingWith(Mono.<List<String>>fromCallable(ArrayList::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessPoolableMatches(list -> list.size() >= 2)
                .allocatingMax(10)
                .toConfig();

        assertThat(config.initialSize()).as("initialSize").isEqualTo(0);
        assertThat(config.maxSize()).as("maxSize").isEqualTo(10);
        assertThat(config.deliveryScheduler()).as("scheduler").isSameAs(Schedulers.immediate());
        assertThat(config.evictionPredicate()).as("evictionPredicate")
                .rejects(new TestPooledRef<>(Collections.emptyList()))
                .rejects(new TestPooledRef<>(Collections.singletonList("A")))
                .accepts(new TestPooledRef<>(Arrays.asList("A", "B")))
                .accepts(new TestPooledRef<>(Arrays.asList("A", "B", "C")));
    }

    @Test
    @DisplayName("PooledRefPredicate AllocatingBetween")
    void builderCombinationMinimal3() {
        final PoolConfig<List<String>> config = PoolBuilder.allocatingWith(Mono.<List<String>>fromCallable(ArrayList::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessRefMatches(acquired(2))
                .allocatingBetween(1, 10)
                .toConfig();

        assertThat(config.initialSize()).as("initialSize").isEqualTo(1);
        assertThat(config.maxSize()).as("maxSize").isEqualTo(10);
        assertThat(config.deliveryScheduler()).as("scheduler").isSameAs(Schedulers.immediate());
        assertThat(config.evictionPredicate()).as("evictionPredicate")
                .accepts(new TestPooledRef<>(Collections.emptyList()))
                .accepts(new TestPooledRef<>(Arrays.asList("A", "B", "C"), 10_000, 2))
                .accepts(new TestPooledRef<>(Arrays.asList("A", "B", "C"), 10_000, 3))
                .rejects(new TestPooledRef<>(Collections.emptyList(), 10_000, 1));
    }

    @Test
    @DisplayName("PooledRefPredicate AllocatingMax")
    void builderCombinationMinimal4() {
        final PoolConfig<List<String>> config = PoolBuilder.allocatingWith(Mono.<List<String>>fromCallable(ArrayList::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessRefMatches(acquired(2))
                .allocatingMax(10)
                .toConfig();

        assertThat(config.initialSize()).as("initialSize").isEqualTo(0);
        assertThat(config.maxSize()).as("maxSize").isEqualTo(10);
        assertThat(config.deliveryScheduler()).as("scheduler").isSameAs(Schedulers.immediate());
        assertThat(config.evictionPredicate()).as("evictionPredicate")
                .accepts(new TestPooledRef<>(Collections.emptyList()))
                .accepts(new TestPooledRef<>(Arrays.asList("A", "B", "C"), 10_000, 2))
                .accepts(new TestPooledRef<>(Arrays.asList("A", "B", "C"), 10_000, 3))
                .rejects(new TestPooledRef<>(Collections.emptyList(), 10_000, 1));
    }

    @Test
    @DisplayName("PoolablePredicate Scheduler AllocatingBetween")
    void builderCombinationWithScheduler1() {
        final PoolConfig<List<String>> config = PoolBuilder.allocatingWith(Mono.<List<String>>fromCallable(ArrayList::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessPoolableMatches(list -> list.size() >= 2)
                .publishOn(Schedulers.parallel())
                .allocatingBetween(1, 10)
                .toConfig();

        assertThat(config.initialSize()).as("initialSize").isEqualTo(1);
        assertThat(config.maxSize()).as("maxSize").isEqualTo(10);
        assertThat(config.deliveryScheduler()).as("scheduler")
                .isSameAs(Schedulers.parallel());
        assertThat(config.evictionPredicate()).as("evictionPredicate")
                .rejects(new TestPooledRef<>(Collections.emptyList()))
                .rejects(new TestPooledRef<>(Collections.singletonList("A")))
                .accepts(new TestPooledRef<>(Arrays.asList("A", "B")))
                .accepts(new TestPooledRef<>(Arrays.asList("A", "B", "C")));
    }

    @Test
    @DisplayName("PoolablePredicate Scheduler AllocatingMax")
    void builderCombinationWithScheduler2() {
        final PoolConfig<List<String>> config = PoolBuilder.allocatingWith(Mono.<List<String>>fromCallable(ArrayList::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessPoolableMatches(list -> list.size() >= 2)
                .publishOn(Schedulers.parallel())
                .allocatingMax(10)
                .toConfig();

        assertThat(config.initialSize()).as("initialSize").isEqualTo(0);
        assertThat(config.maxSize()).as("maxSize").isEqualTo(10);
        assertThat(config.deliveryScheduler()).as("scheduler")
                .isSameAs(Schedulers.parallel());
        assertThat(config.evictionPredicate()).as("evictionPredicate")
                .rejects(new TestPooledRef<>(Collections.emptyList()))
                .rejects(new TestPooledRef<>(Collections.singletonList("A")))
                .accepts(new TestPooledRef<>(Arrays.asList("A", "B")))
                .accepts(new TestPooledRef<>(Arrays.asList("A", "B", "C")));
    }

    @Test
    @DisplayName("PooledRefPredicate Scheduler AllocatingBetween")
    void builderCombinationWithScheduler3() {
        final PoolConfig<List<String>> config = PoolBuilder.allocatingWith(Mono.<List<String>>fromCallable(ArrayList::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessRefMatches(acquired(2))
                .publishOn(Schedulers.parallel())
                .allocatingBetween(1, 10)
                .toConfig();

        assertThat(config.initialSize()).as("initialSize").isEqualTo(1);
        assertThat(config.maxSize()).as("maxSize").isEqualTo(10);
        assertThat(config.deliveryScheduler()).as("scheduler")
                .isSameAs(Schedulers.parallel());
        assertThat(config.evictionPredicate()).as("evictionPredicate")
                .accepts(new TestPooledRef<>(Collections.emptyList()))
                .accepts(new TestPooledRef<>(Arrays.asList("A", "B", "C"), 10_000, 2))
                .accepts(new TestPooledRef<>(Arrays.asList("A", "B", "C"), 10_000, 3))
                .rejects(new TestPooledRef<>(Collections.emptyList(), 10_000, 1));
    }

    @Test
    @DisplayName("PooledRefPredicate Scheduler AllocatingMax")
    void builderCombinationWithScheduler4() {
        final PoolConfig<List<String>> config = PoolBuilder.allocatingWith(Mono.<List<String>>fromCallable(ArrayList::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessRefMatches(acquired(2))
                .publishOn(Schedulers.parallel())
                .allocatingMax(10)
                .toConfig();

        assertThat(config.initialSize()).as("initialSize").isEqualTo(0);
        assertThat(config.maxSize()).as("maxSize").isEqualTo(10);
        assertThat(config.deliveryScheduler()).as("scheduler")
                .isSameAs(Schedulers.parallel());
        assertThat(config.evictionPredicate()).as("evictionPredicate")
                .accepts(new TestPooledRef<>(Collections.emptyList()))
                .accepts(new TestPooledRef<>(Arrays.asList("A", "B", "C"), 10_000, 2))
                .accepts(new TestPooledRef<>(Arrays.asList("A", "B", "C"), 10_000, 3))
                .rejects(new TestPooledRef<>(Collections.emptyList(), 10_000, 1));
    }

    //second predicate, but no scheduler
    @Test
    @DisplayName("PoolablePredicate PoolableSecondPredicate AllocatingBetween")
    void builderCombinationSecondPredicate1() {
        final PoolConfig<List<String>> config = PoolBuilder.allocatingWith(Mono.<List<String>>fromCallable(ArrayList::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessPoolableMatches(list -> list.size() == 2)
                .orPoolableMatches(list -> list.size() > 2)
                .allocatingBetween(1, 10)
                .toConfig();

        assertThat(config.initialSize()).as("initialSize").isEqualTo(1);
        assertThat(config.maxSize()).as("maxSize").isEqualTo(10);
        assertThat(config.deliveryScheduler()).as("scheduler")
                .isSameAs(Schedulers.immediate());
        assertThat(config.evictionPredicate()).as("evictionPredicate")
                .rejects(new TestPooledRef<>(Collections.emptyList()))
                .rejects(new TestPooledRef<>(Collections.singletonList("A")))
                .accepts(new TestPooledRef<>(Arrays.asList("A", "B")))
                .accepts(new TestPooledRef<>(Arrays.asList("A", "B", "C")));
    }

    @Test
    @DisplayName("PoolablePredicate PoolableSecondPredicate AllocatingMax")
    void builderCombinationSecondPredicate2() {
        final PoolConfig<List<String>> config = PoolBuilder.allocatingWith(Mono.<List<String>>fromCallable(ArrayList::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessPoolableMatches(list -> list.size() == 2)
                .orPoolableMatches(list -> list.size() > 2)
                .allocatingMax(10)
                .toConfig();

        assertThat(config.initialSize()).as("initialSize").isEqualTo(0);
        assertThat(config.maxSize()).as("maxSize").isEqualTo(10);
        assertThat(config.deliveryScheduler()).as("scheduler")
                .isSameAs(Schedulers.immediate());
        assertThat(config.evictionPredicate()).as("evictionPredicate")
                .rejects(new TestPooledRef<>(Collections.emptyList()))
                .rejects(new TestPooledRef<>(Collections.singletonList("A")))
                .accepts(new TestPooledRef<>(Arrays.asList("A", "B")))
                .accepts(new TestPooledRef<>(Arrays.asList("A", "B", "C")));
    }

    @Test
    @DisplayName("PoolablePredicate PooledRefSecondPredicate AllocatingBetween")
    void builderCombinationSecondPredicate3() {
        final PoolConfig<List<String>> config = PoolBuilder.allocatingWith(Mono.<List<String>>fromCallable(ArrayList::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessPoolableMatches(list -> list.size() >= 2)
                .orRefMatches(agedMoreThan(Duration.ofSeconds(3)).and(acquired(3)))
                .allocatingBetween(1, 10)
                .toConfig();

        assertThat(config.initialSize()).as("initialSize").isEqualTo(1);
        assertThat(config.maxSize()).as("maxSize").isEqualTo(10);
        assertThat(config.deliveryScheduler()).as("scheduler")
                .isSameAs(Schedulers.immediate());
        assertThat(config.evictionPredicate()).as("evictionPredicate")
                //accept based on timeSinceAllocation and acquire
                .accepts(new TestPooledRef<>(Collections.emptyList()))
                .accepts(new TestPooledRef<>(Collections.singletonList("A")))
                //accept based on size
                .accepts(new TestPooledRef<>(Arrays.asList("A", "B"), 100, 1))
                .accepts(new TestPooledRef<>(Arrays.asList("A", "B", "C"), 100, 2))
                //reject based on both
                .rejects(new TestPooledRef<>(Collections.emptyList(), 100, 2));
    }

    @Test
    @DisplayName("PoolablePredicate PooledRefSecondPredicate AllocatingMax")
    void builderCombinationSecondPredicate4() {
        final PoolConfig<List<String>> config = PoolBuilder.allocatingWith(Mono.<List<String>>fromCallable(ArrayList::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessPoolableMatches(list -> list.size() >= 2)
                .orRefMatches(agedMoreThan(Duration.ofSeconds(3)).and(acquired(3)))
                .allocatingMax(10)
                .toConfig();

        assertThat(config.initialSize()).as("initialSize").isEqualTo(0);
        assertThat(config.maxSize()).as("maxSize").isEqualTo(10);
        assertThat(config.deliveryScheduler()).as("scheduler")
                .isSameAs(Schedulers.immediate());
        assertThat(config.evictionPredicate()).as("evictionPredicate")
                //accept based on timeSinceAllocation and acquire
                .accepts(new TestPooledRef<>(Collections.emptyList()))
                .accepts(new TestPooledRef<>(Collections.singletonList("A")))
                //accept based on size
                .accepts(new TestPooledRef<>(Arrays.asList("A", "B"), 100, 1))
                .accepts(new TestPooledRef<>(Arrays.asList("A", "B", "C"), 100, 2))
                //reject based on both
                .rejects(new TestPooledRef<>(Collections.emptyList(), 100, 2));
    }

    @Test
    @DisplayName("PooledRefPredicate PoolableSecondPredicate AllocatingBetween")
    void builderCombinationSecondPredicate5() {
        final PoolConfig<List<String>> config = PoolBuilder.allocatingWith(Mono.<List<String>>fromCallable(ArrayList::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessRefMatches(agedMoreThan(Duration.ofSeconds(3)).and(acquired(3)))
                .orPoolableMatches(list -> list.size() >= 2)
                .allocatingBetween(1, 10)
                .toConfig();

        assertThat(config.initialSize()).as("initialSize").isEqualTo(1);
        assertThat(config.maxSize()).as("maxSize").isEqualTo(10);
        assertThat(config.deliveryScheduler()).as("scheduler")
                .isSameAs(Schedulers.immediate());
        assertThat(config.evictionPredicate()).as("evictionPredicate")
                //accept based on timeSinceAllocation and acquire
                .accepts(new TestPooledRef<>(Collections.emptyList()))
                .accepts(new TestPooledRef<>(Collections.singletonList("A")))
                //accept based on size
                .accepts(new TestPooledRef<>(Arrays.asList("A", "B"), 100, 1))
                .accepts(new TestPooledRef<>(Arrays.asList("A", "B", "C"), 100, 2))
                //reject based on both
                .rejects(new TestPooledRef<>(Collections.emptyList(), 100, 2));
    }

    @Test
    @DisplayName("PooledRefPredicate PoolableSecondPredicate AllocatingMax")
    void builderCombinationSecondPredicate6() {
        final PoolConfig<List<String>> config = PoolBuilder.allocatingWith(Mono.<List<String>>fromCallable(ArrayList::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessRefMatches(agedMoreThan(Duration.ofSeconds(3)).and(acquired(3)))
                .orPoolableMatches(list -> list.size() >= 2)
                .allocatingMax(10)
                .toConfig();

        assertThat(config.initialSize()).as("initialSize").isEqualTo(0);
        assertThat(config.maxSize()).as("maxSize").isEqualTo(10);
        assertThat(config.deliveryScheduler()).as("scheduler")
                .isSameAs(Schedulers.immediate());
        assertThat(config.evictionPredicate()).as("evictionPredicate")
                //accept based on timeSinceAllocation and acquire
                .accepts(new TestPooledRef<>(Collections.emptyList()))
                .accepts(new TestPooledRef<>(Collections.singletonList("A")))
                //accept based on size
                .accepts(new TestPooledRef<>(Arrays.asList("A", "B"), 100, 1))
                .accepts(new TestPooledRef<>(Arrays.asList("A", "B", "C"), 100, 2))
                //reject based on both
                .rejects(new TestPooledRef<>(Collections.emptyList(), 100, 2));
    }

    @Test
    @DisplayName("PooledRefPredicate PooledRefSecondPredicate AllocatingBetween")
    void builderCombinationSecondPredicate7() {
        final PoolConfig<List<String>> config = PoolBuilder.allocatingWith(Mono.<List<String>>fromCallable(ArrayList::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessRefMatches(agedMoreThan(Duration.ofSeconds(3)).and(acquired(3)))
                .orRefMatches(agedMoreThan(Duration.ofSeconds(10)))
                .allocatingBetween(1, 10)
                .toConfig();

        assertThat(config.initialSize()).as("initialSize").isEqualTo(1);
        assertThat(config.maxSize()).as("maxSize").isEqualTo(10);
        assertThat(config.deliveryScheduler()).as("scheduler")
                .isSameAs(Schedulers.immediate());
        assertThat(config.evictionPredicate()).as("evictionPredicate")
                //accept based on timeSinceAllocation and acquire
                .accepts(new TestPooledRef<>(Collections.emptyList(), 3000, 3))
                .accepts(new TestPooledRef<>(Collections.emptyList(), 10000, 2))
                .rejects(new TestPooledRef<>(Collections.emptyList(), 3000, 2))
                .rejects(new TestPooledRef<>(Collections.emptyList(), 2000, 3));
    }

    @Test
    @DisplayName("PooledRefPredicate PooledRefSecondPredicate AllocatingMax")
    void builderCombinationSecondPredicate8() {
        final PoolConfig<List<String>> config = PoolBuilder.allocatingWith(Mono.<List<String>>fromCallable(ArrayList::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessRefMatches(agedMoreThan(Duration.ofSeconds(3)).and(acquired(3)))
                .orRefMatches(agedMoreThan(Duration.ofSeconds(10)))
                .allocatingMax(10)
                .toConfig();

        assertThat(config.initialSize()).as("initialSize").isEqualTo(0);
        assertThat(config.maxSize()).as("maxSize").isEqualTo(10);
        assertThat(config.deliveryScheduler()).as("scheduler")
                .isSameAs(Schedulers.immediate());
        assertThat(config.evictionPredicate()).as("evictionPredicate")
                //accept based on timeSinceAllocation and acquire
                .accepts(new TestPooledRef<>(Collections.emptyList(), 3000, 3))
                .accepts(new TestPooledRef<>(Collections.emptyList(), 10000, 2))
                .rejects(new TestPooledRef<>(Collections.emptyList(), 3000, 2))
                .rejects(new TestPooledRef<>(Collections.emptyList(), 2000, 3));
    }

    //second predicate AND scheduler
    @Test
    @DisplayName("PoolablePredicate PoolableSecondPredicate Scheduler AllocatingBetween")
    void builderCombinationSecondPredicateScheduler1() {
        final PoolConfig<List<String>> config = PoolBuilder.allocatingWith(Mono.<List<String>>fromCallable(ArrayList::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessPoolableMatches(list -> list.size() == 2)
                .orPoolableMatches(list -> list.size() > 2)
                .publishOn(Schedulers.parallel())
                .allocatingBetween(1, 10)
                .toConfig();

        assertThat(config.initialSize()).as("initialSize").isEqualTo(1);
        assertThat(config.maxSize()).as("maxSize").isEqualTo(10);
        assertThat(config.deliveryScheduler()).as("scheduler")
                .isSameAs(Schedulers.parallel());
        assertThat(config.evictionPredicate()).as("evictionPredicate")
                .accepts(new TestPooledRef<>(Arrays.asList("A", "B")))
                .accepts(new TestPooledRef<>(Arrays.asList("A", "B", "C")))
                .rejects(new TestPooledRef<>(Collections.singletonList("A")));
    }

    @Test
    @DisplayName("PoolablePredicate PoolableSecondPredicate Scheduler AllocatingMax")
    void builderCombinationSecondPredicateScheduler2() {
        final PoolConfig<List<String>> config = PoolBuilder.allocatingWith(Mono.<List<String>>fromCallable(ArrayList::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessPoolableMatches(list -> list.size() == 2)
                .orPoolableMatches(list -> list.size() > 2)
                .publishOn(Schedulers.parallel())
                .allocatingMax(10)
                .toConfig();

        assertThat(config.initialSize()).as("initialSize").isEqualTo(0);
        assertThat(config.maxSize()).as("maxSize").isEqualTo(10);
        assertThat(config.deliveryScheduler()).as("scheduler")
                .isSameAs(Schedulers.parallel());
        assertThat(config.evictionPredicate()).as("evictionPredicate")
                .accepts(new TestPooledRef<>(Arrays.asList("A", "B")))
                .accepts(new TestPooledRef<>(Arrays.asList("A", "B", "C")))
                .rejects(new TestPooledRef<>(Collections.singletonList("A")));
    }

    @Test
    @DisplayName("PoolablePredicate PooledRefSecondPredicate Scheduler AllocatingBetween")
    void builderCombinationSecondPredicateScheduler3() {
        final PoolConfig<List<String>> config = PoolBuilder.allocatingWith(Mono.<List<String>>fromCallable(ArrayList::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessPoolableMatches(list -> list.size() >= 2)
                .orRefMatches(agedMoreThan(Duration.ofSeconds(3)).and(acquired(3)))
                .publishOn(Schedulers.parallel())
                .allocatingBetween(1, 10)
                .toConfig();

        assertThat(config.initialSize()).as("initialSize").isEqualTo(1);
        assertThat(config.maxSize()).as("maxSize").isEqualTo(10);
        assertThat(config.deliveryScheduler()).as("scheduler")
                .isSameAs(Schedulers.parallel());
        assertThat(config.evictionPredicate()).as("evictionPredicate")
                .accepts(new TestPooledRef<>(Arrays.asList("A", "B"), 100, 1))
                .accepts(new TestPooledRef<>(Arrays.asList("A", "B", "C"), 100, 1))
                .accepts(new TestPooledRef<>(Collections.emptyList(), 3000, 3))
                .rejects(new TestPooledRef<>(Collections.singletonList("A"), 3000, 2))
                .rejects(new TestPooledRef<>(Collections.singletonList("A"), 100, 3));
    }

    @Test
    @DisplayName("PoolablePredicate PooledRefSecondPredicate Scheduler AllocatingMax")
    void builderCombinationSecondPredicateScheduler4() {
        final PoolConfig<List<String>> config = PoolBuilder.allocatingWith(Mono.<List<String>>fromCallable(ArrayList::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessPoolableMatches(list -> list.size() >= 2)
                .orRefMatches(agedMoreThan(Duration.ofSeconds(3)).and(acquired(3)))
                .publishOn(Schedulers.parallel())
                .allocatingMax(10)
                .toConfig();

        assertThat(config.initialSize()).as("initialSize").isEqualTo(0);
        assertThat(config.maxSize()).as("maxSize").isEqualTo(10);
        assertThat(config.deliveryScheduler()).as("scheduler")
                .isSameAs(Schedulers.parallel());
        assertThat(config.evictionPredicate()).as("evictionPredicate")
                .accepts(new TestPooledRef<>(Arrays.asList("A", "B"), 100, 1))
                .accepts(new TestPooledRef<>(Arrays.asList("A", "B", "C"), 100, 1))
                .accepts(new TestPooledRef<>(Collections.emptyList(), 3000, 3))
                .rejects(new TestPooledRef<>(Collections.singletonList("A"), 3000, 2))
                .rejects(new TestPooledRef<>(Collections.singletonList("A"), 100, 3));
    }

    @Test
    @DisplayName("PooledRefPredicate PoolableSecondPredicate Scheduler AllocatingBetween")
    void builderCombinationSecondPredicateScheduler5() {
        final PoolConfig<List<String>> config = PoolBuilder.allocatingWith(Mono.<List<String>>fromCallable(ArrayList::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessRefMatches(agedMoreThan(Duration.ofSeconds(3)).and(acquired(3)))
                .orPoolableMatches(list -> list.size() >= 2)
                .publishOn(Schedulers.parallel())
                .allocatingBetween(1, 10)
                .toConfig();

        assertThat(config.initialSize()).as("initialSize").isEqualTo(1);
        assertThat(config.maxSize()).as("maxSize").isEqualTo(10);
        assertThat(config.deliveryScheduler()).as("scheduler")
                .isSameAs(Schedulers.parallel());
        assertThat(config.evictionPredicate()).as("evictionPredicate")
                .accepts(new TestPooledRef<>(Arrays.asList("A", "B"), 100, 1))
                .accepts(new TestPooledRef<>(Arrays.asList("A", "B", "C"), 100, 1))
                .accepts(new TestPooledRef<>(Collections.emptyList(), 3000, 3))
                .rejects(new TestPooledRef<>(Collections.singletonList("A"), 3000, 2))
                .rejects(new TestPooledRef<>(Collections.singletonList("A"), 100, 3));    }

    @Test
    @DisplayName("PooledRefPredicate PoolableSecondPredicate Scheduler AllocatingMax")
    void builderCombinationSecondPredicateScheduler6() {
        final PoolConfig<List<String>> config = PoolBuilder.allocatingWith(Mono.<List<String>>fromCallable(ArrayList::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessRefMatches(agedMoreThan(Duration.ofSeconds(3)).and(acquired(3)))
                .orPoolableMatches(list -> list.size() >= 2)
                .publishOn(Schedulers.parallel())
                .allocatingMax(10)
                .toConfig();

        assertThat(config.initialSize()).as("initialSize").isEqualTo(0);
        assertThat(config.maxSize()).as("maxSize").isEqualTo(10);
        assertThat(config.deliveryScheduler()).as("scheduler")
                .isSameAs(Schedulers.parallel());
        assertThat(config.evictionPredicate()).as("evictionPredicate")
                .accepts(new TestPooledRef<>(Arrays.asList("A", "B"), 100, 1))
                .accepts(new TestPooledRef<>(Arrays.asList("A", "B", "C"), 100, 1))
                .accepts(new TestPooledRef<>(Collections.emptyList(), 3000, 3))
                .rejects(new TestPooledRef<>(Collections.singletonList("A"), 3000, 2))
                .rejects(new TestPooledRef<>(Collections.singletonList("A"), 100, 3));    }

    @Test
    @DisplayName("PooledRefPredicate PooledRefSecondPredicate Scheduler AllocatingBetween")
    void builderCombinationSecondPredicateScheduler7() {
        final PoolConfig<List<String>> config = PoolBuilder.allocatingWith(Mono.<List<String>>fromCallable(ArrayList::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessRefMatches(agedMoreThan(Duration.ofSeconds(3)).and(acquired(3)))
                .orRefMatches(agedMoreThan(Duration.ofSeconds(10)))
                .publishOn(Schedulers.parallel())
                .allocatingBetween(1, 10)
                .toConfig();

        assertThat(config.initialSize()).as("initialSize").isEqualTo(1);
        assertThat(config.maxSize()).as("maxSize").isEqualTo(10);
        assertThat(config.deliveryScheduler()).as("scheduler")
                .isSameAs(Schedulers.parallel());
        assertThat(config.evictionPredicate()).as("evictionPredicate")
                .accepts(new TestPooledRef<>(Collections.emptyList(), 3000, 3))
                .accepts(new TestPooledRef<>(Collections.emptyList(), 10000, 2))
                .rejects(new TestPooledRef<>(Collections.emptyList(), 2000, 2));
    }

    @Test
    @DisplayName("PooledRefPredicate PooledRefSecondPredicate Scheduler AllocatingMax")
    void builderCombinationSecondPredicateScheduler8() {
        final PoolConfig<List<String>> config = PoolBuilder.allocatingWith(Mono.<List<String>>fromCallable(ArrayList::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessRefMatches(agedMoreThan(Duration.ofSeconds(3)).and(acquired(3)))
                .orRefMatches(agedMoreThan(Duration.ofSeconds(10)))
                .publishOn(Schedulers.parallel())
                .allocatingMax(10)
                .toConfig();

        assertThat(config.initialSize()).as("initialSize").isEqualTo(0);
        assertThat(config.maxSize()).as("maxSize").isEqualTo(10);
        assertThat(config.deliveryScheduler()).as("scheduler")
                .isSameAs(Schedulers.parallel());
        assertThat(config.evictionPredicate()).as("evictionPredicate")
                .accepts(new TestPooledRef<>(Collections.emptyList(), 3000, 3))
                .accepts(new TestPooledRef<>(Collections.emptyList(), 10000, 2))
                .rejects(new TestPooledRef<>(Collections.emptyList(), 2000, 2));
    }

    @Test
    void builderMultipleOrAndPredicates() {
        final PoolConfig<String> config = PoolBuilder.allocatingWith(Mono.just("foo"))
                .recycleWith(l -> Mono.empty())
                .unlessRefMatches(acquired(10))
                .orRefMatches(agedMoreThan(Duration.ofSeconds(3)).and(acquired(3)))
                .orPoolableMatches(str -> str.length() > 0 && str.length() < 2)
                .publishOn(Schedulers.parallel())
                .allocatingMax(10)
                .toConfig();

        //acquire 10+ || timeSinceAllocation 3 && acquire 3+ || length > 0 && length < 2
        TestPooledRef<String> ok1 = new TestPooledRef<>("HAHA", 100, 10);
        TestPooledRef<String> ok2 = new TestPooledRef<>("HAHA", 3000, 3);
        TestPooledRef<String> ok3 = new TestPooledRef<>("A", 100, 1);

        TestPooledRef<String> nok1 = new TestPooledRef<>("HAHA", 3000, 2);
        TestPooledRef<String> nok2 = new TestPooledRef<>("", 3000, 2);
        TestPooledRef<String> nok3 = new TestPooledRef<>("HAHA", 100, 3);
        TestPooledRef<String> nok4 = new TestPooledRef<>("", 100, 3);

        assertThat(config.evictionPredicate()).as("evictionPredicate")
                .accepts(ok1, ok2, ok3)
                .rejects(nok1, nok2, nok3, nok4);
    }
}