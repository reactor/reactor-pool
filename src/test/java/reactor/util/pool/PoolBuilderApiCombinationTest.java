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
import reactor.util.pool.api.PoolSlot;
import reactor.util.pool.builder.PoolBuilder;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static reactor.util.pool.api.EvictionStrategies.agedMoreThan;
import static reactor.util.pool.api.EvictionStrategies.borrowed;

//FIXME test each pool configuration combination?
//intentionally one package lower than necessary to ensure correct visibility of APIs vs package-private implementations
class PoolBuilderApiCombinationTest {

    /**
     * Fake {@link PoolSlot} for tests, either wrapping value+age+borrowCount or just a
     * value (in which case age defaults to 12s and borrowCount to 1000)
     * @param <T>
     */
    final class TestSlot<T> implements PoolSlot<T> {

        final T poolable;
        final long age;
        final int borrowCount;

        TestSlot(T poolable, long age, int borrowCount) {
            this.poolable = poolable;
            this.age = age;
            this.borrowCount = borrowCount;
        }

        TestSlot(T poolable) {
            this(poolable, 12_000, 1000);
        }

        @Override
        public T poolable() {
            return poolable;
        }

        @Override
        public Mono<Void> releaseMono() {
            return Mono.empty();
        }

        @Override
        public void release() { }

        @Override
        public void invalidate() { }

        @Override
        public int borrowCount() {
            return borrowCount;
        }

        @Override
        public long age() {
            return age;
        }

        @Override
        public String toString() {
            return "TestSlot{" +
                    "poolable=" + poolable +
                    ", age=" + age +
                    ", borrowCount=" + borrowCount +
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

        assertThat(config.minSize()).as("minSize").isEqualTo(1);
        assertThat(config.maxSize()).as("maxSize").isEqualTo(10);
        assertThat(config.deliveryScheduler()).as("scheduler").isSameAs(Schedulers.immediate());
        assertThat(config.evictionPredicate()).as("evictionPredicate")
                .rejects(new TestSlot<>(Collections.emptyList()))
                .rejects(new TestSlot<>(Collections.singletonList("A")))
                .accepts(new TestSlot<>(Arrays.asList("A", "B")))
                .accepts(new TestSlot<>(Arrays.asList("A", "B", "C")));
    }

    @Test
    @DisplayName("PoolablePredicate AllocatingMax")
    void builderCombinationMinimal2() {
        final PoolConfig<List<String>> config = PoolBuilder.allocatingWith(Mono.<List<String>>fromCallable(ArrayList::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessPoolableMatches(list -> list.size() >= 2)
                .allocatingMax(10)
                .toConfig();

        assertThat(config.minSize()).as("minSize").isEqualTo(0);
        assertThat(config.maxSize()).as("maxSize").isEqualTo(10);
        assertThat(config.deliveryScheduler()).as("scheduler").isSameAs(Schedulers.immediate());
        assertThat(config.evictionPredicate()).as("evictionPredicate")
                .rejects(new TestSlot<>(Collections.emptyList()))
                .rejects(new TestSlot<>(Collections.singletonList("A")))
                .accepts(new TestSlot<>(Arrays.asList("A", "B")))
                .accepts(new TestSlot<>(Arrays.asList("A", "B", "C")));
    }

    @Test
    @DisplayName("SlotPredicate AllocatingBetween")
    void builderCombinationMinimal3() {
        final PoolConfig<List<String>> config = PoolBuilder.allocatingWith(Mono.<List<String>>fromCallable(ArrayList::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessSlotMatches(borrowed(2))
                .allocatingBetween(1, 10)
                .toConfig();

        assertThat(config.minSize()).as("minSize").isEqualTo(1);
        assertThat(config.maxSize()).as("maxSize").isEqualTo(10);
        assertThat(config.deliveryScheduler()).as("scheduler").isSameAs(Schedulers.immediate());
        assertThat(config.evictionPredicate()).as("evictionPredicate")
                .accepts(new TestSlot<>(Collections.emptyList()))
                .accepts(new TestSlot<>(Arrays.asList("A", "B", "C"), 10_000, 2))
                .accepts(new TestSlot<>(Arrays.asList("A", "B", "C"), 10_000, 3))
                .rejects(new TestSlot<>(Collections.emptyList(), 10_000, 1));
    }

    @Test
    @DisplayName("SlotPredicate AllocatingMax")
    void builderCombinationMinimal4() {
        final PoolConfig<List<String>> config = PoolBuilder.allocatingWith(Mono.<List<String>>fromCallable(ArrayList::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessSlotMatches(borrowed(2))
                .allocatingMax(10)
                .toConfig();

        assertThat(config.minSize()).as("minSize").isEqualTo(0);
        assertThat(config.maxSize()).as("maxSize").isEqualTo(10);
        assertThat(config.deliveryScheduler()).as("scheduler").isSameAs(Schedulers.immediate());
        assertThat(config.evictionPredicate()).as("evictionPredicate")
                .accepts(new TestSlot<>(Collections.emptyList()))
                .accepts(new TestSlot<>(Arrays.asList("A", "B", "C"), 10_000, 2))
                .accepts(new TestSlot<>(Arrays.asList("A", "B", "C"), 10_000, 3))
                .rejects(new TestSlot<>(Collections.emptyList(), 10_000, 1));
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

        assertThat(config.minSize()).as("minSize").isEqualTo(1);
        assertThat(config.maxSize()).as("maxSize").isEqualTo(10);
        assertThat(config.deliveryScheduler()).as("scheduler")
                .isSameAs(Schedulers.parallel());
        assertThat(config.evictionPredicate()).as("evictionPredicate")
                .rejects(new TestSlot<>(Collections.emptyList()))
                .rejects(new TestSlot<>(Collections.singletonList("A")))
                .accepts(new TestSlot<>(Arrays.asList("A", "B")))
                .accepts(new TestSlot<>(Arrays.asList("A", "B", "C")));
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

        assertThat(config.minSize()).as("minSize").isEqualTo(0);
        assertThat(config.maxSize()).as("maxSize").isEqualTo(10);
        assertThat(config.deliveryScheduler()).as("scheduler")
                .isSameAs(Schedulers.parallel());
        assertThat(config.evictionPredicate()).as("evictionPredicate")
                .rejects(new TestSlot<>(Collections.emptyList()))
                .rejects(new TestSlot<>(Collections.singletonList("A")))
                .accepts(new TestSlot<>(Arrays.asList("A", "B")))
                .accepts(new TestSlot<>(Arrays.asList("A", "B", "C")));
    }

    @Test
    @DisplayName("SlotPredicate Scheduler AllocatingBetween")
    void builderCombinationWithScheduler3() {
        final PoolConfig<List<String>> config = PoolBuilder.allocatingWith(Mono.<List<String>>fromCallable(ArrayList::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessSlotMatches(borrowed(2))
                .publishOn(Schedulers.parallel())
                .allocatingBetween(1, 10)
                .toConfig();

        assertThat(config.minSize()).as("minSize").isEqualTo(1);
        assertThat(config.maxSize()).as("maxSize").isEqualTo(10);
        assertThat(config.deliveryScheduler()).as("scheduler")
                .isSameAs(Schedulers.parallel());
        assertThat(config.evictionPredicate()).as("evictionPredicate")
                .accepts(new TestSlot<>(Collections.emptyList()))
                .accepts(new TestSlot<>(Arrays.asList("A", "B", "C"), 10_000, 2))
                .accepts(new TestSlot<>(Arrays.asList("A", "B", "C"), 10_000, 3))
                .rejects(new TestSlot<>(Collections.emptyList(), 10_000, 1));
    }

    @Test
    @DisplayName("SlotPredicate Scheduler AllocatingMax")
    void builderCombinationWithScheduler4() {
        final PoolConfig<List<String>> config = PoolBuilder.allocatingWith(Mono.<List<String>>fromCallable(ArrayList::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessSlotMatches(borrowed(2))
                .publishOn(Schedulers.parallel())
                .allocatingMax(10)
                .toConfig();

        assertThat(config.minSize()).as("minSize").isEqualTo(0);
        assertThat(config.maxSize()).as("maxSize").isEqualTo(10);
        assertThat(config.deliveryScheduler()).as("scheduler")
                .isSameAs(Schedulers.parallel());
        assertThat(config.evictionPredicate()).as("evictionPredicate")
                .accepts(new TestSlot<>(Collections.emptyList()))
                .accepts(new TestSlot<>(Arrays.asList("A", "B", "C"), 10_000, 2))
                .accepts(new TestSlot<>(Arrays.asList("A", "B", "C"), 10_000, 3))
                .rejects(new TestSlot<>(Collections.emptyList(), 10_000, 1));
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

        assertThat(config.minSize()).as("minSize").isEqualTo(1);
        assertThat(config.maxSize()).as("maxSize").isEqualTo(10);
        assertThat(config.deliveryScheduler()).as("scheduler")
                .isSameAs(Schedulers.immediate());
        assertThat(config.evictionPredicate()).as("evictionPredicate")
                .rejects(new TestSlot<>(Collections.emptyList()))
                .rejects(new TestSlot<>(Collections.singletonList("A")))
                .accepts(new TestSlot<>(Arrays.asList("A", "B")))
                .accepts(new TestSlot<>(Arrays.asList("A", "B", "C")));
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

        assertThat(config.minSize()).as("minSize").isEqualTo(0);
        assertThat(config.maxSize()).as("maxSize").isEqualTo(10);
        assertThat(config.deliveryScheduler()).as("scheduler")
                .isSameAs(Schedulers.immediate());
        assertThat(config.evictionPredicate()).as("evictionPredicate")
                .rejects(new TestSlot<>(Collections.emptyList()))
                .rejects(new TestSlot<>(Collections.singletonList("A")))
                .accepts(new TestSlot<>(Arrays.asList("A", "B")))
                .accepts(new TestSlot<>(Arrays.asList("A", "B", "C")));
    }

    @Test
    @DisplayName("PoolablePredicate SlotSecondPredicate AllocatingBetween")
    void builderCombinationSecondPredicate3() {
        final PoolConfig<List<String>> config = PoolBuilder.allocatingWith(Mono.<List<String>>fromCallable(ArrayList::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessPoolableMatches(list -> list.size() >= 2)
                .orSlotMatches(agedMoreThan(Duration.ofSeconds(3)).and(borrowed(3)))
                .allocatingBetween(1, 10)
                .toConfig();

        assertThat(config.minSize()).as("minSize").isEqualTo(1);
        assertThat(config.maxSize()).as("maxSize").isEqualTo(10);
        assertThat(config.deliveryScheduler()).as("scheduler")
                .isSameAs(Schedulers.immediate());
        assertThat(config.evictionPredicate()).as("evictionPredicate")
                //accept based on age and borrow
                .accepts(new TestSlot<>(Collections.emptyList()))
                .accepts(new TestSlot<>(Collections.singletonList("A")))
                //accept based on size
                .accepts(new TestSlot<>(Arrays.asList("A", "B"), 100, 1))
                .accepts(new TestSlot<>(Arrays.asList("A", "B", "C"), 100, 2))
                //reject based on both
                .rejects(new TestSlot<>(Collections.emptyList(), 100, 2));
    }

    @Test
    @DisplayName("PoolablePredicate SlotSecondPredicate AllocatingMax")
    void builderCombinationSecondPredicate4() {
        final PoolConfig<List<String>> config = PoolBuilder.allocatingWith(Mono.<List<String>>fromCallable(ArrayList::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessPoolableMatches(list -> list.size() >= 2)
                .orSlotMatches(agedMoreThan(Duration.ofSeconds(3)).and(borrowed(3)))
                .allocatingMax(10)
                .toConfig();

        assertThat(config.minSize()).as("minSize").isEqualTo(0);
        assertThat(config.maxSize()).as("maxSize").isEqualTo(10);
        assertThat(config.deliveryScheduler()).as("scheduler")
                .isSameAs(Schedulers.immediate());
        assertThat(config.evictionPredicate()).as("evictionPredicate")
                //accept based on age and borrow
                .accepts(new TestSlot<>(Collections.emptyList()))
                .accepts(new TestSlot<>(Collections.singletonList("A")))
                //accept based on size
                .accepts(new TestSlot<>(Arrays.asList("A", "B"), 100, 1))
                .accepts(new TestSlot<>(Arrays.asList("A", "B", "C"), 100, 2))
                //reject based on both
                .rejects(new TestSlot<>(Collections.emptyList(), 100, 2));
    }

    @Test
    @DisplayName("SlotPredicate PoolableSecondPredicate AllocatingBetween")
    void builderCombinationSecondPredicate5() {
        final PoolConfig<List<String>> config = PoolBuilder.allocatingWith(Mono.<List<String>>fromCallable(ArrayList::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessSlotMatches(agedMoreThan(Duration.ofSeconds(3)).and(borrowed(3)))
                .orPoolableMatches(list -> list.size() >= 2)
                .allocatingBetween(1, 10)
                .toConfig();

        assertThat(config.minSize()).as("minSize").isEqualTo(1);
        assertThat(config.maxSize()).as("maxSize").isEqualTo(10);
        assertThat(config.deliveryScheduler()).as("scheduler")
                .isSameAs(Schedulers.immediate());
        assertThat(config.evictionPredicate()).as("evictionPredicate")
                //accept based on age and borrow
                .accepts(new TestSlot<>(Collections.emptyList()))
                .accepts(new TestSlot<>(Collections.singletonList("A")))
                //accept based on size
                .accepts(new TestSlot<>(Arrays.asList("A", "B"), 100, 1))
                .accepts(new TestSlot<>(Arrays.asList("A", "B", "C"), 100, 2))
                //reject based on both
                .rejects(new TestSlot<>(Collections.emptyList(), 100, 2));
    }

    @Test
    @DisplayName("SlotPredicate PoolableSecondPredicate AllocatingMax")
    void builderCombinationSecondPredicate6() {
        final PoolConfig<List<String>> config = PoolBuilder.allocatingWith(Mono.<List<String>>fromCallable(ArrayList::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessSlotMatches(agedMoreThan(Duration.ofSeconds(3)).and(borrowed(3)))
                .orPoolableMatches(list -> list.size() >= 2)
                .allocatingMax(10)
                .toConfig();

        assertThat(config.minSize()).as("minSize").isEqualTo(0);
        assertThat(config.maxSize()).as("maxSize").isEqualTo(10);
        assertThat(config.deliveryScheduler()).as("scheduler")
                .isSameAs(Schedulers.immediate());
        assertThat(config.evictionPredicate()).as("evictionPredicate")
                //accept based on age and borrow
                .accepts(new TestSlot<>(Collections.emptyList()))
                .accepts(new TestSlot<>(Collections.singletonList("A")))
                //accept based on size
                .accepts(new TestSlot<>(Arrays.asList("A", "B"), 100, 1))
                .accepts(new TestSlot<>(Arrays.asList("A", "B", "C"), 100, 2))
                //reject based on both
                .rejects(new TestSlot<>(Collections.emptyList(), 100, 2));
    }

    @Test
    @DisplayName("SlotPredicate SlotSecondPredicate AllocatingBetween")
    void builderCombinationSecondPredicate7() {
        final PoolConfig<List<String>> config = PoolBuilder.allocatingWith(Mono.<List<String>>fromCallable(ArrayList::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessSlotMatches(agedMoreThan(Duration.ofSeconds(3)).and(borrowed(3)))
                .orSlotMatches(agedMoreThan(Duration.ofSeconds(10)))
                .allocatingBetween(1, 10)
                .toConfig();

        assertThat(config.minSize()).as("minSize").isEqualTo(1);
        assertThat(config.maxSize()).as("maxSize").isEqualTo(10);
        assertThat(config.deliveryScheduler()).as("scheduler")
                .isSameAs(Schedulers.immediate());
        assertThat(config.evictionPredicate()).as("evictionPredicate")
                //accept based on age and borrow
                .accepts(new TestSlot<>(Collections.emptyList(), 3000, 3))
                .accepts(new TestSlot<>(Collections.emptyList(), 10000, 2))
                .rejects(new TestSlot<>(Collections.emptyList(), 3000, 2))
                .rejects(new TestSlot<>(Collections.emptyList(), 2000, 3));
    }

    @Test
    @DisplayName("SlotPredicate SlotSecondPredicate AllocatingMax")
    void builderCombinationSecondPredicate8() {
        final PoolConfig<List<String>> config = PoolBuilder.allocatingWith(Mono.<List<String>>fromCallable(ArrayList::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessSlotMatches(agedMoreThan(Duration.ofSeconds(3)).and(borrowed(3)))
                .orSlotMatches(agedMoreThan(Duration.ofSeconds(10)))
                .allocatingMax(10)
                .toConfig();

        assertThat(config.minSize()).as("minSize").isEqualTo(0);
        assertThat(config.maxSize()).as("maxSize").isEqualTo(10);
        assertThat(config.deliveryScheduler()).as("scheduler")
                .isSameAs(Schedulers.immediate());
        assertThat(config.evictionPredicate()).as("evictionPredicate")
                //accept based on age and borrow
                .accepts(new TestSlot<>(Collections.emptyList(), 3000, 3))
                .accepts(new TestSlot<>(Collections.emptyList(), 10000, 2))
                .rejects(new TestSlot<>(Collections.emptyList(), 3000, 2))
                .rejects(new TestSlot<>(Collections.emptyList(), 2000, 3));
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

        assertThat(config.minSize()).as("minSize").isEqualTo(1);
        assertThat(config.maxSize()).as("maxSize").isEqualTo(10);
        assertThat(config.deliveryScheduler()).as("scheduler")
                .isSameAs(Schedulers.parallel());
        assertThat(config.evictionPredicate()).as("evictionPredicate")
                .accepts(new TestSlot<>(Arrays.asList("A", "B")))
                .accepts(new TestSlot<>(Arrays.asList("A", "B", "C")))
                .rejects(new TestSlot<>(Collections.singletonList("A")));
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

        assertThat(config.minSize()).as("minSize").isEqualTo(0);
        assertThat(config.maxSize()).as("maxSize").isEqualTo(10);
        assertThat(config.deliveryScheduler()).as("scheduler")
                .isSameAs(Schedulers.parallel());
        assertThat(config.evictionPredicate()).as("evictionPredicate")
                .accepts(new TestSlot<>(Arrays.asList("A", "B")))
                .accepts(new TestSlot<>(Arrays.asList("A", "B", "C")))
                .rejects(new TestSlot<>(Collections.singletonList("A")));
    }

    @Test
    @DisplayName("PoolablePredicate SlotSecondPredicate Scheduler AllocatingBetween")
    void builderCombinationSecondPredicateScheduler3() {
        final PoolConfig<List<String>> config = PoolBuilder.allocatingWith(Mono.<List<String>>fromCallable(ArrayList::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessPoolableMatches(list -> list.size() >= 2)
                .orSlotMatches(agedMoreThan(Duration.ofSeconds(3)).and(borrowed(3)))
                .publishOn(Schedulers.parallel())
                .allocatingBetween(1, 10)
                .toConfig();

        assertThat(config.minSize()).as("minSize").isEqualTo(1);
        assertThat(config.maxSize()).as("maxSize").isEqualTo(10);
        assertThat(config.deliveryScheduler()).as("scheduler")
                .isSameAs(Schedulers.parallel());
        assertThat(config.evictionPredicate()).as("evictionPredicate")
                .accepts(new TestSlot<>(Arrays.asList("A", "B"), 100, 1))
                .accepts(new TestSlot<>(Arrays.asList("A", "B", "C"), 100, 1))
                .accepts(new TestSlot<>(Collections.emptyList(), 3000, 3))
                .rejects(new TestSlot<>(Collections.singletonList("A"), 3000, 2))
                .rejects(new TestSlot<>(Collections.singletonList("A"), 100, 3));
    }

    @Test
    @DisplayName("PoolablePredicate SlotSecondPredicate Scheduler AllocatingMax")
    void builderCombinationSecondPredicateScheduler4() {
        final PoolConfig<List<String>> config = PoolBuilder.allocatingWith(Mono.<List<String>>fromCallable(ArrayList::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessPoolableMatches(list -> list.size() >= 2)
                .orSlotMatches(agedMoreThan(Duration.ofSeconds(3)).and(borrowed(3)))
                .publishOn(Schedulers.parallel())
                .allocatingMax(10)
                .toConfig();

        assertThat(config.minSize()).as("minSize").isEqualTo(0);
        assertThat(config.maxSize()).as("maxSize").isEqualTo(10);
        assertThat(config.deliveryScheduler()).as("scheduler")
                .isSameAs(Schedulers.parallel());
        assertThat(config.evictionPredicate()).as("evictionPredicate")
                .accepts(new TestSlot<>(Arrays.asList("A", "B"), 100, 1))
                .accepts(new TestSlot<>(Arrays.asList("A", "B", "C"), 100, 1))
                .accepts(new TestSlot<>(Collections.emptyList(), 3000, 3))
                .rejects(new TestSlot<>(Collections.singletonList("A"), 3000, 2))
                .rejects(new TestSlot<>(Collections.singletonList("A"), 100, 3));
    }

    @Test
    @DisplayName("SlotPredicate PoolableSecondPredicate Scheduler AllocatingBetween")
    void builderCombinationSecondPredicateScheduler5() {
        final PoolConfig<List<String>> config = PoolBuilder.allocatingWith(Mono.<List<String>>fromCallable(ArrayList::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessSlotMatches(agedMoreThan(Duration.ofSeconds(3)).and(borrowed(3)))
                .orPoolableMatches(list -> list.size() >= 2)
                .publishOn(Schedulers.parallel())
                .allocatingBetween(1, 10)
                .toConfig();

        assertThat(config.minSize()).as("minSize").isEqualTo(1);
        assertThat(config.maxSize()).as("maxSize").isEqualTo(10);
        assertThat(config.deliveryScheduler()).as("scheduler")
                .isSameAs(Schedulers.parallel());
        assertThat(config.evictionPredicate()).as("evictionPredicate")
                .accepts(new TestSlot<>(Arrays.asList("A", "B"), 100, 1))
                .accepts(new TestSlot<>(Arrays.asList("A", "B", "C"), 100, 1))
                .accepts(new TestSlot<>(Collections.emptyList(), 3000, 3))
                .rejects(new TestSlot<>(Collections.singletonList("A"), 3000, 2))
                .rejects(new TestSlot<>(Collections.singletonList("A"), 100, 3));    }

    @Test
    @DisplayName("SlotPredicate PoolableSecondPredicate Scheduler AllocatingMax")
    void builderCombinationSecondPredicateScheduler6() {
        final PoolConfig<List<String>> config = PoolBuilder.allocatingWith(Mono.<List<String>>fromCallable(ArrayList::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessSlotMatches(agedMoreThan(Duration.ofSeconds(3)).and(borrowed(3)))
                .orPoolableMatches(list -> list.size() >= 2)
                .publishOn(Schedulers.parallel())
                .allocatingMax(10)
                .toConfig();

        assertThat(config.minSize()).as("minSize").isEqualTo(0);
        assertThat(config.maxSize()).as("maxSize").isEqualTo(10);
        assertThat(config.deliveryScheduler()).as("scheduler")
                .isSameAs(Schedulers.parallel());
        assertThat(config.evictionPredicate()).as("evictionPredicate")
                .accepts(new TestSlot<>(Arrays.asList("A", "B"), 100, 1))
                .accepts(new TestSlot<>(Arrays.asList("A", "B", "C"), 100, 1))
                .accepts(new TestSlot<>(Collections.emptyList(), 3000, 3))
                .rejects(new TestSlot<>(Collections.singletonList("A"), 3000, 2))
                .rejects(new TestSlot<>(Collections.singletonList("A"), 100, 3));    }

    @Test
    @DisplayName("SlotPredicate SlotSecondPredicate Scheduler AllocatingBetween")
    void builderCombinationSecondPredicateScheduler7() {
        final PoolConfig<List<String>> config = PoolBuilder.allocatingWith(Mono.<List<String>>fromCallable(ArrayList::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessSlotMatches(agedMoreThan(Duration.ofSeconds(3)).and(borrowed(3)))
                .orSlotMatches(agedMoreThan(Duration.ofSeconds(10)))
                .publishOn(Schedulers.parallel())
                .allocatingBetween(1, 10)
                .toConfig();

        assertThat(config.minSize()).as("minSize").isEqualTo(1);
        assertThat(config.maxSize()).as("maxSize").isEqualTo(10);
        assertThat(config.deliveryScheduler()).as("scheduler")
                .isSameAs(Schedulers.parallel());
        assertThat(config.evictionPredicate()).as("evictionPredicate")
                .accepts(new TestSlot<>(Collections.emptyList(), 3000, 3))
                .accepts(new TestSlot<>(Collections.emptyList(), 10000, 2))
                .rejects(new TestSlot<>(Collections.emptyList(), 2000, 2));
    }

    @Test
    @DisplayName("SlotPredicate SlotSecondPredicate Scheduler AllocatingMax")
    void builderCombinationSecondPredicateScheduler8() {
        final PoolConfig<List<String>> config = PoolBuilder.allocatingWith(Mono.<List<String>>fromCallable(ArrayList::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessSlotMatches(agedMoreThan(Duration.ofSeconds(3)).and(borrowed(3)))
                .orSlotMatches(agedMoreThan(Duration.ofSeconds(10)))
                .publishOn(Schedulers.parallel())
                .allocatingMax(10)
                .toConfig();

        assertThat(config.minSize()).as("minSize").isEqualTo(0);
        assertThat(config.maxSize()).as("maxSize").isEqualTo(10);
        assertThat(config.deliveryScheduler()).as("scheduler")
                .isSameAs(Schedulers.parallel());
        assertThat(config.evictionPredicate()).as("evictionPredicate")
                .accepts(new TestSlot<>(Collections.emptyList(), 3000, 3))
                .accepts(new TestSlot<>(Collections.emptyList(), 10000, 2))
                .rejects(new TestSlot<>(Collections.emptyList(), 2000, 2));
    }

    @Test
    void builderMultipleOrAndPredicates() {
        final PoolConfig<String> config = PoolBuilder.allocatingWith(Mono.just("foo"))
                .recycleWith(l -> Mono.empty())
                .unlessSlotMatches(borrowed(10))
                .orSlotMatches(agedMoreThan(Duration.ofSeconds(3)).and(borrowed(3)))
                .orPoolableMatches(str -> str.length() > 0 && str.length() < 2)
                .publishOn(Schedulers.parallel())
                .allocatingMax(10)
                .toConfig();

        //borrow 10+ || age 3 && borrow 3+ || length > 0 && length < 2
        TestSlot<String> ok1 = new TestSlot<>("HAHA", 100, 10);
        TestSlot<String> ok2 = new TestSlot<>("HAHA", 3000, 3);
        TestSlot<String> ok3 = new TestSlot<>("A", 100, 1);

        TestSlot<String> nok1 = new TestSlot<>("HAHA", 3000, 2);
        TestSlot<String> nok2 = new TestSlot<>("", 3000, 2);
        TestSlot<String> nok3 = new TestSlot<>("HAHA", 100, 3);
        TestSlot<String> nok4 = new TestSlot<>("", 100, 3);

        assertThat(config.evictionPredicate()).as("evictionPredicate")
                .accepts(ok1, ok2, ok3)
                .rejects(nok1, nok2, nok3, nok4);
    }
}