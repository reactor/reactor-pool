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
import reactor.util.pool.api.Pool;
import reactor.util.pool.builder.PoolBuilder;

import java.time.Duration;
import java.util.ArrayList;

import static reactor.util.pool.api.EvictionStrategies.agedMoreThan;
import static reactor.util.pool.api.EvictionStrategies.borrowed;

//FIXME test each pool configuration combination?
//intentionally one package lower than necessary to ensure correct visibility of APIs vs package-private implementations
class PoolBuilderApiCombinationTest {

    @Test
    @DisplayName("PoolablePredicate AllocatingBetween")
    void builderCombinationMinimal1() {
        final Pool<ArrayList<String>> pool = PoolBuilder.allocatingWith(Mono.fromCallable(ArrayList<String>::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessPoolableMatches(list -> list.size() > 3)
                .allocatingBetween(1, 10)
                .buildQueuePool();
    }

    @Test
    @DisplayName("PoolablePredicate AllocatingMax")
    void builderCombinationMinimal2() {
        final Pool<ArrayList<String>> pool = PoolBuilder.allocatingWith(Mono.fromCallable(ArrayList<String>::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessPoolableMatches(list -> list.size() > 3)
                .allocatingMax(10)
                .buildQueuePool();
    }

    @Test
    @DisplayName("SlotPredicate AllocatingBetween")
    void builderCombinationMinimal3() {
        final Pool<ArrayList<String>> pool = PoolBuilder.allocatingWith(Mono.fromCallable(ArrayList<String>::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessSlotMatches(borrowed(2))
                .allocatingBetween(1, 10)
                .buildQueuePool();
    }

    @Test
    @DisplayName("SlotPredicate AllocatingMax")
    void builderCombinationMinimal4() {
        final Pool<ArrayList<String>> pool = PoolBuilder.allocatingWith(Mono.fromCallable(ArrayList<String>::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessSlotMatches(borrowed(2))
                .allocatingMax(10)
                .buildQueuePool();
    }

    @Test
    @DisplayName("PoolablePredicate Scheduler AllocatingBetween")
    void builderCombinationWithScheduler1() {
        final Pool<ArrayList<String>> pool = PoolBuilder.allocatingWith(Mono.fromCallable(ArrayList<String>::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessPoolableMatches(list -> list.size() > 3)
                .publishOn(Schedulers.parallel())
                .allocatingBetween(1, 10)
                .buildQueuePool();
    }

    @Test
    @DisplayName("PoolablePredicate Scheduler AllocatingMax")
    void builderCombinationWithScheduler2() {
        final Pool<ArrayList<String>> pool = PoolBuilder.allocatingWith(Mono.fromCallable(ArrayList<String>::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessPoolableMatches(list -> list.size() > 3)
                .publishOn(Schedulers.parallel())
                .allocatingMax(10)
                .buildQueuePool();
    }

    @Test
    @DisplayName("SlotPredicate Scheduler AllocatingBetween")
    void builderCombinationWithScheduler3() {
        final Pool<ArrayList<String>> pool = PoolBuilder.allocatingWith(Mono.fromCallable(ArrayList<String>::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessSlotMatches(borrowed(2))
                .publishOn(Schedulers.parallel())
                .allocatingBetween(1, 10)
                .buildQueuePool();
    }

    @Test
    @DisplayName("SlotPredicate Scheduler AllocatingMax")
    void builderCombinationWithScheduler4() {
        final Pool<ArrayList<String>> pool = PoolBuilder.allocatingWith(Mono.fromCallable(ArrayList<String>::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessSlotMatches(borrowed(2))
                .publishOn(Schedulers.parallel())
                .allocatingMax(10)
                .buildQueuePool();
    }

    //second predicate, but no scheduler
    @Test
    @DisplayName("PoolablePredicate PoolableSecondPredicate AllocatingBetween")
    void builderCombinationSecondPredicate1() {
        final Pool<ArrayList<String>> pool = PoolBuilder.allocatingWith(Mono.fromCallable(ArrayList<String>::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessPoolableMatches(list -> list.size() == 3)
                .orPoolableMatches(list -> list.size() > 3)
                .allocatingBetween(1, 10)
                .buildQueuePool();
    }

    @Test
    @DisplayName("PoolablePredicate PoolableSecondPredicate AllocatingMax")
    void builderCombinationSecondPredicate2() {
        final Pool<ArrayList<String>> pool = PoolBuilder.allocatingWith(Mono.fromCallable(ArrayList<String>::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessPoolableMatches(list -> list.size() == 3)
                .orPoolableMatches(list -> list.size() > 3)
                .allocatingMax(10)
                .buildQueuePool();
    }

    @Test
    @DisplayName("PoolablePredicate SlotSecondPredicate AllocatingBetween")
    void builderCombinationSecondPredicate3() {
        final Pool<ArrayList<String>> pool = PoolBuilder.allocatingWith(Mono.fromCallable(ArrayList<String>::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessPoolableMatches(list -> list.size() > 3)
                .orSlotMatches(agedMoreThan(Duration.ofSeconds(3)).and(borrowed(3)))
                .allocatingBetween(1, 10)
                .buildQueuePool();
    }

    @Test
    @DisplayName("PoolablePredicate SlotSecondPredicate AllocatingMax")
    void builderCombinationSecondPredicate4() {
        final Pool<ArrayList<String>> pool = PoolBuilder.allocatingWith(Mono.fromCallable(ArrayList<String>::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessPoolableMatches(list -> list.size() > 3)
                .orSlotMatches(agedMoreThan(Duration.ofSeconds(3)).and(borrowed(3)))
                .allocatingMax(10)
                .buildQueuePool();
    }

    @Test
    @DisplayName("SlotPredicate PoolableSecondPredicate AllocatingBetween")
    void builderCombinationSecondPredicate5() {
        final Pool<ArrayList<String>> pool = PoolBuilder.allocatingWith(Mono.fromCallable(ArrayList<String>::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessSlotMatches(agedMoreThan(Duration.ofSeconds(3)).and(borrowed(3)))
                .orPoolableMatches(list -> list.size() >= 3)
                .allocatingBetween(1, 10)
                .buildQueuePool();
    }

    @Test
    @DisplayName("SlotPredicate PoolableSecondPredicate AllocatingMax")
    void builderCombinationSecondPredicate6() {
        final Pool<ArrayList<String>> pool = PoolBuilder.allocatingWith(Mono.fromCallable(ArrayList<String>::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessSlotMatches(agedMoreThan(Duration.ofSeconds(3)).and(borrowed(3)))
                .orPoolableMatches(list -> list.size() >= 3)
                .allocatingMax(10)
                .buildQueuePool();
    }

    @Test
    @DisplayName("SlotPredicate SlotSecondPredicate AllocatingBetween")
    void builderCombinationSecondPredicate7() {
        final Pool<ArrayList<String>> pool = PoolBuilder.allocatingWith(Mono.fromCallable(ArrayList<String>::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessSlotMatches(agedMoreThan(Duration.ofSeconds(3)).and(borrowed(3)))
                .orSlotMatches(agedMoreThan(Duration.ofSeconds(10)))
                .allocatingBetween(1, 10)
                .buildQueuePool();
    }

    @Test
    @DisplayName("SlotPredicate SlotSecondPredicate AllocatingMax")
    void builderCombinationSecondPredicate8() {
        final Pool<ArrayList<String>> pool = PoolBuilder.allocatingWith(Mono.fromCallable(ArrayList<String>::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessSlotMatches(agedMoreThan(Duration.ofSeconds(3)).and(borrowed(3)))
                .orSlotMatches(agedMoreThan(Duration.ofSeconds(10)))
                .allocatingMax(10)
                .buildQueuePool();
    }

    //second predicate AND scheduler
    @Test
    @DisplayName("PoolablePredicate PoolableSecondPredicate Scheduler AllocatingBetween")
    void builderCombinationSecondPredicateScheduler1() {
        final Pool<ArrayList<String>> pool = PoolBuilder.allocatingWith(Mono.fromCallable(ArrayList<String>::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessPoolableMatches(list -> list.size() == 3)
                .orPoolableMatches(list -> list.size() > 3)
                .publishOn(Schedulers.parallel())
                .allocatingBetween(1, 10)
                .buildQueuePool();
    }

    @Test
    @DisplayName("PoolablePredicate PoolableSecondPredicate Scheduler AllocatingMax")
    void builderCombinationSecondPredicateScheduler2() {
        final Pool<ArrayList<String>> pool = PoolBuilder.allocatingWith(Mono.fromCallable(ArrayList<String>::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessPoolableMatches(list -> list.size() == 3)
                .orPoolableMatches(list -> list.size() > 3)
                .publishOn(Schedulers.parallel())
                .allocatingMax(10)
                .buildQueuePool();
    }

    @Test
    @DisplayName("PoolablePredicate SlotSecondPredicate Scheduler AllocatingBetween")
    void builderCombinationSecondPredicateScheduler3() {
        final Pool<ArrayList<String>> pool = PoolBuilder.allocatingWith(Mono.fromCallable(ArrayList<String>::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessPoolableMatches(list -> list.size() > 3)
                .orSlotMatches(agedMoreThan(Duration.ofSeconds(3)).and(borrowed(3)))
                .publishOn(Schedulers.parallel())
                .allocatingBetween(1, 10)
                .buildQueuePool();
    }

    @Test
    @DisplayName("PoolablePredicate SlotSecondPredicate Scheduler AllocatingMax")
    void builderCombinationSecondPredicateScheduler4() {
        final Pool<ArrayList<String>> pool = PoolBuilder.allocatingWith(Mono.fromCallable(ArrayList<String>::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessPoolableMatches(list -> list.size() > 3)
                .orSlotMatches(agedMoreThan(Duration.ofSeconds(3)).and(borrowed(3)))
                .publishOn(Schedulers.parallel())
                .allocatingMax(10)
                .buildQueuePool();
    }

    @Test
    @DisplayName("SlotPredicate PoolableSecondPredicate Scheduler AllocatingBetween")
    void builderCombinationSecondPredicateScheduler5() {
        final Pool<ArrayList<String>> pool = PoolBuilder.allocatingWith(Mono.fromCallable(ArrayList<String>::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessSlotMatches(agedMoreThan(Duration.ofSeconds(3)).and(borrowed(3)))
                .orPoolableMatches(list -> list.size() > 3)
                .publishOn(Schedulers.parallel())
                .allocatingBetween(1, 10)
                .buildQueuePool();
    }

    @Test
    @DisplayName("SlotPredicate PoolableSecondPredicate Scheduler AllocatingMax")
    void builderCombinationSecondPredicateScheduler6() {
        final Pool<ArrayList<String>> pool = PoolBuilder.allocatingWith(Mono.fromCallable(ArrayList<String>::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessSlotMatches(agedMoreThan(Duration.ofSeconds(3)).and(borrowed(3)))
                .orPoolableMatches(list -> list.size() > 3)
                .publishOn(Schedulers.parallel())
                .allocatingMax(10)
                .buildQueuePool();
    }

    @Test
    @DisplayName("SlotPredicate SlotSecondPredicate Scheduler AllocatingBetween")
    void builderCombinationSecondPredicateScheduler7() {
        final Pool<ArrayList<String>> pool = PoolBuilder.allocatingWith(Mono.fromCallable(ArrayList<String>::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessSlotMatches(agedMoreThan(Duration.ofSeconds(3)).and(borrowed(3)))
                .orSlotMatches(agedMoreThan(Duration.ofSeconds(10)))
                .publishOn(Schedulers.parallel())
                .allocatingBetween(1, 10)
                .buildQueuePool();
    }

    @Test
    @DisplayName("SlotPredicate SlotSecondPredicate Scheduler AllocatingMax")
    void builderCombinationSecondPredicateScheduler8() {
        final Pool<ArrayList<String>> pool = PoolBuilder.allocatingWith(Mono.fromCallable(ArrayList<String>::new))
                .recycleWith(l -> Mono.fromRunnable(l::clear))
                .unlessSlotMatches(agedMoreThan(Duration.ofSeconds(3)).and(borrowed(3)))
                .orSlotMatches(agedMoreThan(Duration.ofSeconds(10)))
                .publishOn(Schedulers.parallel())
                .allocatingMax(10)
                .buildQueuePool();
    }
}