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

package reactor.util.pool.api;

import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Simon Baslé
 */
class AllocationStrategiesTest {

    private static final Logger LOG = Loggers.getLogger(AllocationStrategies.class);

    @DisplayName("allocatingMax")
    @Nested
    class AllocatingMaxTest {

        @Test
        void negativeMaxGivesOnePermit() {
            AllocationStrategy test = AllocationStrategies.allocatingMax(-1);

            assertThat(test.estimatePermitCount()).isOne();
        }

        @Test
        void zeroMaxGivesOnePermit() {
            AllocationStrategy test = AllocationStrategies.allocatingMax(0);

            assertThat(test.estimatePermitCount()).isOne();
        }

        @Test
        void onePermitCount() {
            AllocationStrategy test = AllocationStrategies.allocatingMax(1);

            assertThat(test.estimatePermitCount()).isOne();
        }

        @Test
        void onePermitGet() {
            AllocationStrategy test = AllocationStrategies.allocatingMax(1);

            assertThat(test.getPermit()).as("first try").isTrue();
            assertThat(test.getPermit()).as("second try").isFalse();
        }

        @Test
        void onePermitGetDesired() {
            AllocationStrategy test = AllocationStrategies.allocatingMax(1);

            assertThat(test.getPermits(100)).as("desired 100").isOne();
            assertThat(test.getPermits(1)).as("desired 1 more").isZero();
        }

        @Test
        void getPermitDesiredZero() {
            AllocationStrategy test = AllocationStrategies.allocatingMax(1);

            assertThat(test.getPermits(0)).isZero();
        }

        @Test
        void getPermitDesiredNegative() {
            AllocationStrategy test = AllocationStrategies.allocatingMax(1);

            assertThat(test.getPermits(-1)).isZero();
        }

        @Test
        void addPermitCanGoOverMax() {
            final AllocationStrategy test = AllocationStrategies.allocatingMax(1);

            test.addPermit();

            assertThat(test.estimatePermitCount()).isEqualTo(2);
        }

        @Test
        void addPermitsCanGoOverMax() {
            final AllocationStrategy test = AllocationStrategies.allocatingMax(1);

            test.addPermits(100);

            assertThat(test.estimatePermitCount()).isEqualTo(101);
        }

        @ParameterizedTest(name = "{0} workers")
        @ValueSource(ints = {5, 10, 20})
        @Tag("race")
        void raceGetPermit(int workerCount) throws InterruptedException {
            final AllocationStrategy test = AllocationStrategies.allocatingMax(10);

            LongAdder counter = new LongAdder();
            ExecutorService es = Executors.newFixedThreadPool(workerCount);

            for (int i = 0; i < 1_000_000; i++) {
                es.submit(() -> {
                    if (test.getPermit()) {
                        counter.increment();
                        test.addPermit();
                    }
                });
            }

            es.shutdown();
            es.awaitTermination(6, TimeUnit.SECONDS); //wait for the tasks to finish

            assertThat(counter.sum()).as("permits acquired").isEqualTo(1_000_000L);
            assertThat(test.estimatePermitCount()).as("end permit count").isEqualTo(10);
        }

        @ParameterizedTest(name = "{0} workers")
        @ValueSource(ints = {5, 10, 20})
        @Tag("race")
        void racePermitsRandom(int workerCount, TestInfo testInfo) throws InterruptedException {
            final AllocationStrategy test = AllocationStrategies.allocatingMax(10);

            LongAdder counter = new LongAdder();
            LongAdder gotZeroCounter = new LongAdder();
            ExecutorService es = Executors.newFixedThreadPool(workerCount);

            for (int i = 0; i < 1_000_000; i++) {
                es.submit(() -> {
                    ThreadLocalRandom tlr = ThreadLocalRandom.current();
                    int desired = tlr.nextInt(4, 7);
                    int got = test.getPermits(desired);
                    if (got == 0) gotZeroCounter.increment();

                    counter.add(got);
                    test.addPermits(got);
                });
            }

            es.shutdown();
            es.awaitTermination(6, TimeUnit.SECONDS); //wait for the tasks to finish

            LOG.info("{} - got 0 permit: {}%", testInfo.getTestMethod().map(m -> m.getName() + ", " + testInfo.getDisplayName()).orElse("?"),
                    gotZeroCounter.sum() * 100d / 1_000_000d);
            assertThat(counter.sum()).as("permits acquired").isBetween(1_000_000L, 10_000_000L);
            assertThat(test.estimatePermitCount()).as("end permit count").isEqualTo(10);
        }

        @ParameterizedTest(name = "{0} workers")
        @ValueSource(ints = {5, 10, 20})
        @Tag("race")
        void raceMixGetPermitWithGetRandomPermits(int workerCount, TestInfo testInfo) throws InterruptedException {
            final AllocationStrategy test = AllocationStrategies.allocatingMax(10);

            LongAdder usedGetRandomPermits = new LongAdder();
            LongAdder usedGetPermit = new LongAdder();
            LongAdder gotZeroCounter = new LongAdder();
            ExecutorService es = Executors.newFixedThreadPool(workerCount);

            for (int i = 0; i < 1_000_000; i++) {
                es.submit(() -> {
                    ThreadLocalRandom tlr = ThreadLocalRandom.current();
                    if (tlr.nextBoolean()) {
                        usedGetRandomPermits.increment();
                        //use permits
                        int desired = tlr.nextInt(0, 12);
                        int got = test.getPermits(desired);
                        if (got == 0) gotZeroCounter.increment();

                        test.addPermits(got);
                    }
                    else {
                        usedGetPermit.increment();
                        if (test.getPermit()) {
                            test.addPermit();
                        }
                        else {
                            gotZeroCounter.increment();
                        }
                    }
                });
            }

            es.shutdown();
            es.awaitTermination(6, TimeUnit.SECONDS); //wait for the tasks to finish

            LOG.info("{} - getPermit: {}%, getPermits(random): {}%, got 0 permit: {}%, ",
                    testInfo.getTestMethod().map(m -> m.getName() + ", " + testInfo.getDisplayName()).orElse("?"),
                    usedGetPermit.sum() * 100d / 1_000_000d,
                    usedGetRandomPermits.sum() * 100d / 1_000_000d,
                    gotZeroCounter.sum() * 100d / 1_000_000d);
            assertThat(test.estimatePermitCount()).as("end permit count").isEqualTo(10);
        }

        @ParameterizedTest(name = "{0} workers")
        @ValueSource(ints = {5, 10, 20})
        @Tag("race")
        void racePermitsRandomWithInnerLoop(int workerCount, TestInfo testInfo) throws InterruptedException {
            final AllocationStrategy test = AllocationStrategies.allocatingMax(10);

            LongAdder counter = new LongAdder();
            LongAdder gotZeroCounter = new LongAdder();
            ExecutorService es = Executors.newFixedThreadPool(workerCount);

            for (int i = 0; i < 1_000_000; i++) {
                es.submit(() -> {
                    ThreadLocalRandom tlr = ThreadLocalRandom.current();
                    int desired = tlr.nextInt(4, 7);
                    int got = test.getPermits(desired);
                    if (got == 0) gotZeroCounter.increment();
                    for (int j = 0; j < got; j++) {
                        counter.increment();
                        test.addPermit();
                    }
                });
            }

            es.shutdown();
            es.awaitTermination(6, TimeUnit.SECONDS); //wait for the tasks to finish

            LOG.info("{} - got 0 permit: {}%", testInfo.getTestMethod().map(m -> m.getName() + ", " + testInfo.getDisplayName()).orElse("?"),
                    gotZeroCounter.sum() * 100d / 1_000_000d);
            assertThat(counter.sum()).as("permits acquired").isBetween(1_000_000L, 10_000_000L);
            assertThat(test.estimatePermitCount()).as("end permit count").isEqualTo(10);
        }
    }

    @DisplayName("unbounded")
    @Nested
    class UnboundedTest {

        @Test
        void permitCountIsMaxValue() {
            AllocationStrategy test = AllocationStrategies.unbounded();

            assertThat(test.estimatePermitCount()).isEqualTo(Integer.MAX_VALUE);
        }

        @Test
        void getPermitDoesntChangeCount() {
            AllocationStrategy test = AllocationStrategies.unbounded();

            assertThat(test.getPermit()).as("first try").isTrue();
            assertThat(test.getPermit()).as("second try").isTrue();
            assertThat(test.estimatePermitCount()).as("permit count unbounded").isEqualTo(Integer.MAX_VALUE);
        }

        @Test
        void getPermitsDoesntChangeCount() {
            AllocationStrategy test = AllocationStrategies.unbounded();

            assertThat(test.getPermits(100)).as("first try").isEqualTo(100);
            assertThat(test.getPermits(1000)).as("second try").isEqualTo(1000);
            assertThat(test.estimatePermitCount()).as("permit count unbounded").isEqualTo(Integer.MAX_VALUE);
        }

        @Test
        void getPermitDesiredZero() {
            AllocationStrategy test = AllocationStrategies.unbounded();

            assertThat(test.getPermits(0)).isZero();
        }

        @Test
        void getPermitDesiredNegative() {
            AllocationStrategy test = AllocationStrategies.unbounded();

            assertThat(test.getPermits(-1)).isZero();
        }

        @Test
        void addPermitDoesntChangeMax() {
            final AllocationStrategy test = AllocationStrategies.unbounded();

            test.addPermit();

            assertThat(test.estimatePermitCount()).isEqualTo(Integer.MAX_VALUE);
        }

        @Test
        void addPermitsDoesntChangeMax() {
            final AllocationStrategy test = AllocationStrategies.unbounded();

            test.addPermits(1000);

            assertThat(test.estimatePermitCount()).isEqualTo(Integer.MAX_VALUE);
        }
    }

}