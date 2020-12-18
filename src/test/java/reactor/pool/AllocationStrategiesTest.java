/*
 * Copyright (c) 2018-Present VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.pool;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import reactor.pool.AllocationStrategies.SizeBasedAllocationStrategy;
import reactor.pool.AllocationStrategies.UnboundedAllocationStrategy;
import reactor.util.Logger;
import reactor.util.Loggers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * @author Simon Baslé
 */
class AllocationStrategiesTest {

    private static final Logger LOG = Loggers.getLogger(AllocationStrategies.class);

    @DisplayName("allocatingSize")
    @SuppressWarnings("ClassCanBeStatic")
    @Nested
    class AllocatingSizeTest {

        @Test
        void negativeMaxThrows() {
            assertThatIllegalArgumentException()
                    .isThrownBy(() -> new SizeBasedAllocationStrategy(0, -1))
                    .withMessage("max must be strictly positive");
        }

        @Test
        void zeroMaxThrows() {
            assertThatIllegalArgumentException()
                    .isThrownBy(() -> new SizeBasedAllocationStrategy(0, 0))
                    .withMessage("max must be strictly positive");
        }

        @Test
        void negativeMinThrows() {
            assertThatIllegalArgumentException()
                    .isThrownBy(() -> new SizeBasedAllocationStrategy(-1, 0))
                    .withMessage("min must be positive or zero");
        }

        @Test
        void minMoreThanMaxThrows() {
            assertThatIllegalArgumentException()
                    .isThrownBy(() -> new SizeBasedAllocationStrategy(2, 1))
                    .withMessage("min must be less than or equal to max");
        }

        @Test
        void minDoesntInfluenceInitialPermitCount() {
            AllocationStrategy test = new SizeBasedAllocationStrategy(2, 5);

            assertThat(test.estimatePermitCount()).isEqualTo(5);
        }

        @Test
        void atMostOnePermitCount() {
            AllocationStrategy test = new SizeBasedAllocationStrategy(0, 1);

            assertThat(test.estimatePermitCount()).isOne();
        }

        @Test
        void atMostOnePermitGet() {
            AllocationStrategy test = new SizeBasedAllocationStrategy(0, 1);

            assertThat(test.getPermits(1)).as("first try").isOne();
            assertThat(test.getPermits(1)).as("second try").isZero();
        }

        @Test
        void atMostOnePermitGetDesired() {
            AllocationStrategy test = new SizeBasedAllocationStrategy(0, 1);

            assertThat(test.getPermits(100)).as("desired 100").isOne();
            assertThat(test.getPermits(1)).as("desired 1 more").isZero();
        }

        @Test
        void getPermitDesiredZeroWithNoMin() {
            AllocationStrategy test = new SizeBasedAllocationStrategy(0, 1);

            assertThat(test.getPermits(0)).isZero();
        }

        @Test
        void getPermitDesiredZeroWithMin() {
            AllocationStrategy test = new SizeBasedAllocationStrategy(4, 8);

            assertThat(test.getPermits(0)).isEqualTo(4);
        }

        @Test
        void getPermitDesiredZeroWithMinPartiallyReached() {
            AllocationStrategy test = new SizeBasedAllocationStrategy(4, 8);
            //first getPermit returns min, but we can return some of these permits
            assertThat(test.getPermits(0)).as("initial warmup").isEqualTo(4);

            test.returnPermits(3);

            assertThat(test.getPermits(0)).as("partial warmup").isEqualTo(3);
        }

        @Test
        void getPermitDesiredNegative() {
            AllocationStrategy test = new SizeBasedAllocationStrategy(1, 2);

            assertThat(test.getPermits(-1)).isZero();
        }

        @Test
        void misuseReturnPermitThrowsWhenGoingOverMax() {
            final AllocationStrategy test = new SizeBasedAllocationStrategy(0, 1);

            assertThatIllegalArgumentException().isThrownBy(() -> test.returnPermits(1))
                                             .withMessage("Too many permits returned: returned=1, would bring to 2/1");

            assertThat(test.estimatePermitCount()).isEqualTo(1);
        }

        @Test
        void misuseReturnPermitsThrowsWhenGoingOverMax() {
            final AllocationStrategy test = new SizeBasedAllocationStrategy(0, 1);

            assertThatIllegalArgumentException().isThrownBy(() -> test.returnPermits(100))
                                             .withMessage("Too many permits returned: returned=100, would bring to 101/1");

            assertThat(test.estimatePermitCount()).isEqualTo(1);
        }

        @Test
        void minMaxPermitScenario1() {
            final AllocationStrategy test = new SizeBasedAllocationStrategy(2, 3);

            assertThat(test.estimatePermitCount()).as("initial state").isEqualTo(3);

            assertThat(test.getPermits(1)).as("granted for get(1)").isEqualTo(2);
            assertThat(test.estimatePermitCount()).as("after get").isOne();

            assertThat(test.getPermits(2)).as("granted for get(2)").isOne();
            assertThat(test.estimatePermitCount()).as("after second get").isZero();

            assertThat(test.getPermits(1)).as("get when max reached").isZero();
            assertThat(test.estimatePermitCount()).as("after no-op get").isZero();
        }

        @Test
        void minMaxPermitScenario2() {
            final AllocationStrategy test = new SizeBasedAllocationStrategy(4, 8);

            int firstGet = test.getPermits(1);
            test.returnPermits(1);
            test.returnPermits(2);
            int secondGet = test.getPermits(1);
            int drainingGet = test.getPermits(4);
            int starvedGet = test.getPermits(1);

            assertThat(firstGet).as("first getPermits(1)").isEqualTo(4);
            assertThat(secondGet).as("second getPermits(1)").isEqualTo(3);
            assertThat(drainingGet).as("draining getPermits(4)").isEqualTo(4);
            assertThat(starvedGet).as("starved getPermits(1)").isZero();
        }

        @Test
        void minMaxPermitWhenPartiallyStarved() {
            final AllocationStrategy test = new SizeBasedAllocationStrategy(4, 8);
            test.getPermits(6);

            assertThat(test.getPermits(4)).isEqualTo(2);
        }

        @Test
        void minMaxPermitWhenTotallyStarved() {
            final AllocationStrategy test = new SizeBasedAllocationStrategy(4, 8);
            test.getPermits(8);

            assertThat(test.getPermits(4)).isZero();
        }

        @Test
        void minMaxPermitWhenDesiredGreaterThanMin() {
            final AllocationStrategy test = new SizeBasedAllocationStrategy(4, 8);

            assertThat(test.getPermits(5)).isEqualTo(5);
        }

        @SuppressWarnings("FutureReturnValueIgnored")
        @ParameterizedTest(name = "{0} workers")
        @ValueSource(ints = {5, 10, 20})
        @Tag("race")
        void raceGetPermit(int workerCount) throws InterruptedException {
            final AllocationStrategy test = new SizeBasedAllocationStrategy(0, 10);

            LongAdder counter = new LongAdder();
            ExecutorService es = Executors.newFixedThreadPool(workerCount);

            for (int i = 0; i < 1_000_000; i++) {
                es.submit(() -> {
                    if (test.getPermits(1) == 1) {
                        counter.increment();
                        test.returnPermits(1);
                    }
                });
            }

            es.shutdown();
            es.awaitTermination(6, TimeUnit.SECONDS); //wait for the tasks to finish

            assertThat(counter.sum()).as("permits acquired").isEqualTo(1_000_000L);
            assertThat(test.estimatePermitCount()).as("end permit count").isEqualTo(10);
        }

        @SuppressWarnings("FutureReturnValueIgnored")
        @ParameterizedTest(name = "{0} workers")
        @ValueSource(ints = {5, 10, 20})
        @Tag("race")
        void racePermitsRandom(int workerCount, TestInfo testInfo) throws InterruptedException {
            final AllocationStrategy test = new SizeBasedAllocationStrategy(0, 10);

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
                    test.returnPermits(got);
                });
            }

            es.shutdown();
            es.awaitTermination(6, TimeUnit.SECONDS); //wait for the tasks to finish

            LOG.info("{} - got 0 permit: {}%", testInfo.getTestMethod().map(m -> m.getName() + ", " + testInfo.getDisplayName()).orElse("?"),
                    gotZeroCounter.sum() * 100d / 1_000_000d);
            assertThat(counter.sum()).as("permits acquired").isBetween(1_000_000L, 10_000_000L);
            assertThat(test.estimatePermitCount()).as("end permit count").isEqualTo(10);
        }

        @SuppressWarnings("FutureReturnValueIgnored")
        @ParameterizedTest(name = "{0} workers")
        @ValueSource(ints = {5, 10, 20})
        @Tag("race")
        void raceMixGetPermitWithGetRandomPermits(int workerCount, TestInfo testInfo) throws InterruptedException {
            final AllocationStrategy test = new SizeBasedAllocationStrategy(0, 10);

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

                        test.returnPermits(got);
                    }
                    else {
                        usedGetPermit.increment();
                        if (test.getPermits(1) == 1) {
                            test.returnPermits(1);
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

        @SuppressWarnings("FutureReturnValueIgnored")
        @ParameterizedTest(name = "{0} workers")
        @ValueSource(ints = {5, 10, 20})
        @Tag("race")
        void racePermitsRandomWithInnerLoop(int workerCount, TestInfo testInfo) throws InterruptedException {
            final AllocationStrategy test = new SizeBasedAllocationStrategy(0, 10);

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
                        test.returnPermits(1);
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

        //TODO race tests with a minimum
    }

    @DisplayName("unbounded")
    @Nested
    @SuppressWarnings("ClassCanBeStatic")
    class UnboundedTest {

        @Test
        void permitCountIsMaxValue() {
            AllocationStrategy test = new UnboundedAllocationStrategy();

            assertThat(test.estimatePermitCount()).isEqualTo(Integer.MAX_VALUE);
        }

        @Test
        void getPermitsDoesntChangeCount() {
            AllocationStrategy test = new UnboundedAllocationStrategy();

            assertThat(test.getPermits(100)).as("first try").isEqualTo(100);
            assertThat(test.getPermits(1000)).as("second try").isEqualTo(1000);
            assertThat(test.estimatePermitCount()).as("permit count unbounded").isEqualTo(Integer.MAX_VALUE);
        }

        @Test
        void getPermitDesiredZero() {
            AllocationStrategy test = new UnboundedAllocationStrategy();

            assertThat(test.getPermits(0)).isZero();
        }

        @Test
        void getPermitDesiredNegative() {
            AllocationStrategy test = new UnboundedAllocationStrategy();

            assertThat(test.getPermits(-1)).isZero();
        }

        @Test
        void returnPermitsDoesntChangeMax() {
            final AllocationStrategy test = new UnboundedAllocationStrategy();

            test.returnPermits(1000);

            assertThat(test.estimatePermitCount()).isEqualTo(Integer.MAX_VALUE);
        }
    }

}