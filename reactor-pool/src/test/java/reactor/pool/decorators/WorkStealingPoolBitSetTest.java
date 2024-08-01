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

package reactor.pool.decorators;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import reactor.pool.decorators.WorkStealingPool.AtomicBitSet;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.anyOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;

class WorkStealingPoolBitSetTest {

    private static final int MAX_BITS = 64;
    private static final int MAX_THREADS = 10;
    private static final int REPEAT_COUNT = 1000;

    @Test
    void testSetAndGet() {
        AtomicBitSet bitSet = new AtomicBitSet(MAX_BITS);
        for (int i = 0; i < MAX_BITS; i ++) {
            bitSet.clear();
            bitSet.set(i);
            assertTrue(bitSet.contains(i));
        }
    }

    @Test
    void testContains() {
        AtomicBitSet bitSet = new AtomicBitSet(MAX_BITS);
        int[] result = new int[MAX_BITS];

        for (int i = 0; i < MAX_BITS; i ++) {
            assertFalse(bitSet.contains(i));
            bitSet.set(i);
            assertTrue(bitSet.contains(i));
            int count = bitSet.getBits(result);
            assertEquals(i+1, count);
            for (int j = 0; j < count; j ++) {
                assertTrue(bitSet.contains(result[j]));
            }
        }
    }

    @Test
    void testRandomBitOperations() {
        int maxBits=100;
        AtomicBitSet bitSet = new AtomicBitSet(maxBits);

        for (int i = 0; i < maxBits; i += 2) {
            bitSet.set(i);
        }

        int[] result = new int[maxBits];
        int count = bitSet.getRandomBits(result, ThreadLocalRandom.current());

        assertThat(count, is(50));

        for (int value : result) {
            assertThat(value % 2, is(0));
            assertThat(bitSet.contains(value), is(true));
        }

        int randomIndex = bitSet.getRandom(ThreadLocalRandom.current());
        assertThat(randomIndex, allOf(greaterThanOrEqualTo(0), lessThan(100), is(new EvenMatcher())));
    }

    @Test
    void testVisitor() {
        AtomicBitSet bitSet = new AtomicBitSet(MAX_BITS);

        bitSet.set(3);
        bitSet.set(8);
        bitSet.set(15);

        int[] result = new int[MAX_BITS];
        bitSet.accept(index -> {
            result[index] = index;
            return true;
        });

        assertThat(result[3], is(3));
        assertThat(result[8], is(8));
        assertThat(result[15], is(15));

        int[] visitedIndexes = new int[MAX_BITS];
        int[] tmp = new int[MAX_BITS];
        bitSet.acceptRandom(ThreadLocalRandom.current(), tmp, index -> {
            visitedIndexes[index] = index;
            return true;
        });

        assertThat(visitedIndexes[3], anyOf(is(3), is(8), is(15)));
        assertThat(visitedIndexes[8], anyOf(is(3), is(8), is(15)));
        assertThat(visitedIndexes[15], anyOf(is(3), is(8), is(15)));
        for (int i = 0; i < MAX_BITS; i++) {
            if (i != 3 && i != 8 && i != 15) {
                assertThat(visitedIndexes[i], is(0));
            }
        }
    }

    @RepeatedTest(100)
    void testConcurrentAccess() throws InterruptedException {
        AtomicBitSet bitSet = new AtomicBitSet(MAX_BITS);
        AtomicInteger counter = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(MAX_THREADS * REPEAT_COUNT);
        Thread[] threads = new Thread[MAX_THREADS];

        for (int i = 0; i < MAX_THREADS; i++) {
            (threads[i] = new Thread(() -> {
                for (int j = 0; j < REPEAT_COUNT; j++) {
                    int randomIndex = ThreadLocalRandom.current().nextInt(MAX_BITS);
                    if (!bitSet.contains(randomIndex)) {
                        if (bitSet.setIfNotSet(randomIndex)) {
                            counter.incrementAndGet();
                        }
                    }
                    latch.countDown();
                }
            })).start();
        }

        for (int i = 0; i < MAX_THREADS; i ++) {
            threads[i].join();
        }

        latch.await();

        // Ensure that all set operations were successful
        assertEquals(counter.get(), bitSet.getBits(new int[MAX_BITS]));
    }

    static class EvenMatcher extends BaseMatcher<Integer> {
        @Override
        public boolean matches(Object item) {
            if (!(item instanceof Integer)) {
                return false;
            }
            return ((Integer) item) % 2 == 0;
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("an even number");
        }
    }
}