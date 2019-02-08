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

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * Various pre-made {@link AllocationStrategy}.
 *
 * @author Simon Baslé
 */
public final class AllocationStrategies {

    /**
     * Let the {@link Pool} allocate at most {@code max} resources, rejecting further allocations until
     * {@link AllocationStrategy#returnPermits(int)} has been called.
     *
     * @param max the maximum number of live resources to keep in the pool
     * @return an {@link AllocationStrategy} that allows at most N live resources
     */
    public static final AllocationStrategy allocatingMax(int max) {
        return new SizeBasedAllocationStrategy(max);
    }

    /**
     * Let the {@link Pool} allocate new resources when no idle resource is available, without limit.
     *
     * @return an unbounded {@link AllocationStrategy}
     */
    public static final AllocationStrategy unbounded() {
        return UNBOUNDED;
    }

    private static final AllocationStrategy UNBOUNDED = new AllocationStrategy() {

        @Override
        public int getPermits(int desired) {
            return desired <= 0 ? 0 : desired;
        }

        @Override
        public int estimatePermitCount() {
            return Integer.MAX_VALUE;
        }

        @Override
        public void returnPermits(int returned) {
            //NO-OP
        }
    };

    static final class SizeBasedAllocationStrategy implements AllocationStrategy {

        final int max;

        volatile int permits;
        static final AtomicIntegerFieldUpdater<SizeBasedAllocationStrategy> PERMITS = AtomicIntegerFieldUpdater.newUpdater(SizeBasedAllocationStrategy.class, "permits");

        SizeBasedAllocationStrategy(int max) {
            this.max = Math.max(1, max);
            PERMITS.lazySet(this, this.max);
        }

        @Override
        public int getPermits(int desired) {
            if (desired < 1) return 0;

            //impl note: this should be more performant compared to the previous approach for desired == 1
            // (incrementAndGet + decrementAndGet compensation both induce a CAS loop, vs single loop here)
            for (;;) {
                int p = permits;
                int possible = Math.min(desired, p);

                if (PERMITS.compareAndSet(this, p, p - possible)) {
                    return possible;
                }
            }
        }

        @Override
        public int estimatePermitCount() {
            return PERMITS.get(this);
        }

        @Override
        public void returnPermits(int returned) {
            PERMITS.addAndGet(this, returned);
        }
    }
}
