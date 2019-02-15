/*
 * Copyright (c) 2018-Present Pivotal Software Inc, All Rights Reserved.
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
package reactor.pool;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * Various pre-made {@link AllocationStrategy} for internal use.
 *
 * @author Simon Baslé
 */
final class AllocationStrategies {

    static final AllocationStrategy UNBOUNDED = new AllocationStrategy() {

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
