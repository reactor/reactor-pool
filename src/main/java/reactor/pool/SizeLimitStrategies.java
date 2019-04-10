/*
 * Copyright (c) 2018-Present Pivotal Software Inc, All Rights Reserved.
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

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * Various pre-made {@link SizeLimitStrategy} for internal use.
 *
 * @author Simon Baslé
 * @author Stephane Maldini
 */
final class SizeLimitStrategies {

    static final class SizeBasedAllocationStrategy implements SizeLimitStrategy {

        final int max;

        volatile int permits;
        static final AtomicIntegerFieldUpdater<SizeBasedAllocationStrategy> PERMITS = AtomicIntegerFieldUpdater.newUpdater(SizeBasedAllocationStrategy.class, "permits");

        SizeBasedAllocationStrategy(int max) {
            this.max = max;
            PERMITS.lazySet(this, max);
        }

        @Override
        public int getPermits(int desired) {
            if (desired < 0) {
                throw new IllegalArgumentException("desired must be a positive number");
            }

            //impl note: this should be more efficient compared to the previous approach for desired == 1
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

    static final SizeLimitStrategy UNBOUNDED = new UnboundedSizeLimitStrategy();

    static final class UnboundedSizeLimitStrategy implements SizeLimitStrategy {

        @Override
        public int getPermits(int desired) {
            if (desired < 0) {
                throw new IllegalArgumentException("desired must be a positive number");
            }

            return desired;
        }

        @Override
        public int estimatePermitCount() {
            return Integer.MAX_VALUE;
        }

        @Override
        public void returnPermits(int returned) {
            //NO-OP
        }
    }

}
