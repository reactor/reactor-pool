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
 * Various pre-made {@link SizeLimitStrategy} for internal use.
 *
 * @author Simon Baslé
 */
final class SizeLimitStrategies {

    static final class BoundedSizeLimitStrategy implements SizeLimitStrategy {

        static final AtomicIntegerFieldUpdater<BoundedSizeLimitStrategy> CURRENT = AtomicIntegerFieldUpdater.newUpdater(BoundedSizeLimitStrategy.class, "current");

        final int max;

        final int min;

        volatile int current;

        BoundedSizeLimitStrategy(int max) {
            this(0, max);
        }

        BoundedSizeLimitStrategy(int min, int max) {
            this.min = Math.min(max, min);
            this.max = Math.max(max, min);
        }

        @Override
        public int getPermits(int desired) {
            if (desired < 0) {
                throw new IllegalArgumentException("desired must be a positive number");
            }

            for(;;) {
                int current = this.current;
                int newValue;
                int delta;


                if(current < min) {
                    newValue = min;
                    delta = min - current;
                }
                else if (current + desired > max) {
                    newValue = max;
                    delta = max - current;
                }
                else {
                    newValue = current + desired;
                    delta = desired;
                }

                if (CURRENT.compareAndSet(this, current, newValue)) {
                    return delta;
                }
            }
        }

        @Override
        public int estimatePermitCount() {
            return max - CURRENT.get(this);
        }

        @Override
        public void returnPermits(int returned) {
            CURRENT.addAndGet(this, -returned);
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
