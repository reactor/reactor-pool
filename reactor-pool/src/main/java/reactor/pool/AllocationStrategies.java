/*
 * Copyright (c) 2019-2022 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.pool;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * Various pre-made {@link AllocationStrategy} for internal use.
 *
 * @author Simon Baslé
 */
final class AllocationStrategies {

	//FIXME tests
	static final class UnboundedAllocationStrategy extends AtomicInteger implements AllocationStrategy {

		@Override
		public int getPermits(int desired) {
			if (desired <= 0) {
				return 0;
			}
			int overflowCheck = addAndGet(desired); //+desired currently live
			if (overflowCheck < 0) {
				compareAndSet(overflowCheck, Integer.MAX_VALUE);
			}
			return desired;
		}

		@Override
		public int estimatePermitCount() {
			return Integer.MAX_VALUE;
		}

		@Override
		public void returnPermits(int returned) {
			int updated = addAndGet(-returned);
			if (updated < 0) {
				compareAndSet(updated, 0);
			}
		}

		@Override
		public int permitMinimum() {
			return 0;
		}

		@Override
		public int permitMaximum() {
			return Integer.MAX_VALUE;
		}

		@Override
		public int permitGranted() {
			return get();
		}
	}

	static final class SizeBasedAllocationStrategy implements AllocationStrategy {

		final int min;
		final int max;

		volatile int permits;
		static final AtomicIntegerFieldUpdater<SizeBasedAllocationStrategy> PERMITS = AtomicIntegerFieldUpdater.newUpdater(SizeBasedAllocationStrategy.class, "permits");

		SizeBasedAllocationStrategy(int min, int max) {
			if (min < 0) throw new IllegalArgumentException("min must be positive or zero");
			if (max < 1) throw new IllegalArgumentException("max must be strictly positive");
			if (min > max) throw new IllegalArgumentException("min must be less than or equal to max");
			this.min = min;
			this.max = max;
			PERMITS.lazySet(this, this.max);
		}

		@Override
		public int getPermits(int desired) {
			if (desired < 0) return 0;

			//impl note: this should be more efficient compared to the previous approach for desired == 1
			// (incrementAndGet + decrementAndGet compensation both induce a CAS loop, vs single loop here)
			for (;;) {
				int target;
				int p = permits;
				int granted = max - p;

				if (desired >= p) {
					target = p;
				}
				else if (granted < min) {
					target = Math.max(desired, min - granted);
				}
				else {
					target = desired;
				}

				if (PERMITS.compareAndSet(this, p, p - target)) {
					return target;
				}
			}
		}

		@Override
		public int estimatePermitCount() {
			return PERMITS.get(this);
		}

		@Override
		public int permitMinimum() {
			return min;
		}

		@Override
		public int permitMaximum() {
			return max;
		}

		@Override
		public int permitGranted() {
			return max - PERMITS.get(this);
		}

		@Override
		public void returnPermits(int returned) {
			for(;;) {
				int p = PERMITS.get(this);
				if (p + returned > max) {
					throw new IllegalArgumentException("Too many permits returned: returned=" + returned + ", would bring to " + (p + returned) + "/" + max);
				}
				if (PERMITS.compareAndSet(this, p, p + returned)) {
					return;
				}
			}
		}
	}
}
