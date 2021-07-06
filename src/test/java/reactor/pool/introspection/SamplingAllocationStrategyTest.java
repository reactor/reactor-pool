/*
 * Copyright (c) 2021 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.pool.introspection;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import reactor.pool.AllocationStrategy;

import static org.assertj.core.api.Assertions.*;

class SamplingAllocationStrategyTest {

	@Test
	void delegateNull() {
		assertThatNullPointerException()
				.isThrownBy(() -> new SamplingAllocationStrategy(null, 1d, 1d))
				.withMessage("delegate");
	}

	@Test
	void gettingSamplingRateNegative() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> new SamplingAllocationStrategy(SamplingAllocationStrategy.sizeBetweenHelper(0, 100),
						-0.5d, 0.5d))
				.withMessage("gettingSamplingRate must be between 0d and 1d (percentage)");
	}

	@Test
	void gettingSamplingRateZeroDoesNotIncreaseCountOrSampleSize() {
		SamplingAllocationStrategy strategy = new SamplingAllocationStrategy(SamplingAllocationStrategy.sizeBetweenHelper(0, 100),
				0.0d, 0.5d);

		for (int i = 0; i < 100; i++) {
			strategy.getPermits(1);
		}

		assertThat(strategy.countGetting).isZero();
		assertThat(strategy.gettingSamples).isEmpty();
	}

	@Test
	void gettingSamplingRateFiftyPercent() {
		SamplingAllocationStrategy strategy = new SamplingAllocationStrategy(SamplingAllocationStrategy.sizeBetweenHelper(0, 100),
				0.5d, 0.5d);

		for (int i = 0; i < 100; i++) {
			strategy.getPermits(1);
		}

		assertThat(strategy.countGetting).isEqualTo(100);
		assertThat(strategy.gettingSamples.size()).isEqualTo(50);
	}

	@Test
	void gettingSamplingRateOne() {
		SamplingAllocationStrategy strategy = new SamplingAllocationStrategy(SamplingAllocationStrategy.sizeBetweenHelper(0, 100),
				1.0d, 0.5d);

		for (int i = 0; i < 100; i++) {
			strategy.getPermits(1);
		}

		assertThat(strategy.countGetting).isEqualTo(100);
		assertThat(strategy.gettingSamples).hasSize(100);
	}

	@Test
	void gettingSamplingRateAboveOne() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> new SamplingAllocationStrategy(SamplingAllocationStrategy.sizeBetweenHelper(0, 100),
						1.5d, 0.5d))
				.withMessage("gettingSamplingRate must be between 0d and 1d (percentage)");
	}

	@Test
	void returningSamplingRateNegative() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> new SamplingAllocationStrategy(SamplingAllocationStrategy.sizeBetweenHelper(0, 100),
						0.5d, -0.5d))
				.withMessage("returningSamplingRate must be between 0d and 1d (percentage)");
	}

	@Test
	void returningSamplingRateZeroDoesNotIncreaseCountOrSampleSize() {
		SamplingAllocationStrategy strategy = new SamplingAllocationStrategy(SamplingAllocationStrategy.sizeBetweenHelper(0, 100),
				0.5d, 0.0d);
		strategy.getPermits(100); //needed to make returnPermits calls possible

		for (int i = 0; i < 100; i++) {
			strategy.returnPermits(1);
		}

		assertThat(strategy.countReturning).isZero();
		assertThat(strategy.returningSamples).isEmpty();
	}

	@Test
	void returningSamplingRateFiftyPercent() {
		SamplingAllocationStrategy strategy = new SamplingAllocationStrategy(SamplingAllocationStrategy.sizeBetweenHelper(0, 100),
				0.5d, 0.5d);
		strategy.getPermits(100); //needed to make returnPermits calls possible

		for (int i = 0; i < 100; i++) {
			strategy.returnPermits(1);
		}

		assertThat(strategy.countReturning).isEqualTo(100);
		assertThat(strategy.returningSamples.size()).isEqualTo(50);
	}

	@Test
	void returningSamplingRateOne() {
		SamplingAllocationStrategy strategy = new SamplingAllocationStrategy(SamplingAllocationStrategy.sizeBetweenHelper(0, 100),
				0.5d, 1d);
		strategy.getPermits(100); //needed to make returnPermits calls possible

		for (int i = 0; i < 100; i++) {
			strategy.returnPermits(1);
		}

		assertThat(strategy.countReturning).isEqualTo(100);
		assertThat(strategy.returningSamples).hasSize(100);
	}

	@Test
	void returningSamplingRateAboveOne() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> new SamplingAllocationStrategy(SamplingAllocationStrategy.sizeBetweenHelper(0, 100),
						0.5d, 1.5d))
				.withMessage("returningSamplingRate must be between 0d and 1d (percentage)");
	}

	@Test
	void smokeTestBusinessException() {
		AllocationStrategy strategy = SamplingAllocationStrategy.sizeBetweenWithSampling(0, 100, 0.5d, 0.1d);

		for (int i = 0; i < 100; i++) {
			strategy.getPermits(1);
		}
		for (int i = 0; i < 100; i++) {
			strategy.returnPermits(1);
		}
		assertThatIllegalArgumentException()
				.isThrownBy(() -> strategy.returnPermits(1))
				.withMessage("Return permits failed, see cause for 50 getPermits samples (50% of 100 calls) and " +
						"10 returnPermits samples (10% of 100 calls). Reason: Too many permits returned: " +
						"returned=1, would bring to 101/100");
	}

	@Test
	void smokeTestFieldAccess() {
		AllocationStrategy delegate = Mockito.mock(AllocationStrategy.class);
		SamplingAllocationStrategy strategy = SamplingAllocationStrategy.withSampling(delegate, 0.2, 0.8);

		assertThat(strategy.delegate).as("delegate").isSameAs(delegate);
		assertThat(strategy.gettingSamplingRate).as("gettingSamplingRate").isEqualTo(0.2);
		assertThat(strategy.returningSamplingRate).as("returningSamplingRate").isEqualTo(0.8);
	}

	@Test
	void smokeTestListFieldAccess() {
		//this test is separated to use a more predictable sampling rate of 100%
		AllocationStrategy delegate = Mockito.mock(AllocationStrategy.class);
		SamplingAllocationStrategy strategy = SamplingAllocationStrategy.withSampling(delegate, 1d, 1d);

		for (int i = 0; i < 10; i++) {
			strategy.getPermits(1);
			if (i % 2 == 0) { //we only return half, allowing to ensure each list is different
				strategy.returnPermits(1);
			}
		}

		assertThat(strategy.gettingSamples)
				.as("gettingSamples")
				.isNotEmpty()
				.hasSize(10);
		assertThat(strategy.returningSamples)
				.as("returningSamples")
				.isNotEmpty()
				.hasSize(5);
	}

}