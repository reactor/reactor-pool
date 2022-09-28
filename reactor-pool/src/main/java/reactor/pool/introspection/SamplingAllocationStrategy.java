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

import java.util.BitSet;
import java.util.LinkedList;
import java.util.Objects;
import java.util.Random;

import reactor.core.Exceptions;
import reactor.core.publisher.Mono;
import reactor.pool.AllocationStrategy;
import reactor.pool.PoolBuilder;

/**
 * An {@link AllocationStrategy} that delegates to an underlying {@link AllocationStrategy} and samples
 * the {@link #getPermits(int)} and {@link #returnPermits(int)} methods.
 * <p>
 * Only a given percentage of calls are sampled, and these are stored as stacktraces in the sampling strategy.
 * If a {@link AllocationStrategy#returnPermits(int)} call fails, a new {@link IllegalArgumentException}
 * is thrown with a composite cause that represents all the sampled calls (see {@link Exceptions#unwrapMultiple(Throwable)}).
 * <p>
 * The two types of calls are sampled separately. The reservoir sampling algorithm is used to instantiate a sampling decision
 * bitset over 100 slots, which are then applied to windows of 100 calls.
 * Access to the {@link #delegate} field is possible, as well as {@link #gettingSamplingRate}/{@link #returningSamplingRate}
 * configuration fields and {@link #gettingSamples}/{@link #returningSamples} collections.
 *
 * @author Simon Baslé
 */
public final class SamplingAllocationStrategy implements AllocationStrategy {

	//TODO replace with a more direct call to the standard strategy in case we make something public, eg. AllocationStrategies factory methods
	static AllocationStrategy sizeBetweenHelper(int min, int max) {
		AllocationStrategy[] as = new AllocationStrategy[1];
		PoolBuilder.from(Mono.empty())
		           .sizeBetween(min, max)
		           .build(config -> {
			           as[0] = config.allocationStrategy();
			           return null;
		           });
		return as[0];
	}

	/**
	 * An {@link SamplingAllocationStrategy} that wraps a {@link PoolBuilder#sizeBetween(int, int) sizeBetween} {@link AllocationStrategy}
	 * and samples calls to {@link AllocationStrategy#getPermits(int)} and {@link AllocationStrategy#returnPermits(int)}.
	 *
	 * @param min the minimum number of permits (see {@link PoolBuilder#sizeBetween(int, int)})
	 * @param max the maximum number of permits (see {@link PoolBuilder#sizeBetween(int, int)})
	 * @param getPermitsSamplingRate the sampling rate for {@link AllocationStrategy#getPermits(int)} calls (0.0d to 1.0d)
	 * @param returnPermitsSamplingRate the sampling rate for {@link AllocationStrategy#returnPermits(int)} calls (0.0d to 1.0d)
	 * @return a sampled {@link AllocationStrategy} that otherwise behaves like the {@link PoolBuilder#sizeBetween(int, int)} strategy
	 */
	public static SamplingAllocationStrategy sizeBetweenWithSampling(int min, int max, double getPermitsSamplingRate, double returnPermitsSamplingRate) {
		return new SamplingAllocationStrategy(sizeBetweenHelper(min, max), getPermitsSamplingRate, returnPermitsSamplingRate);
	}

	/**
	 * An {@link AllocationStrategy} that wraps a {@code delegate} and samples calls to {@link AllocationStrategy#getPermits(int)}
	 * and {@link AllocationStrategy#returnPermits(int)}. Only a given percentage of calls are sampled, and these are stored as stacktraces
	 * in the sampling strategy. If a {@link AllocationStrategy#returnPermits(int)} call fails, a new {@link IllegalArgumentException}
	 * is thrown with a composite cause that represents all the sampled calls (see {@link Exceptions#unwrapMultiple(Throwable)}).
	 * <p>
	 * The two types of calls are sampled separately. The reservoir sampling algorithm is used to instantiate a sampling decision
	 * bitset over 100 slots, which are then applied live to windows of 100 calls.
	 *
	 * @param delegate the underlying {@link AllocationStrategy}
	 * @param getPermitsSamplingRate the sampling rate for {@link AllocationStrategy#getPermits(int)} calls (0.0d to 1.0d)
	 * @param returnPermitsSamplingRate the sampling rate for {@link AllocationStrategy#returnPermits(int)} calls (0.0d to 1.0d)
	 * @return a sampled {@link AllocationStrategy} that otherwise behaves like the {@code delegate}
	 */
	public static SamplingAllocationStrategy withSampling(AllocationStrategy delegate, double getPermitsSamplingRate, double returnPermitsSamplingRate) {
		return new SamplingAllocationStrategy(delegate, getPermitsSamplingRate, returnPermitsSamplingRate);
	}

	/**
	 * The delegate {@link AllocationStrategy} backing this sampling strategy.
	 */
	public final AllocationStrategy    delegate;

	/**
	 * The list of samples for {@link #getPermits(int)} calls, as {@link Throwable} that trace back
	 * to the callers of the sampled calls.
	 */
	public final LinkedList<Throwable> gettingSamples;
	/**
	 * The configured sampling rate for {@link #getPermits(int)} calls, as a double between 0d and 1d (percentage).
	 */
	public final double                gettingSamplingRate;
	final        BitSet                gettingSampleDecisions;

	/**
	 * The list of samples for {@link #returnPermits(int)} calls, as {@link Throwable} that trace back
	 * to the callers of the sampled calls.
	 */
	public final LinkedList<Throwable> returningSamples;
	/**
	 * The configured sampling rate for {@link #returnPermits(int)} calls, as a double between 0d and 1d (percentage).
	 */
	public final double                returningSamplingRate;
	final        BitSet                returningSampleDecisions;

	long countGetting   = 0L;
	long countReturning = 0L;

	SamplingAllocationStrategy(AllocationStrategy delegate, double gettingSamplingRate, double returningSamplingRate) {
		if (gettingSamplingRate < 0d || gettingSamplingRate > 1d) {
			throw new IllegalArgumentException("gettingSamplingRate must be between 0d and 1d (percentage)");
		}
		if (returningSamplingRate < 0d || returningSamplingRate > 1d) {
			throw new IllegalArgumentException("returningSamplingRate must be between 0d and 1d (percentage)");
		}
		this.delegate = Objects.requireNonNull(delegate, "delegate");

		this.gettingSamples = new LinkedList<>();
		this.gettingSamplingRate = gettingSamplingRate;
		int percentOfGetting = (int) (this.gettingSamplingRate * 100.0d); //constrained between 0 and 1, percentage
		this.gettingSampleDecisions = sampleBitSet(percentOfGetting);

		this.returningSamples = new LinkedList<>();
		this.returningSamplingRate = returningSamplingRate;
		int percentOfReturning = (int) (this.returningSamplingRate * 100.0d); //constrained between 0 and 1, percentage
		this.returningSampleDecisions = sampleBitSet(percentOfReturning);
	}

	/**
	 * Reservoir sampling algorithm borrowed from Stack Overflow.
	 * https://stackoverflow.com/questions/12817946/generate-a-random-bitset-with-n-1s
	 *
	 * @param selectedOutOf100 cardinality of the bit set
	 * @return a random bitset
	 */
	static BitSet sampleBitSet(int selectedOutOf100) {
		int size = 100;
		Random rnd = new Random();
		BitSet result = new BitSet(size);
		int[] chosen = new int[selectedOutOf100];
		int i;
		for (i = 0; i < selectedOutOf100; ++i) {
			chosen[i] = i;
			result.set(i);
		}
		for (; i < size; ++i) {
			int j = rnd.nextInt(i + 1);
			if (j < selectedOutOf100) {
				result.clear(chosen[j]);
				result.set(i);
				chosen[j] = i;
			}
		}
		return result;
	}

	void sampleGetting(final int desired) {
		if (gettingSamplingRate == 0d) {
			return;
		}
		long c = countGetting++;
		boolean doSample;
		if (gettingSamplingRate == 1d) {
			doSample = true;
		}
		else {
			long cMod = c % 100L;
			doSample = gettingSampleDecisions.get((int) cMod);
		}

		if (doSample) {
			synchronized (gettingSamples) {
				gettingSamples.add(new RuntimeException("sample #" + c + ", getPermits(" + desired + ")"));
			}
		}
	}

	void sampleReturning(final int returned) {
		if (returningSamplingRate == 0d) {
			return;
		}
		long c = countReturning++;
		boolean doSample;
		if (returningSamplingRate == 1d) {
			doSample = true;
		}
		else {
			long cMod = c % 100L;
			doSample = returningSampleDecisions.get((int) cMod);
		}

		if (doSample) {
			synchronized (gettingSamples) {
				returningSamples.add(new RuntimeException("sample #" + c + ", returnPermits(" + returned +") while granted=" + permitGranted()));
			}
		}
	}

	@Override
	public int getPermits(int desired) {
		sampleGetting(desired);
		return delegate.getPermits(desired);
	}

	@Override
	public void returnPermits(int returned) {
		try {
			delegate.returnPermits(returned);
			//don't sample if the returnPermits failed (the thrown exception will point to the over-returning code path)
			sampleReturning(returned);
		}
		catch (Throwable permitError) {
			RuntimeException cause = Exceptions.multiple(gettingSamples);
			throw new IllegalArgumentException(
					String.format("Return permits failed, see cause for %d getPermits samples (%d%% of %d calls) and %d returnPermits samples (%d%% of %d calls). Reason: %s",
							this.gettingSamples.size(), (int) (gettingSamplingRate * 100d), this.countGetting,
							this.returningSamples.size(), (int) (returningSamplingRate * 100d), this.countReturning,
							permitError.getMessage()),
					cause);
		}
	}

	@Override
	public int estimatePermitCount() {
		return delegate.estimatePermitCount();
	}

	@Override
	public int permitGranted() {
		return delegate.permitGranted();
	}

	@Override
	public int permitMinimum() {
		return delegate.permitMinimum();
	}

	@Override
	public int permitMaximum() {
		return delegate.permitMaximum();
	}
}