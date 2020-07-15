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

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.LLLL_Result;
import org.openjdk.jcstress.infra.results.II_Result;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import static org.openjdk.jcstress.annotations.Expect.*;

public class SimpleDequePoolStressTest {

	@JCStressTest
	@Outcome(id = {"EVICTED, ACQUIRED-10, 0, 0"}, expect = ACCEPTABLE,  desc = "evicted, acquired second resource")
	@Outcome(id = {"EVICTED, NOT ACQUIRED, 0, 1"}, expect = ACCEPTABLE_INTERESTING,  desc = "evicted, acquired failed fast during/before destroy")
	@Outcome(id = {"EVICTED, ACQUIRED1, 0, 0"}, expect = FORBIDDEN,  desc = "evicted resource acquired, before destroy")
	@Outcome(id = {"EVICTED, ACQUIRED1001, 0, 0"}, expect = FORBIDDEN,  desc = "evicted resource acquired, after destroy")
	@State
	public static class BackgroundEvictionVsAcquire {

		final AtomicInteger resource = new AtomicInteger();
		final AtomicBoolean firstResourceCreated = new AtomicBoolean();

		final SimpleDequePool<AtomicInteger> pool = PoolBuilder
				.from(Mono.defer(() -> {
					if (firstResourceCreated.getAndSet(true)) {
						return Mono.just(new AtomicInteger(-10));
					}
					resource.compareAndSet(0, 1);
					return Mono.just(resource);
				}))
				.evictInBackground(Duration.ZERO, Schedulers.immediate()) //we'll call directly
				.sizeBetween(1, 1) //we'll warmup the first resource
				.evictionPredicate((res, meta) -> res.get() > 0)
				.destroyHandler(ai -> Mono.fromRunnable(() -> ai.addAndGet(1000)))
				.build(conf -> new SimpleDequePool<>(conf, true));


		{
			int warmedUp = pool.warmup().block(Duration.ofSeconds(1));
			if (warmedUp != 1) throw new IllegalStateException("should have warmed up one");
			if (resource.get() != 1) throw new IllegalStateException("should have initiated");
		}

		@Actor
		public void backgroundEviction() {
			pool.evictInBackground();
		}

		@Actor
		public void acquisition(LLLL_Result r) {
			try {
				AtomicInteger ai = pool.acquire().block().poolable();
				r.r2 = "ACQUIRED" + ai.get();
			}
			catch (PoolAcquirePendingLimitException error) {
				r.r2 = "NOT ACQUIRED";
			}
		}

		@Arbiter
		public void arbiter(LLLL_Result r) {
			r.r1 = resource.get() > 1 ? "EVICTED" : "NOT EVICTED";
			r.r3 = pool.idleResources.size();
			r.r4 = pool.poolConfig.allocationStrategy().estimatePermitCount();
		}
	}

	@JCStressTest
	@Outcome(id = "2, 0", expect = ACCEPTABLE,  desc = "acquired a new resource")
	@Outcome(id = "-1, 1", expect = ACCEPTABLE,  desc = "acquired failed fast during/before destroy")
	@Outcome(id = "1, 0", expect = FORBIDDEN,  desc = "acquired the first resource, before destroy")
	@Outcome(id = "1001, 0", expect = FORBIDDEN,  desc = "acquired the first resource, after destroy")
	@State
	public static class MaxPendingAcquireWhileDestroying {

		final AtomicBoolean firstResourceCreated = new AtomicBoolean();

		final SimpleDequePool<AtomicInteger> pool = PoolBuilder
				.from(Mono.fromCallable(() -> new AtomicInteger(firstResourceCreated.getAndSet(true) ? 2 : 1)))
				.sizeBetween(0, 1)
				.maxPendingAcquire(0)
				.releaseHandler(ai -> Mono.fromRunnable(ai::incrementAndGet))
				.evictionPredicate((res, meta) -> {
					int v = res.get();
					return v > 1;
				})
				.destroyHandler(ai -> Mono.fromRunnable(() -> ai.addAndGet(1000)))
				.build(conf -> new SimpleDequePool<>(conf, true));

		private final PooledRef<AtomicInteger> ref = pool.acquire().block(Duration.ofMillis(100));

		@Actor
		public void invalidate() {
			ref.invalidate().block(Duration.ofMillis(100));
		}

		@Actor
		public void acquisition(II_Result r) {
			try {
				AtomicInteger ai = pool.acquire().block().poolable();
				r.r1 = ai.get();
			}
			catch (PoolAcquirePendingLimitException error) {
				r.r1 = -1;
			}
		}

		@Arbiter
		public void arbiter(II_Result r) {
			r.r2 = pool.poolConfig.allocationStrategy().estimatePermitCount();
		}
	}

}
