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

import reactor.core.Disposable;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Simon Baslé
 */
public class TestUtils {

    public static final class TestPooledRef<T> implements PooledRef<T> {

        final T poolable;
        final int msSinceRelease;
        final int msSinceAllocation;
        final int acquireCount;

        public TestPooledRef(T poolable, int acquireCount, int secondsSinceRelease, int secondsSinceAllocation) {
            this.poolable = poolable;
            this.acquireCount = acquireCount;
            this.msSinceRelease = secondsSinceRelease * 1000;
            this.msSinceAllocation = secondsSinceAllocation * 1000;
        }

        @Override
        public T poolable() {
            return this.poolable;
        }

        @Override
        public Mono<Void> release() {
            return Mono.empty();
        }

        @Override
        public Mono<Void> invalidate() {
            return Mono.empty();
        }

        @Override
        public int acquireCount() {
            return acquireCount;
        }

        @Override
        public long lifeTime() {
            return msSinceAllocation;
        }

        @Override
        public long idleTime() {
            return msSinceRelease;
        }
    }

    public static final class PoolableTest implements Disposable {

        private static AtomicInteger defaultId = new AtomicInteger();

        public int usedUp;
        public int discarded;
        public final int id;

        public PoolableTest() {
            this(defaultId.incrementAndGet());
        }

        public PoolableTest(int id) {
            this.id = id;
            this.usedUp = 0;
        }

        public void clean() {
            this.usedUp++;
        }

        public boolean isHealthy() {
            return usedUp < 2;
        }

        @Override
        public void dispose() {
            discarded++;
        }

        @Override
        public boolean isDisposed() {
            return discarded > 0;
        }

        @Override
        public String toString() {
            return "PoolableTest{id=" + id + ", used=" + usedUp + "}";
        }
    }


}
