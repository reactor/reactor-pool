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

package reactor.util.pool.builder;

import reactor.core.publisher.Mono;
import reactor.util.pool.api.PooledRef;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * An abstract base for most common statistics operator of {@link PooledRef}.
 *
 * @author Simon Baslé
 */
abstract class AbstractPooledRef<T> implements PooledRef<T> {

    final long creationTimestamp;

    volatile T poolable;

    volatile int acquireCount;

    static final AtomicIntegerFieldUpdater<AbstractPooledRef> ACQUIRE = AtomicIntegerFieldUpdater.newUpdater(AbstractPooledRef.class, "acquireCount");

    AbstractPooledRef(T poolable) {
        this.poolable = poolable;
        this.creationTimestamp = System.currentTimeMillis();
    }

    @Override
    public T poolable() {
        return poolable;
    }

    /**
     * Atomically increment the {@link #acquireCount()} of this slot, returning the new value.
     *
     * @return the incremented {@link #acquireCount()}
     */
    int acquireIncrement() {
        return ACQUIRE.incrementAndGet(this);
    }

    @Override
    public int acquireCount() {
        return ACQUIRE.get(this);
    }

    @Override
    public long timeSinceAllocation() {
        return System.currentTimeMillis() - creationTimestamp;
    }

    @Override
    public abstract Mono<Void> release();

    @Override
    public abstract Mono<Void> invalidate();

    @Override
    public String toString() {
        return "PooledRef{" +
                "poolable=" + poolable +
                ", timeSinceAllocation=" + timeSinceAllocation() + "ms" +
                ", acquireCount=" + acquireCount +
                '}';
    }
}
