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
package reactor.pool.impl;

import reactor.core.publisher.Mono;
import reactor.pool.PooledRef;
import reactor.pool.metrics.PoolMetricsRecorder;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * An abstract base for most common statistics operator of {@link PooledRef}.
 *
 * @author Simon Baslé
 */
abstract class AbstractPooledRef<T> implements PooledRef<T> {

    final long            creationTimestamp;
    final PoolMetricsRecorder metricsRecorder;

    volatile T poolable;

    volatile int acquireCount;
    static final AtomicIntegerFieldUpdater<AbstractPooledRef> ACQUIRE = AtomicIntegerFieldUpdater.newUpdater(AbstractPooledRef.class, "acquireCount");

    //might be peeked at by multiple threads, in which case a value of -1 indicates it is currently held/acquired
    volatile long timeSinceRelease;
    static final AtomicLongFieldUpdater<AbstractPooledRef> TIME_SINCE_RELEASE = AtomicLongFieldUpdater.newUpdater(AbstractPooledRef.class, "timeSinceRelease");

    AbstractPooledRef(T poolable, PoolMetricsRecorder metricsRecorder) {
        this.poolable = poolable;
        this.metricsRecorder = metricsRecorder;
        this.creationTimestamp = metricsRecorder.now();
        this.timeSinceRelease = -2L;
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
    int markAcquired() {
        int acq = ACQUIRE.incrementAndGet(this);
        long tsr = TIME_SINCE_RELEASE.getAndSet(this, -1);
        if (tsr > 0) {
            metricsRecorder.recordIdleTime(metricsRecorder.measureTime(tsr));
        }
        else if (tsr < -1L) { //allocated, never acquired
            metricsRecorder.recordIdleTime(metricsRecorder.measureTime(creationTimestamp));
        }
        return acq;
    }

    void markReleased() {
        this.timeSinceRelease = metricsRecorder.now();
    }

    @Override
    public int acquireCount() {
        return ACQUIRE.get(this);
    }

    @Override
    public long timeSinceAllocation() {
        return metricsRecorder.measureTime(creationTimestamp);
    }

    @Override
    public long timeSinceRelease() {
        long tsr = this.timeSinceRelease;
        if (tsr == -1L) { //-1 is when it's been marked as acquired
            return 0L;
        }
        if (tsr < 0L) tsr = creationTimestamp; //any negative date other than -1 is considered "never yet released"
        return metricsRecorder.measureTime(tsr);
    }

    /**
     * Implementors MUST have the Mono call {@link #markReleased()} upon subscription.
     * <p>
     * {@inheritDoc}
     */
    @Override
    public abstract Mono<Void> release();

    /**
     * Implementors MUST have the Mono call {@link #markReleased()} upon subscription.
     * <p>
     * {@inheritDoc}
     */
    @Override
    public abstract Mono<Void> invalidate();

    @Override
    public String toString() {
        return "PooledRef{" +
                "poolable=" + poolable +
                ", timeSinceAllocation=" + timeSinceAllocation() + "ms" +
                ", timeSinceRelease=" + timeSinceRelease() + "ms" +
                ", acquireCount=" + acquireCount +
                '}';
    }
}
