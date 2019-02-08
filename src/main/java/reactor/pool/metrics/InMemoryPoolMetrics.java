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
package reactor.pool.metrics;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.ShortCountsHistogram;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * A simple in memory {@link PoolMetricsRecorder} based on HdrHistograms than can also be used to get the metrics.
 *
 * @author Simon Baslé
 */
public class InMemoryPoolMetrics implements PoolMetricsRecorder {

    private final ShortCountsHistogram allocationSuccessHistogram;
    private final ShortCountsHistogram allocationErrorHistogram;
    private final ShortCountsHistogram resetHistogram;
    private final ShortCountsHistogram destroyHistogram;
    private final LongAdder recycledCounter;
    private final LongAdder slowPathCounter;
    private final LongAdder fastPathCounter;
    private final Histogram lifetimeHistogram;
    private final Histogram idleTimeHistogram;

    public InMemoryPoolMetrics() {
        long maxLatency = TimeUnit.HOURS.toMillis(1);
        int precision = 3; //precision 3 = 1/1000 of each time unit
        allocationSuccessHistogram = new ShortCountsHistogram(1L, maxLatency, precision);
        allocationErrorHistogram = new ShortCountsHistogram(1L, maxLatency, precision);
        resetHistogram = new ShortCountsHistogram(1L, maxLatency, precision);
        destroyHistogram = new ShortCountsHistogram(1L, maxLatency, precision);
        lifetimeHistogram = new Histogram(precision);
        idleTimeHistogram = new Histogram(precision);
        recycledCounter = new LongAdder();
        slowPathCounter = new LongAdder();
        fastPathCounter = new LongAdder();
    }

    @Override
    public long now() {
        return System.nanoTime() / 1000000;
    }

    @Override
    public long measureTime(long startTimeMillis) {
        final long l = (System.nanoTime() / 1000000) - startTimeMillis;
        if (l <= 0) return 1;
        return l;
    }

    @Override
    public void recordAllocationSuccessAndLatency(long latencyMs) {
        allocationSuccessHistogram.recordValue(latencyMs);
    }

    @Override
    public void recordAllocationFailureAndLatency(long latencyMs) {
        allocationErrorHistogram.recordValue(latencyMs);
    }

    @Override
    public void recordResetLatency(long latencyMs) {
        resetHistogram.recordValue(latencyMs);
    }

    @Override
    public void recordDestroyLatency(long latencyMs) {
        destroyHistogram.recordValue(latencyMs);
    }

    @Override
    public void recordRecycled() {
        recycledCounter.increment();
    }

    @Override
    public void recordLifetimeDuration(long millisecondsSinceAllocation) {
        this.lifetimeHistogram.recordValue(millisecondsSinceAllocation);
    }

    @Override
    public void recordIdleTime(long millisecondsIdle) {
        this.idleTimeHistogram.recordValue(millisecondsIdle);
    }

    @Override
    public void recordSlowPath() {
        this.slowPathCounter.increment();
    }

    @Override
    public void recordFastPath() {
        this.fastPathCounter.increment();
    }

    public long getAllocationTotalCount() {
        return allocationSuccessHistogram.getTotalCount() + allocationErrorHistogram.getTotalCount();
    }

    public long getAllocationSuccessCount() {
        return allocationSuccessHistogram.getTotalCount();
    }

    public long getAllocationErrorCount() {
        return allocationErrorHistogram.getTotalCount();
    }

    public long getResetCount() {
        return resetHistogram.getTotalCount();
    }

    public long getDestroyCount() {
        return destroyHistogram.getTotalCount();
    }

    public long getRecycledCount() {
        return recycledCounter.sum();
    }

    public ShortCountsHistogram getAllocationSuccessHistogram() {
        return allocationSuccessHistogram;
    }

    public ShortCountsHistogram getAllocationErrorHistogram() {
        return allocationErrorHistogram;
    }

    public ShortCountsHistogram getResetHistogram() {
        return resetHistogram;
    }

    public ShortCountsHistogram getDestroyHistogram() {
        return destroyHistogram;
    }

    public Histogram getLifetimeHistogram() {
        return lifetimeHistogram;
    }

    public Histogram getIdleTimeHistogram() {
        return idleTimeHistogram;
    }

    public long getFastPathCount() {
        return fastPathCounter.sum();
    }

    public long getSlowPathCount() {
        return slowPathCounter.sum();
    }
}
