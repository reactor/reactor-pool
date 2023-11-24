/*
 * Copyright (c) 2024 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.pool.decorators;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.pool.*;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.function.Tuple2;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.function.IntPredicate;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * The WorkStealingPool class represents a scheduler of instrumented pools, with acquisition task stealing
 * and borrower stealing between pools. It is suitable for scenarios where acquisition tasks
 * can be distributed among multiple executors to maximize throughput and resource utilization, while preventing the
 * SimpleDequeueLoop drainLoop method being run forever under very high load from a single thread when there are
 * permanently hundreds of thousands of active borrowers.
 * <p>
 * Each pool is assigned to a unique executor, that is not shared among pools.
 * <p>
 * When a resource is acquired:
 * <ul>
 *     <li>a random pool is selected like this: if the current thread is one of the pool executors, then the current pool
 *     of the current thread is selected. Else, a random pool is selected.
 *     <li>once a pool is selected, the acquisition is then scheduled in the associated (uniq, not shared) pool executor
 *     </li>
 *     <li>when an executor has finished to handle its acquisition tasks, it tries to steal pending borrowers from other
 *     pools, then if it still can acquire some resources, it also tries to steal pending acquisition taskss from any
 *     other pools</li>
 * </ul>
 * <p>
 * Unlike the ForkJoinPool, this class doesn't create any daemon threads, it is up to
 * you to provide an Executor for each pool instance (one single executor must be solely assigned to a single pool).
 */
final class WorkStealingPool<T> implements PoolScheduler<T>, InstrumentedPool.PoolMetrics {
    private static final Logger log = Loggers.getLogger(WorkStealingPool.class);

    private final Worker<T>[] workers;

    private final InstrumentedPool<T>[] pools;

    private final ThreadLocal<Worker<T>> currentWorker = ThreadLocal.withInitial(() -> null);

    private final LongAdder stealCount = new LongAdder();

    /**
     * Pools which can be signaled because they can acquire some resources so they can steal
     * some borrowers from other pools
     */
    private final AtomicBitSet signalablePools;

    /**
     * Pools which may have some pending borrowers to steal
     */
    private final AtomicBitSet pendingBorrowerPools;

    /**
     * Workers which may have some pending acquisition tasks to steal
     */
    private final AtomicBitSet pendingTaskPools;

    @SuppressWarnings("unchecked")
    WorkStealingPool(int size, Function<ResourceManager, Tuple2<InstrumentedPool<T>, Executor>> poolFactory) {
        this.signalablePools = new AtomicBitSet(size);
        this.pendingBorrowerPools = new AtomicBitSet(size);
        this.pendingTaskPools = new AtomicBitSet(size);

        this.workers = IntStream.range(0, size)
                .peek(signalablePools::set)
                .mapToObj(i -> new Worker<>(size, this, i, poolFactory))
                .peek(worker -> worker.exec.execute(() -> currentWorker.set(worker)))
                .toArray(Worker[]::new);

        this.pools = Stream.of(workers)
                .map(Worker::pool)
                .toArray(InstrumentedPool[]::new);
    }

    @Override
    public String toString() {
        return "[steals=" + stealCount() + "]";
    }

    public Mono<PooledRef<T>> acquire() {
        return acquire(Duration.ZERO);
    }

    public Mono<PooledRef<T>> acquire(Duration timeout) {
        return Mono.create(sink -> execute(pool -> pool.acquire(timeout).subscribe(sink::success, sink::error)));
    }

    public Mono<Integer> warmup() {
        AtomicInteger remaining = new AtomicInteger(pools.length);
        AtomicInteger count = new AtomicInteger();
        return Mono.create(sink ->
                Stream.of(pools).forEach(pool -> pool.warmup().subscribe(result -> {
                    count.addAndGet(result);
                    if (remaining.decrementAndGet() == 0) {
                        sink.success(count.get());
                    }
                })));
    }

    public PoolConfig<T> config() {
        // TODO there is no config currently for this pool decorator, so what config to return ?
        // FIXME For now, return the config of the first pool, that's probably not a good thing.
        return pools[0].config();
    }

    public Mono<Void> disposeLater() {
        // Create a Flux of Mono<Void> from disposeLater() of each Pool
        Flux<Mono<Void>> disposables = Flux.fromArray(pools)
                .map(Pool::disposeLater);

        // Wait for all disposables to complete
        return Flux.merge(disposables).then();
    }

    public PoolMetrics metrics() {
        return this;
    }

    // Metrics (TODO should be reworked using shared LongAdders ...)

    @Override
    public int acquiredSize() {
        return Stream.of(pools).mapToInt(p -> p.metrics().acquiredSize()).sum();
    }

    @Override
    public int allocatedSize() {
        return Stream.of(pools).mapToInt(p -> p.metrics().allocatedSize()).sum();
    }

    @Override
    public int idleSize() {
        return Stream.of(pools).mapToInt(p -> p.metrics().idleSize()).sum();
    }

    @Override
    public int pendingAcquireSize() {
        return Stream.of(pools).mapToInt(p -> p.metrics().pendingAcquireSize()).sum();
    }

    @Override
    public long secondsSinceLastInteraction() {
        return Stream.of(pools).mapToLong(p -> p.metrics().secondsSinceLastInteraction()).sum();
    }

    @Override
    public int getMaxAllocatedSize() {
        return Stream.of(pools).mapToInt(p -> p.metrics().getMaxAllocatedSize()).max().orElse(0);
    }

    @Override
    public int getMaxPendingAcquireSize() {
        return Stream.of(pools).mapToInt(p -> p.metrics().getMaxPendingAcquireSize()).sum();
    }

    @Override
    public boolean isInactiveForMoreThan(Duration duration) {
        for (InstrumentedPool<T> pool : pools) {
            if (pool.metrics().isInactiveForMoreThan(duration)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public List<InstrumentedPool<T>> getPools() {
        return Collections.unmodifiableList(Arrays.asList(pools));
    }

    @Override
    public long stealCount() {
        return stealCount.sum();
    }

    private void execute(Task<T> task) {
        Worker<T> currWorker = currentWorker.get();
        Worker<T> worker = currWorker == null ? nextWorker() : currWorker;
        worker.execute(task);
    }

    private Worker<T> nextWorker() {
        int index = ThreadLocalRandom.current().nextInt(workers.length);
        return workers[index];
    }

    private interface Task<T> {
        void run(InstrumentedPool<T> context);
    }

    private static final class SignalWork<T> implements Task<T> {
        static final SignalWork INSTANCE = new SignalWork();

        @Override
        public void run(InstrumentedPool<T> ctx) {
        }
    }

    private static final class Worker<T> implements Runnable, ResourceManager {
        final WorkStealingPool<T> workStealingPool;

        final AtomicInteger tasksScheduled = new AtomicInteger();

        final ConcurrentLinkedDeque<Task<T>> queue;

        final int workerIndex;

        final InstrumentedPool<T> pool;

        final Executor exec;

        final AtomicInteger signals = new AtomicInteger();

        ThreadLocalRandom rnd;

        final int[] tempWorkerIndexes; // temp array used to get random pos of worker indexes

        public Worker(int subpools, WorkStealingPool<T> workStealingPool, int workerIndex, Function<ResourceManager, Tuple2<InstrumentedPool<T>, Executor>> poolFactory) {
            this.workStealingPool = workStealingPool;
            this.workerIndex = workerIndex;
            this.queue = new ConcurrentLinkedDeque<>();
            Tuple2<InstrumentedPool<T>, Executor> tuple = poolFactory.apply(this);
            this.pool = tuple.getT1();
            this.exec = tuple.getT2();
            this.tempWorkerIndexes = new int[subpools];
        }

        InstrumentedPool<T> pool() {
            return pool;
        }

        void execute(Task<T> task) {
            queue.add(task);
            if (tasksScheduled.getAndIncrement() == 0) {
                workStealingPool.pendingTaskPools.set(workerIndex);
                exec.execute(this);
            }
        }

        @Override
        public void run() {
            ConcurrentLinkedDeque<Task<T>> queue = this.queue;
            AtomicInteger tasksScheduled = this.tasksScheduled;
            AtomicInteger signals = this.signals;
            int currentSignals = -1;
            boolean canAcquire;

            try {
                Task<T> task;

                if (rnd == null) {
                    rnd = ThreadLocalRandom.current();
                }

                do {
                    task = queue.poll();
                    if ((task instanceof SignalWork)) {
                        continue;
                    }
                    // if task is null, it means it has been stolen
                    if (task != null) {
                        signalAny(index -> index != workerIndex);
                        runTask(task);
                    }
                } while (tasksScheduled.decrementAndGet() > 0);

                // Get a snapshot of the current signals count, because we don't want to miss any signals while we are stealing
                currentSignals = signals.get();

                // No more acquisition tasks available, remove our worker index from the pendingTaskPools bitset.
                workStealingPool.pendingTaskPools.unset(workerIndex);

                // steal other pools, if we can acquire.
                if (canAcquire = pool.hasAvailableResources()) {
                    // We can acquire more resources, set ourself as signalable
                    workStealingPool.signalablePools.set(workerIndex);

                    // And start stealing pending borrowers from other pools
                    if (canAcquire = stealPendingBorrowers()) {
                        // If we can still acquire more resources, steal acquisition tasks from other pools
                        canAcquire = stealPendingTasks();
                    }
                }

                if (canAcquire) {
                    // We can currently acquire more resources, register this worker in the set of signalable workers
                    workStealingPool.signalablePools.set(workerIndex);
                    // We don't have any pending borrowers, make sure we are not registered in the set of pending pools
                    workStealingPool.pendingBorrowerPools.unset(workerIndex);
                } else {
                    // Can't currently acquire more resources, remove this worker from set of signalable workers.
                    workStealingPool.signalablePools.unset(workerIndex);
                    // We do have pendings, register this worker in the set of pending pools, so others can steal us
                    workStealingPool.pendingBorrowerPools.set(workerIndex);
                    // And signal another signalable worker
                    signalAny(index -> index != workerIndex);
                }
            } catch (Exception e) {
                log.warn("exception", e);
            } finally {
                if (!signals.compareAndSet(currentSignals, 0)) {
                    // Someone signaled us while we were stealing, reschedule the signal to ourself
                    execute(SignalWork.INSTANCE);
                }
            }
        }

        /**
         * Signal this worker, so it can attempt to stil any pools currently registered in the bitset that are holding
         * the current set of pools that do have some pending borrowers.
         */
        void signal() {
            if (signals.getAndIncrement() == 0) {
                execute(SignalWork.INSTANCE);
            }
        }

        /**
         * Signal the first available signalable pool which index is matching a specified predicate.
         * @param filter the predicate used to select another worker
         */
        private void signalFirst(IntPredicate filter) {
            workStealingPool.signalablePools.accept(index -> {
                if (filter.test(index)) {
                    workStealingPool.workers[index].signal();
                    // stop iterating on next pending worker
                    return false;
                }
                // continue iterating on next pending worker
                return true;
            });
        }

        /**
         * Signal another random signalable pool which index is matching a specified predicate.
         * @param filter the predicate used to select another worker
         */
        private void signalAny(IntPredicate filter) {
            workStealingPool.signalablePools.acceptRandom(rnd, tempWorkerIndexes, index -> {
                if (filter.test(index)) {
                    workStealingPool.workers[index].signal();
                    // stop iterating on next pending worker
                    return false;
                }
                // continue iterating on next pending worker
                return true;
            });
        }

        private void runTask(Task<T> task) {
            try {
                task.run(pool);
            } catch (Throwable t) {
                log.warn("Exception caught while running worker task", t);
            }
        }

        /**
         * Try to steal any pending acquisition tasks from other workers registered in the bitset of the pending tasks.
         * @return true if the current  worker pool can acquire more resources, false if not
         */
        private boolean stealPendingTasks() {
            Worker<T>[] workers = workStealingPool.workers;
            LongAdder stealCount = workStealingPool.stealCount;

            workStealingPool.pendingTaskPools.acceptRandom(rnd, tempWorkerIndexes, index -> {
                Task<T> work;
                Worker<T> workerToSteal;
                boolean canAcquire;

                if (index == workerIndex) {
                    // continue iterating on next random pool index
                    return true;
                }

                workerToSteal = workers[index];

                while ((canAcquire = pool.hasAvailableResources()) && (work = workerToSteal.queue.pollFirst()) != null) {
                    stealCount.increment();
                    runTask(work);
                }
                // if we can't acquire more resources, false is returned, meaning stop iterating over next random pending workers.
                return canAcquire;
            });

            return pool.hasAvailableResources(); // true means we can still acquire more resources
        }

        /**
         * Try to steal pending borrowers from other pools registered in the bitset of pending borrowers
         * @return true if the current  worker pool can acquire more resources, false if not
         */
        private boolean stealPendingBorrowers() {
            Worker<T>[] workers = workStealingPool.workers;
            LongAdder stealCount = workStealingPool.stealCount;

            workStealingPool.pendingBorrowerPools.acceptRandom(rnd, tempWorkerIndexes, index -> {
                Worker<T> workerToSteal;
                boolean canAcquire;

                if (index == workerIndex) {
                    return true;
                }
                workerToSteal = workers[index];
                while ((canAcquire = pool.hasAvailableResources()) && pool.transferBorrowersFrom(workerToSteal.pool)) {
                    stealCount.add(1);
                }
                return canAcquire;
            });

            return pool.hasAvailableResources();
        }

        /**
         * Sub pools implementation are assumed to call this method when either one idle resources becomes available
         * or when some more resources can be allocated. The method will then signal the associated worker, so it can
         * attempt to steal other pools that currently do have some pending borrowers or some pending acquisition tasks.
         */
        @Override
        public void resourceAvailable() {
            signal();
        }
    }

    /**
     * Efficiently manages a set of enabled bits using an AtomicIntegerArray.
     */
    static final class AtomicBitSet {
        private final AtomicIntegerArray bits;

        interface BitsVisitor {
            boolean accept(int index);
        }

        /**
         * Constructs an AtomicBitSet with the specified number of bits.
         *
         * @param bits The number of bits to accommodate.
         */
        AtomicBitSet(int bits) {
            // determines the required length of the AtomicIntegerArray to accommodate the specified number of bits.
            // Adds 31 to the bits for division rounding purposes, making sure it's rounded up to the nearest multiple of 32,
            // and then divide it 32.
            int arrayLength = (bits + 31) >>> 5; // unsigned / 32
            this.bits = new AtomicIntegerArray(arrayLength);
        }

        /**
         * Clear all bits
         */
        void clear() {
            int length = bits.length();
            for (int i = 0; i < length; i++) {
                bits.set(i, 0);
            }
        }

        /**
         * Sets the specified bit.
         *
         * @param n The index of the bit to set.
         * @return the previous value
         */
         int set(int n) {
            int mask = 1 << n;
            int index = n >>> 5;
            return bits.getAndAccumulate(index, mask, (oldValue, maskToAdd) -> oldValue | maskToAdd);
        }

        /**
         * Unsets the specified bit if it was previously set.
         *
         * @param n The index of the bit to unset.
         * @return true if the bit has been unset, false if not
         */
        boolean unsetIfSet(int n) {
            int mask = 1 << n;
            int index = n >>> 5;
            int oldValue = bits.get(index);
            boolean wasSet = (oldValue & mask) != 0;

            if (wasSet) {
                return bits.compareAndSet(index, oldValue, oldValue & ~mask);
            }
            return false;
        }

        /**
         * Sets the specified bit if it wasn't previously set.
         *
         * @param n The index of the bit to set if not already set.
         * @return true if the bit has been set, false if not
         */
        boolean setIfNotSet(int n) {
            int mask = 1 << n;
            int index = n >>> 5;
            int oldValue = bits.get(index);
            boolean wasSet = (oldValue & mask) != 0;

            if (!wasSet) {
                return bits.compareAndSet(index, oldValue, oldValue | mask);
            }
            return false;
        }

        /**
         * Unsets the specified bit.
         *
         * @param n The index of the bit to unset.
         */
        void unset(int n) {
            int mask = ~(1 << n);
            int index = n >>> 5;
            bits.getAndAccumulate(index, mask, (oldValue, maskToRemove) -> oldValue & maskToRemove);
        }

        /**
         * Checks if the specified bit is enabled.
         *
         * @param n The index of the bit to check.
         * @return True if the bit is set, false otherwise.
         */
         boolean contains(int n) {
            int bit = 1 << n;
            int idx = n >>> 5;
            int num = bits.get(idx);
            return (num & bit) != 0;
        }

        /**
         * Retrieves a random enabled bit index.
         *
         * @param rnd The ThreadLocalRandom object to generate randomness.
         * @return A randomly chosen enabled bit index, or -1 if no bits are set.
         */
        public int getRandom(ThreadLocalRandom rnd) {
            int length = bits.length();
            int value = 0;
            int randomIdx = 0;

            if (length > 1) {
                randomIdx = rnd.nextInt(length);
                for (int i = 0; i < length; i ++) {
                    if (randomIdx >= length) {
                        randomIdx = 0;
                    }
                    value = bits.get(randomIdx);
                    if (value != 0) {
                        break;
                    }
                }
            } else {
                value = bits.get(0);
            }

            if (value == 0) {
                return -1; // No bits are set to 1
            }

            int iterations = rnd.nextInt(1, Integer.bitCount(value) + 1);
            int result = -1;
            for (int i = 0; i < iterations && value != 0; i ++) {
                int rightmostSetBit = value & -value; // Extract the rightmost set bit
                int bitIndex = (randomIdx << 5) + Integer.numberOfTrailingZeros(rightmostSetBit); // Calculate bit index
                result = bitIndex;
                value &= (value - 1); // Clear the rightmost set bit
            }

            return result;
        }

        /**
         * Retrieves all enabled bit indexes into the provided array.
         * Take care, the provided array length is assumed to match the max number of enabled bits.
         *
         * @param result The array to store the enabled bit indexes.
         * @return The count of enabled bits stored in the result array.
         */
        int getBits(int[] result) {
            int count = 0;
            for (int i = 0; i < bits.length(); i++) {
                int value = bits.get(i);

                while (value != 0) {
                    int rightmostSetBit = value & -value; // Extract the rightmost set bit
                    int bitIndex = (i << 5) + Integer.numberOfTrailingZeros(rightmostSetBit); // Calculate bit index
                    result[count++] = bitIndex;
                    if (count == result.length) {
                        return count;
                    }
                    value &= (value - 1); // Clear the rightmost set bit
                }
            }
            return count;
        }

        /**
         * Retrieves random enabled bit indexes into the provided array.
         * Take care, the provided array length is assumed to match the max number of enabled bits.
         *
         * @param result The array to store randomly chosen enabled bit indexes.
         * @param rnd    The ThreadLocalRandom object to generate randomness.
         * @return The count of enabled bits stored in the result array.
         */
         int getRandomBits(int[] result, ThreadLocalRandom rnd) {
            int count = getBits(result);
            // Fisher-Yates shuffle
            for (int i = count - 1; i > 0; i--) {
                int index = rnd.nextInt(i + 1);
                int temp = result[index];
                result[index] = result[i];
                result[i] = temp;
            }
            return count;
        }

        /**
         * Accepts a visitor for each enabled bit index.
         *
         * @param visitor The BitsVisitor implementing logic for accepted bit indexes. The iteration of enabled bits
         *                stops when the visitor returns false, else it continues until no more enabled bits are available
         */
        int accept(BitsVisitor visitor) {
            int visited = 0;
            for (int i = 0; i < bits.length(); i++) {
                int value = bits.get(i);

                while (value != 0) {
                    visited ++;
                    int rightmostSetBit = value & -value; // Extract the rightmost set bit
                    int bitIndex = (i << 5) + Integer.numberOfTrailingZeros(rightmostSetBit); // Calculate bit index
                    if (!visitor.accept(bitIndex)) {
                        return visited;
                    }
                    value &= (value - 1); // Clear the rightmost set bit
                }
            }
            return visited;
        }

        /**
         * Accepts a visitor for randomly chosen enabled bit indexes.
         *
         * @param random  The ThreadLocalRandom object to generate randomness.
         * @param indexes The array containing randomly chosen bit indexes.
         * @param visitor The BitsVisitor implementing logic for accepted bit indexes. The iteration of enabled bits
         *                stops when the visitor returns false, else it continues until no more enabled bits are available
         */
        void acceptRandom(ThreadLocalRandom random, int[] indexes, BitsVisitor visitor) {
            int size = getRandomBits(indexes, random);
            if (size == 0) {
                return;
            }
            for (int i = 0; i < size; i++) {
                if (!visitor.accept(indexes[i])) {
                    break;
                }
            }
        }
    }
}
