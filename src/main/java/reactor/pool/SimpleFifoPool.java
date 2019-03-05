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

import java.util.Queue;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.jctools.queues.MpscLinkedQueue8;

import reactor.util.concurrent.Queues;

/**
 * This implementation is based on MPSC queues for both idle resources and pending {@link Pool#acquire()} Monos,
 * resulting in serving pending borrowers in FIFO order.
 *
 * See {@link SimplePool} for other characteristics of the simple pool.
 *
 * @author Simon Baslé
 */
final class SimpleFifoPool<POOLABLE> extends SimplePool<POOLABLE> {

    private static final Queue TERMINATED = Queues.empty().get();

    volatile Queue<Borrower<POOLABLE>>                                      pending;
    private static final AtomicReferenceFieldUpdater<SimpleFifoPool, Queue> PENDING = AtomicReferenceFieldUpdater.newUpdater(
            SimpleFifoPool.class, Queue.class, "pending");

    public SimpleFifoPool(DefaultPoolConfig<POOLABLE> poolConfig) {
        super(poolConfig);
        this.pending = new MpscLinkedQueue8<>(); //unbounded MPSC
    }

    @Override
    boolean pendingOffer(Borrower<POOLABLE> pending) {
        int maxPending = poolConfig.maxPending;
        for (;;) {
            int currentPending = PENDING_COUNT.get(this);
            if (maxPending >= 0 && currentPending == maxPending) {
                pending.fail(new IllegalStateException("Pending acquire queue has reached its maximum size of " + maxPending));
                return false;
            }
            else if (PENDING_COUNT.compareAndSet(this, currentPending, currentPending + 1)) {
                this.pending.offer(pending); //unbounded
                return true;
            }
        }
    }

    @Override
    Borrower<POOLABLE> pendingPoll() {
        Queue<Borrower<POOLABLE>> q = this.pending;
        Borrower<POOLABLE> b = q.poll();
        if (b != null) PENDING_COUNT.decrementAndGet(this);
        return b;
    }

    @Override
    public void dispose() {
        @SuppressWarnings("unchecked")
        Queue<Borrower<POOLABLE>> q = PENDING.getAndSet(this, TERMINATED);
        if (q != TERMINATED) {
            while(!q.isEmpty()) {
                q.poll().fail(new RuntimeException("Pool has been shut down"));
            }

            while (!elements.isEmpty()) {
                destroyPoolable(elements.poll()).block();
            }
        }
    }

    @Override
    public boolean isDisposed() {
        return PENDING.get(this) == TERMINATED;
    }

}
