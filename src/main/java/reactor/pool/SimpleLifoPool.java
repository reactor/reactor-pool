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

import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * This implementation is based on {@link java.util.concurrent.ConcurrentLinkedQueue} MPMC queue
 * for idle resources and a {@link ConcurrentLinkedDeque} for pending {@link Pool#acquire()}
 * Monos, used as a stack ({@link java.util.Deque#offerFirst(Object)},
 * {@link Deque#pollFirst()}. This results in serving pending borrowers in LIFO order.
 *
 * See {@link SimplePool} for other characteristics of the simple pool.
 *
 * @author Simon Baslé
 */
final class SimpleLifoPool<POOLABLE> extends SimplePool<POOLABLE> {

    private static final ConcurrentLinkedDeque TERMINATED = new ConcurrentLinkedDeque();

    volatile ConcurrentLinkedDeque<Borrower<POOLABLE>>                                      pending;
    private static final AtomicReferenceFieldUpdater<SimpleLifoPool, ConcurrentLinkedDeque> PENDING = AtomicReferenceFieldUpdater.newUpdater(
            SimpleLifoPool.class, ConcurrentLinkedDeque.class, "pending");

    public SimpleLifoPool(DefaultPoolConfig<POOLABLE> poolConfig) {
        super(poolConfig);
        this.pending = new ConcurrentLinkedDeque<>(); //unbounded
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
                this.pending.offerFirst(pending); //unbounded
                return true;
            }
        }
    }

    @Override
    Borrower<POOLABLE> pendingPoll() {
        ConcurrentLinkedDeque<Borrower<POOLABLE>> q = this.pending;
        Borrower<POOLABLE> b = q.pollFirst();
        if (b != null) PENDING_COUNT.decrementAndGet(this);
        return b;
    }

    @Override
    void cancelAcquire(Borrower<POOLABLE> borrower) {
        if (!isDisposed()) { //ignore pool disposed
            ConcurrentLinkedDeque<Borrower<POOLABLE>> q = this.pending;
            if (q.remove(borrower)) {
                PENDING_COUNT.decrementAndGet(this);
            }
        }
    }

    @Override
    public void dispose() {
        @SuppressWarnings("unchecked")
        ConcurrentLinkedDeque<Borrower<POOLABLE>> q = PENDING.getAndSet(this, TERMINATED);
        if (q != TERMINATED) {
            Borrower<POOLABLE> p;
            while((p = q.pollFirst()) != null) {
                p.fail(new RuntimeException("Pool has been shut down"));
            }

            while (!elements.isEmpty()) {
                destroyPoolable(elements.poll()).subscribe();
            }
        }
    }

    @Override
    public boolean isDisposed() {
        return PENDING.get(this) == TERMINATED;
    }

}
