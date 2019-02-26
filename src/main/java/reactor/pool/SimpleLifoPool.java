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

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * This implementation is based on MPSC queues for idle resources and a {@link TreiberStack}
 * for pending {@link Pool#acquire()} Monos, resulting in serving pending borrowers in LIFO order.
 *
 * See {@link SimplePool} for other characteristics of the simple pool.
 *
 * @author Simon Baslé
 */
final class SimpleLifoPool<POOLABLE> extends SimplePool<POOLABLE> {

    private static final TreiberStack TERMINATED = TreiberStack.empty();

    volatile TreiberStack<Borrower<POOLABLE>>                                      pending;
    private static final AtomicReferenceFieldUpdater<SimpleLifoPool, TreiberStack> PENDING = AtomicReferenceFieldUpdater.newUpdater(
            SimpleLifoPool.class, TreiberStack.class, "pending");

    public SimpleLifoPool(DefaultPoolConfig<POOLABLE> poolConfig) {
        super(poolConfig);
        this.pending = new TreiberStack<>(); //unbounded
    }

    @Override
    int pendingSize() {
        return PENDING.get(this).size();
    }

    @Override
    boolean pendingOffer(Borrower<POOLABLE> pending) {
        return this.pending.push(pending);
    }

    @Override
    Borrower<POOLABLE> pendingPoll() {
        return this.pending.pop();
    }

    @Override
    public void dispose() {
        @SuppressWarnings("unchecked")
        TreiberStack<Borrower<POOLABLE>> q = PENDING.getAndSet(this, TERMINATED);
        if (q != TERMINATED) {
            Borrower<POOLABLE> p;
            while((p = q.pop()) != null) {
                p.fail(new RuntimeException("Pool has been shut down"));
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
