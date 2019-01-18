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

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.publisher.Operators;
import reactor.util.annotation.Nullable;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * Common inner {@link Subscription} to be used to deliver poolable elements wrapped in {@link AbstractPooledRef} from
 * an {@link AbstractPool}.
 *
 * @author Simon Baslé
 */
final class Borrower<POOLABLE> implements Scannable, Subscription {

    static final int STATE_INIT = 0;
    static final int STATE_REQUESTED = 1;
    static final int STATE_CANCELLED = 2;

    final CoreSubscriber<? super AbstractPooledRef<POOLABLE>> actual;
    final AbstractPool<POOLABLE> parent;

    volatile int state;
    static final AtomicIntegerFieldUpdater<Borrower> STATE = AtomicIntegerFieldUpdater.newUpdater(Borrower.class, "state");

    Borrower(CoreSubscriber<? super AbstractPooledRef<POOLABLE>> actual, AbstractPool<POOLABLE> parent) {
        this.actual = actual;
        this.parent = parent;
    }

    @Override
    public void request(long n) {
        if (Operators.validate(n) && STATE.compareAndSet(this, STATE_INIT, STATE_REQUESTED)) {
            parent.doAcquire(this);
        }
    }

    @Override
    public void cancel() {
        STATE.getAndSet(this, STATE_CANCELLED);
    }

    @Override
    @Nullable
    public Object scanUnsafe(Attr key) {
        if (key == Attr.PARENT) return parent;
        if (key == Attr.CANCELLED) return state == STATE_CANCELLED;
        if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return state == STATE_REQUESTED ? 1 : 0;
        if (key == Attr.ACTUAL) return actual;

        return null;
    }

    void deliver(AbstractPooledRef<POOLABLE> poolSlot) {
        switch (state) {
            case STATE_REQUESTED:
                poolSlot.acquireIncrement();
                actual.onNext(poolSlot);
                actual.onComplete();
                break;
            case STATE_CANCELLED:
                poolSlot.release().subscribe(aVoid -> {}, actual::onError);
                break;
            default:
                //shouldn't happen since the PoolInner isn't registered with the pool before having requested
                poolSlot.release().subscribe(aVoid -> {}, actual::onError, () -> actual.onError(Exceptions.failWithOverflow()));
        }
    }

    void fail(Throwable error) {
        if (state == STATE_REQUESTED) {
            actual.onError(error);
        }
    }
}