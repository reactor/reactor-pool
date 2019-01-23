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
import reactor.core.Scannable;
import reactor.core.publisher.Operators;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Common inner {@link Subscription} to be used to deliver poolable elements wrapped in {@link AbstractPooledRef} from
 * an {@link AbstractPool}.
 *
 * @author Simon Baslé
 */
final class Borrower<POOLABLE> extends AtomicBoolean implements Scannable, Subscription  {

    final CoreSubscriber<? super AbstractPooledRef<POOLABLE>> actual;

    Borrower(CoreSubscriber<? super AbstractPooledRef<POOLABLE>> actual) {
        this.actual = actual;
    }

    @Override
    public void request(long n) {
        //IGNORED, a Borrower is always considered requested upon subscription
    }

    @Override
    public void cancel() {
        set(true);
    }

    @Override
    @Nullable
    public Object scanUnsafe(Attr key) {
        if (key == Attr.CANCELLED) return get();
        if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return 1;
        if (key == Attr.ACTUAL) return actual;

        return null;
    }

    void deliver(AbstractPooledRef<POOLABLE> poolSlot) {
        if (get()) {
            //CANCELLED
            poolSlot.release().subscribe(aVoid -> {}, e -> Operators.onErrorDropped(e, Context.empty())); //actual mustn't receive onError
        }
        else {
            poolSlot.markAcquired();
            actual.onNext(poolSlot);
            actual.onComplete();
        }
    }

    void fail(Throwable error) {
        if (!get()) {
            actual.onError(error);
        }
    }

    @Override
    public String toString() {
        return get() ? "Borrower(cancelled)" : "Borrower";
    }
}