/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
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

package reactor.util.pool;

import reactor.core.publisher.Mono;

/**
 * @author Simon Baslé
 */
public interface Pool<POOLABLE> {

    /**
     * Borrow a {@code POOLABLE} from the pool upon subscription. The resulting {@link Mono} emits the {@code POOLABLE}
     * as it becomes available. Cancelling the {@link org.reactivestreams.Subscription} before the {@code POOLABLE} has
     * been emitted will either avoid object borrowing entirely or will result in immediate {@link PoolConfig#cleaner()} release}
     * of the {@code POOLABLE}.
     *
     * @return a {@link Mono}, each subscription to which represents the act of borrowing a pooled object
     */
    Mono<POOLABLE> borrow();

    /**
     * Return a {@link Mono} that, once subscribed, will release the {@code POOLABLE} back to the pool asynchronously.
     * This method can be used if it is needed to wait for the recycling of the POOLABLE, but users should usually call
     * {@link #release(Object)}.
     *
     * @param poolable the {@code POOLABLE} to be released back to the pool
     * @return a {@link Mono} that will complete empty when the object has been released. In case of an error the object
     * is always discarded.
     */
    Mono<Void> releaseMono(POOLABLE poolable);

    /**
     * Trigger the <strong>asynchronous</strong> release of the {@code POOLABLE} back to the pool and
     * <strong>immediately return</strong>. The underlying {@link #releaseMono(Object)} is subscribed to but no blocking
     * is performed to wait for it to signal completion. Note however that in case of a releasing error the object is
     * always discarded.
     * <p>
     * When releasing, a borrowing party usually doesn't care that the release completed, which will have more impact on
     * <strong>pending</strong> borrowers.
     *
     * @param poolable the {@code POOLABLE} to be released back to the pool
     */
    void release(POOLABLE poolable);


}
