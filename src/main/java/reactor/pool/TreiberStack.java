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

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import reactor.util.annotation.NonNull;
import reactor.util.annotation.Nullable;

/**
 * Implementation of a non-blocking stack using Treiber's algorithm (Treiber, 1986).
 * Source: Java Concurrency in Practice, listing 15.6 (ISBN 0-321-34960-1)
 *
 * @author Brian Goetz
 * @author Tim Peierls
 * @author Simon Baslé
 */
class TreiberStack<T> {

	static <T> TreiberStack<T> empty() {
		return new TreiberStack<T>() {
			@Override
			public boolean push(T item) {
				return false;
			}

			@Override
			public T pop() {
				return null;
			}

			@Override
			public int size() {
				return 0;
			}
		};
	}

	private static class Node<T> {
		final T item;
		Node<T> next;

		Node(T value) {
			this.item = value;
		}
	}

	volatile Node<T> top;
	static final AtomicReferenceFieldUpdater<TreiberStack, Node> TOP = AtomicReferenceFieldUpdater.newUpdater(TreiberStack.class, Node.class, "top");

	volatile int size = 0;

	public boolean push(@NonNull T item) {
		size++;
		Node<T> newHead = new Node<>(item);
		Node<T> oldHead;
		for(;;) {
			oldHead = top;
			newHead.next = oldHead;
			if (TOP.compareAndSet(this, oldHead, newHead)) {
				return true;
			}
		}
	}

	@Nullable
	public T pop() {
		size--;
		Node<T> oldHead;
		Node<T> newHead;
		for(;;) {
			oldHead = top;
			if (oldHead == null) {
				return null;
			}
			newHead = oldHead.next;
			if (TOP.compareAndSet(this, oldHead, newHead)) {
				break;
			}
		}
		return oldHead.item;
	}

	public int size() {
		return size;
	}

}
