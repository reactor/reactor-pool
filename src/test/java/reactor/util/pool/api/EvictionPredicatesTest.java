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

package reactor.util.pool.api;

import org.junit.jupiter.api.Test;
import reactor.util.pool.TestUtils;

import java.time.Duration;
import java.util.Objects;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;

class EvictionPredicatesTest {

    @Test
    void agedMoreThan() {
        Predicate<PooledRef<Object>> predicate = EvictionPredicates.agedMoreThan(Duration.ofSeconds(3));

        assertThat(predicate).as("clearly out of bounds")
                .accepts(new TestUtils.TestPooledRef<>("anything", -1, -1, 4))
                .rejects(new TestUtils.TestPooledRef<>("anything", -1, -1, 2));

        assertThat(predicate).as("ttl is inclusive")
                .accepts(new TestUtils.TestPooledRef<>("anything", -1, -1, 3));
    }

    @Test
    void acquiredMoreThan() {
        Predicate<PooledRef<Object>> predicate = EvictionPredicates.acquiredMoreThan(3);

        assertThat(predicate).as("clearly out of bounds")
                .accepts(new TestUtils.TestPooledRef<>("anything", 4, -1, -1))
                .rejects(new TestUtils.TestPooledRef<>("anything", 2, -1, -1));

        assertThat(predicate).as("acquireCount is inclusive")
                .accepts(new TestUtils.TestPooledRef<>("anything", 3, -1, -1));
    }

    @Test
    void poolableMatchesObjectPredicate() {
        Predicate<PooledRef<String>> predicate = EvictionPredicates.poolableMatches(Objects::isNull);

        assertThat(predicate)
                .accepts(new TestUtils.TestPooledRef<>(null, -1, -1, -1))
                .rejects(new TestUtils.TestPooledRef<>("anything", -1, -1, -1));
    }

    @Test
    void poolableMatchesStringPredicate() {
        Predicate<PooledRef<String>> predicate = EvictionPredicates.poolableMatches(str -> str.length() > 3);

        assertThat(predicate)
                .accepts(new TestUtils.TestPooledRef<>("fooz", -1, -1, -1))
                .rejects(new TestUtils.TestPooledRef<>("foo", -1, -1, -1));
    }

    @Test
    void combineRefPredicateAndPoolablePredicate() {
        Predicate<PooledRef<String>> predicate = EvictionPredicates.poolableMatches(str -> str.length() > 3);
        predicate = predicate.and(EvictionPredicates.acquiredMoreThan(100));

        assertThat(predicate)
                .accepts(new TestUtils.TestPooledRef<>("foobar", 100, -1,-1))
                .rejects(new TestUtils.TestPooledRef<>("foo", 100, -1,-1),
                        new TestUtils.TestPooledRef<>("foobar", 99, -1,-1),
                        new TestUtils.TestPooledRef<>("foo", 99, -1, -1));
    }

    @Test
    void combineRefPredicateOrPoolablePredicate() {
        Predicate<PooledRef<String>> predicate = EvictionPredicates.poolableMatches(str -> str.length() > 3);
        predicate = predicate.or(EvictionPredicates.acquiredMoreThan(100));

        assertThat(predicate)
                .accepts(new TestUtils.TestPooledRef<>("foobar", 100, -1,-1),
                        new TestUtils.TestPooledRef<>("foo", 100, -1,-1),
                        new TestUtils.TestPooledRef<>("foobar", 99, -1,-1))
                .rejects(new TestUtils.TestPooledRef<>("foo", 99, -1, -1));
    }

    @Test
    void combinePoolablePredicateAndRefPredicate() {
        Predicate<PooledRef<String>> predicate = EvictionPredicates.acquiredMoreThan(100);
        predicate = predicate.and(EvictionPredicates.poolableMatches(str -> str.length() > 3));

        assertThat(predicate)
                .accepts(new TestUtils.TestPooledRef<>("foobar", 100, -1,-1))
                .rejects(new TestUtils.TestPooledRef<>("foo", 100, -1,-1),
                        new TestUtils.TestPooledRef<>("foobar", 99, -1,-1),
                        new TestUtils.TestPooledRef<>("foo", 99, -1, -1));
    }

    @Test
    void combinePoolablePredicateOrRefPredicate() {
        Predicate<PooledRef<String>> predicate = EvictionPredicates.acquiredMoreThan(100);
        predicate = predicate.or(EvictionPredicates.poolableMatches(str -> str.length() > 3));


        assertThat(predicate)
                .accepts(new TestUtils.TestPooledRef<>("foobar", 100, -1,-1),
                        new TestUtils.TestPooledRef<>("foo", 100, -1,-1),
                        new TestUtils.TestPooledRef<>("foobar", 99, -1,-1))
                .rejects(new TestUtils.TestPooledRef<>("foo", 99, -1, -1));
    }
}