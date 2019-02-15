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

import java.time.Duration;
import java.util.function.Predicate;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PoolBuilderTest {

    @Test
    void idlePredicate() {
        Predicate<PooledRef<Object>> predicate = PoolBuilder.idlePredicate(Duration.ofSeconds(3));

        assertThat(predicate).as("clearly out of bounds")
                             .accepts(new TestUtils.TestPooledRef<>("anything", 100, 4, 100))
                             .rejects(new TestUtils.TestPooledRef<>("anything", 100, 2, 100));

        assertThat(predicate).as("ttl is inclusive")
                             .accepts(new TestUtils.TestPooledRef<>("anything", 100, 3, 100));
    }
}