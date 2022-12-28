/*
 * Copyright 2021 Flyte Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.flyte.flytekit;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.matchesRegex;
import static org.hamcrest.Matchers.startsWith;

import java.util.Set;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class NodeNamePolicyTest {

  @Test
  void nextNodeIdShouldConformToFormat() {
    assertThat(new NodeNamePolicy().nextNodeId(), matchesRegex("w[a-z]{4}-n\\d+"));
  }

  @Test
  void nextNodeIdShouldReturnUniqueIds() {
    NodeNamePolicy policy = new NodeNamePolicy();

    var ids = IntStream.range(0, 100).mapToObj(__ -> policy.nextNodeId()).collect(toList());
    var uniqueIds = Set.copyOf(ids);

    assertThat("returned duplicated id", ids, hasSize(uniqueIds.size()));
  }

  @Test
  void nextNodeIdShouldReturnSamePrefixForSamePolicy() {
    NodeNamePolicy policy = new NodeNamePolicy();

    var ids = IntStream.range(0, 100).mapToObj(__ -> policy.nextNodeId()).collect(toList());

    String firstId = ids.get(0);
    String commonPrefix = firstId.substring(0, firstId.indexOf('-'));
    assertThat(ids, everyItem(startsWith(commonPrefix)));
  }

  @Test
  void nextNodeIdShouldReturnUniquePrefixForDistinctPolicies() {
    var prefixes =
        IntStream.range(0, 100)
            .mapToObj(__ -> new NodeNamePolicy())
            .map(NodeNamePolicy::nextNodeId)
            .map(id -> id.substring(0, id.indexOf('-')))
            .collect(toList());
    var uniquePrefixes = Set.copyOf(prefixes);

    assertThat("returned duplicated prefix", prefixes, hasSize(uniquePrefixes.size()));
  }

  @ParameterizedTest
  @CsvSource({
    "Foo,foo",
    "FooBar,foo-bar",
    "FooBarBaz,foo-bar-baz",
    "Foo$Bar,foo-bar",
    "com.example.FooBar,foo-bar"
  })
  void toNodeName(String taskName, String expectedNodeName) {
    NodeNamePolicy policy = new NodeNamePolicy();

    assertThat(policy.toNodeName(taskName), equalTo(expectedNodeName));
  }
}
