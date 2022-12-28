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

import java.util.Locale;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

class NodeNamePolicy {
  private static final Pattern UPPER_AFTER_LOWER_PATTERN = Pattern.compile("([a-z])([A-Z]+)");
  private static final int RND_PREFIX_SIZE = 4;

  private final String nodeIdPrefix;
  private final AtomicInteger nodeIdSuffix;

  NodeNamePolicy() {
    this.nodeIdPrefix = randomPrefix();
    this.nodeIdSuffix = new AtomicInteger();
  }

  // Returns nodes in the format "wqjoz-n1"
  String nextNodeId() {
    return nodeIdPrefix + "n" + nodeIdSuffix.getAndIncrement();
  }

  // Translates "FooBar" to "foo-bar"
  String toNodeName(String name) {
    String lastPart = name.substring(name.lastIndexOf('.') + 1);
    return UPPER_AFTER_LOWER_PATTERN
        .matcher(lastPart)
        .replaceAll("$1-$2")
        .toLowerCase(Locale.ROOT)
        .replaceAll("\\$", "-");
  }

  // Returns random prefix in the following format "wqjoz-"
  private static String randomPrefix() {
    return "w"
        + ThreadLocalRandom.current()
            .ints(RND_PREFIX_SIZE, 'a', 'z' + 1)
            .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
            .append('-');
  }
}
