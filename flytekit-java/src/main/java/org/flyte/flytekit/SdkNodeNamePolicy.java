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

/**
 * Controls the default node id and node name policy when applying {@link SdkTransform} to {@link
 * SdkWorkflowBuilder}. When using {@link SdkWorkflowBuilder#apply(SdkTransform)} or {@link
 * SdkWorkflowBuilder#apply(SdkTransform, Object)} then the node id used would be the one returned
 * by {@link #nextNodeId()}. Also, if a node name haven't been set by the user, then {@link
 * #toNodeName(String)} would be used.
 */
class SdkNodeNamePolicy {
  private static final Pattern UPPER_AFTER_LOWER_PATTERN = Pattern.compile("([a-z])([A-Z]+)");
  private static final int RND_PREFIX_SIZE = 4;

  private final String nodeIdPrefix;
  private final AtomicInteger nodeIdSuffix;

  SdkNodeNamePolicy() {
    this.nodeIdPrefix = randomPrefix();
    this.nodeIdSuffix = new AtomicInteger();
  }

  /**
   * Returns a unique node ids in the format {@code <prefix>-n<consecutive-number>}, where prefix is
   * a random, but shared among all ids for this object, set of character in the format {@code
   * wRRRR} and {@code R} is a random letter in {@code a-z} range.
   *
   * @return next unique node id for this policy.
   */
  String nextNodeId() {
    return nodeIdPrefix + "n" + nodeIdSuffix.getAndIncrement();
  }

  /**
   * Returns a node appropriate name for a given transformation name. The transformation done are
   *
   * <ul>
   *   <li>Package name is removed
   *   <li>CamelCase is transformed to kebab-case
   *   <li>$ is transformed to -
   * </ul>
   *
   * <p>For example {@code com.example.Outer$InnerTask} get translated to {@code outer-inner-task}.
   *
   * @return node name.
   */
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
