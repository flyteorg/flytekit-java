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
package org.flyte.api.v1;

import static java.util.Collections.emptyMap;

import java.util.List;
import java.util.Locale;

/** Building block for tasks that execute arbitrary containers. */
public interface ContainerTask {

  /** Specifies task name. */
  String getName();

  /** Specifies container image. */
  String getImage();

  /** Specifies command line arguments for container command. */
  List<String> getArgs();

  /** Specifies container command. Example: {@code List.of("java", "-Xmx1G")}. */
  List<String> getCommand();

  /** Specifies container environment variables. */
  List<KeyValuePair> getEnv();

  default String getType() {
    return "raw-container".toLowerCase(Locale.ROOT);
  }

  TypedInterface getInterface();

  /** Specifies container resource requests. */
  default Resources getResources() {
    return Resources.builder().build();
  }

  /** Specifies task retry policy. */
  RetryStrategy getRetries();

  /** Specifies custom container parameters. */
  default Struct getCustom() {
    return Struct.of(emptyMap());
  }
}
