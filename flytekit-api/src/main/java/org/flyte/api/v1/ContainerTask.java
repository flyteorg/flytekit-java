/*
 * Copyright 2020-2023 Flyte Authors.
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

import java.util.List;

/** Building block for tasks that execute arbitrary containers. */
public interface ContainerTask extends Task {

  /** Specifies container image. */
  String getImage();

  /** Specifies command line arguments for container command. */
  List<String> getArgs();

  /** Specifies container command. Example: {@code List.of("java", "-Xmx1G")}. */
  List<String> getCommand();

  /** Specifies container environment variables. */
  List<KeyValuePair> getEnv();

  @Override
  default String getType() {
    return "raw-container";
  }

  /** Specifies container resource requests. */
  default Resources getResources() {
    return Resources.builder().build();
  }
}
