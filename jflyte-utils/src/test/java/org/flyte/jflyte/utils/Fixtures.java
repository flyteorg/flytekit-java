/*
 * Copyright 2023 Flyte Authors.
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
package org.flyte.jflyte.utils;

import static java.util.Collections.emptyMap;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.flyte.api.v1.Container;
import org.flyte.api.v1.KeyValuePair;
import org.flyte.api.v1.RetryStrategy;
import org.flyte.api.v1.SimpleType;
import org.flyte.api.v1.Struct;
import org.flyte.api.v1.TaskTemplate;
import org.flyte.api.v1.TypedInterface;

final class Fixtures {
  static final String IMAGE_NAME = "alpine:latest";
  static final String COMMAND = "date";

  static final Container CONTAINER =
      Container.builder()
          .command(ImmutableList.of(COMMAND))
          .args(ImmutableList.of())
          .image(IMAGE_NAME)
          .env(ImmutableList.of(KeyValuePair.of("key", "value")))
          .build();
  static final TypedInterface INTERFACE_ =
      TypedInterface.builder()
          .inputs(ImmutableMap.of("x", ApiUtils.createVar(SimpleType.STRING)))
          .outputs(ImmutableMap.of("y", ApiUtils.createVar(SimpleType.INTEGER)))
          .build();
  static final RetryStrategy RETRIES = RetryStrategy.builder().retries(4).build();
  static final TaskTemplate TASK_TEMPLATE =
      TaskTemplate.builder()
          .container(CONTAINER)
          .type("custom-task")
          .interface_(INTERFACE_)
          .custom(Struct.of(emptyMap()))
          .retries(RETRIES)
          .discoverable(false)
          .cacheSerializable(false)
          .isSyncPlugin(false)
          .build();

  private Fixtures() {
    throw new UnsupportedOperationException();
  }
}
