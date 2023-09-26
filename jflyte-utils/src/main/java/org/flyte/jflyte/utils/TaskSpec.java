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
package org.flyte.jflyte.utils;

import com.google.auto.value.AutoValue;
import org.flyte.api.v1.TaskTemplate;

/** Class for Task specification. */
@AutoValue
public abstract class TaskSpec {

  public abstract TaskTemplate taskTemplate();

  static TaskSpec create(TaskTemplate taskTemplate) {
    return new AutoValue_TaskSpec(taskTemplate);
  }
}
