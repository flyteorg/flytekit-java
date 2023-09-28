/*
 * Copyright 2020-2021 Flyte Authors
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

import com.google.auto.value.AutoValue;

/** Encapsulation of fields that uniquely identifies a task. */
@AutoValue
public abstract class TaskIdentifier implements Identifier {

  public static Builder builder() {
    return new AutoValue_TaskIdentifier.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder domain(String domain);

    public abstract Builder project(String project);

    public abstract Builder name(String name);

    public abstract Builder version(String version);

    public abstract TaskIdentifier build();
  }
}
