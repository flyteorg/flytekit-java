/*
 * Copyright 2020-2023 Flyte Authors
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

/** Encapsulation of fields that uniquely identifies a launch plan. */
@AutoValue
public abstract class LaunchPlanIdentifier implements Identifier {

  public static Builder builder() {
    return new AutoValue_LaunchPlanIdentifier.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    /**
     * Name of the domain the launch plan belongs to. A domain can be considered as a subset within
     * a specific project.
     */
    public abstract Builder domain(String domain);

    /** Name of the project the launch plan belongs to. */
    public abstract Builder project(String project);

    /** User provided value for the launch plan. */
    public abstract Builder name(String name);

    /** Specific version of the launch plan. */
    public abstract Builder version(String version);

    public abstract LaunchPlanIdentifier build();
  }
}
