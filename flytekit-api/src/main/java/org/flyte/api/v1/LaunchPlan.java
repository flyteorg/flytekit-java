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

import com.google.auto.value.AutoValue;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;

/** User-provided launch plan definition and configuration values. */
@AutoValue
public abstract class LaunchPlan {
  public abstract String name();
  /** Reference to the Workflow template that the launch plan references. */
  public abstract PartialWorkflowIdentifier workflowId();

  /**
   * Fixed, non-overridable inputs for the Launch Plan. These can not be overriden when an execution
   * is created with this launch plan.
   */
  public abstract Map<String, Literal> fixedInputs();

  /**
   * Input values to be passed for the execution. These can be overriden when an execution is
   * created with this launch plan.
   */
  public abstract Map<String, Parameter> defaultInputs();

  /**
   * Controls the maximum number of tasknodes that can be run in parallel for the entire workflow
   */
  public abstract Optional<Integer> maxParallelism();

  @Nullable
  public abstract CronSchedule cronSchedule();

  // TODO: check if this should follow launch_plan.proto definitions.

  public static Builder builder() {
    return new AutoValue_LaunchPlan.Builder()
        .fixedInputs(Collections.emptyMap())
        .defaultInputs(Collections.emptyMap());
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder name(String name);

    public abstract Builder workflowId(PartialWorkflowIdentifier workflowId);

    public abstract Builder fixedInputs(Map<String, Literal> fixedInputs);

    public abstract Builder defaultInputs(Map<String, Parameter> defaultInputs);

    public abstract Builder cronSchedule(CronSchedule cronSchedule);

    public abstract Builder maxParallelism(Optional<Integer> maxParallelism);

    public abstract LaunchPlan build();
  }
}
