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

import com.google.auto.value.AutoValue;
import java.util.List;
import java.util.Map;

@AutoValue
public abstract class DynamicJobSpec {

  /**
   * A collection of nodes to execute.
   *
   * @return nodes
   */
  public abstract List<Node> nodes();

  /**
   * Describes how to bind the final output of the dynamic job from the outputs of executed nodes.
   * The referenced ids in bindings should have the generated id for the subtask.
   *
   * @return outputs
   */
  public abstract List<Binding> outputs();

  /** @return sub-workflows */
  public abstract Map<WorkflowIdentifier, WorkflowTemplate> subWorkflows();

  /** @return tasks */
  public abstract Map<TaskIdentifier, TaskTemplate> tasks();

  public static Builder builder() {
    return new AutoValue_DynamicJobSpec.Builder();
  }

  public abstract Builder toBuilder();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder nodes(List<Node> nodes);

    public abstract Builder outputs(List<Binding> outputs);

    public abstract Builder subWorkflows(Map<WorkflowIdentifier, WorkflowTemplate> subWorkflows);

    public abstract Builder tasks(Map<TaskIdentifier, TaskTemplate> tasks);

    public abstract DynamicJobSpec build();
  }
}
