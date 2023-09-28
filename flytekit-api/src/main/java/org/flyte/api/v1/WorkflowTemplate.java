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
import java.util.List;

/**
 * Structure that encapsulates task, branch and subworkflow nodes to form a statically analyzable,
 * directed acyclic graph.
 */
@AutoValue
public abstract class WorkflowTemplate {

  /**
   * A list of nodes. In addition, 'globals' is a special reserved node id that can be used to
   * consume workflow inputs.
   */
  public abstract List<Node> nodes();
  /** Extra metadata about the workflow. */
  public abstract WorkflowMetadata metadata();

  public static Builder builder() {
    return new AutoValue_WorkflowTemplate.Builder();
  }

  /**
   * Defines a strongly typed interface for the Workflow. This can include some optional parameters.
   */
  public abstract TypedInterface interface_();

  /**
   * A list of output bindings that specify how to construct workflow outputs. Bindings can pull
   * node outputs or specify literals. All workflow outputs specified in the interface field must be
   * bound in order for the workflow to be validated. A workflow has an implicit dependency on all
   * of its nodes to execute successfully in order to bind final outputs. Most of these outputs will
   * be Binding's with a BindingData of type OutputReference. That is, your workflow can just have
   * an output of some constant (`Output(5)`), but usually, the workflow will be pulling outputs
   * from the output of a task.
   */
  public abstract List<Binding> outputs();

  // TODO: add failure_node and metadata_defaults from src/main/proto/flyteidl/core/workflow.proto

  public abstract Builder toBuilder();

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder nodes(List<Node> nodes);

    public abstract Builder metadata(WorkflowMetadata metadata);

    public abstract Builder interface_(TypedInterface interface_);

    public abstract Builder outputs(List<Binding> outputs);

    public abstract WorkflowTemplate build();
  }
}
