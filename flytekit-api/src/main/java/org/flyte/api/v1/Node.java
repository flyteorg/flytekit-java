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
import java.util.List;
import javax.annotation.Nullable;

/**
 * A Workflow graph Node. One unit of execution in the graph. Each node can be linked to a Task, a
 * Workflow or a BranchNode.
 */
@AutoValue
public abstract class Node {

  public static final String START_NODE_ID = "start-node";

  /**
   * A workflow-level unique identifier that identifies this node in the workflow. 'inputs' and
   * 'outputs' are reserved node ids that cannot be used by other nodes.
   */
  public abstract String id();

  /** Extra metadata about the node. */
  @Nullable
  public abstract NodeMetadata metadata();

  /** Information about the target to execute in this node. */
  @Nullable
  public abstract TaskNode taskNode();

  /** Information about the branch node to evaluate in this node. */
  @Nullable
  public abstract BranchNode branchNode();

  /** Information about the Workflow to execute in this mode. */
  @Nullable
  public abstract WorkflowNode workflowNode();

  /**
   * Specifies how to bind the underlying interface's inputs. All required inputs specified in the
   * underlying interface must be fulfilled. This node will have an implicit dependency on * any
   * node that appears in inputs field.
   */
  public abstract List<Binding> inputs();

  /**
   * [Optional] Specifies execution dependency for this node ensuring it will only get scheduled to
   * run after all its upstream nodes have completed.
   */
  public abstract List<String> upstreamNodeIds();

  // TODO: add outputAliases from src/main/proto/flyteidl/core/workflow.proto

  public abstract Builder toBuilder();

  public static Builder builder() {
    return new AutoValue_Node.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder id(String id);

    public abstract Builder metadata(NodeMetadata metadata);

    public abstract Builder taskNode(TaskNode taskNode);

    public abstract Builder branchNode(BranchNode branchNode);

    public abstract Builder workflowNode(WorkflowNode workflowNode);

    public abstract Builder inputs(List<Binding> inputs);

    public abstract Builder upstreamNodeIds(List<String> upstreamNodeIds);

    public abstract Node build();
  }
}
