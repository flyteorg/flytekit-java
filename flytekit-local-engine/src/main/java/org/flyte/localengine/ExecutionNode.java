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
package org.flyte.localengine;

import com.google.auto.value.AutoValue;
import java.util.List;
import javax.annotation.Nullable;
import org.flyte.api.v1.Binding;
import org.flyte.api.v1.RunnableNode;
import org.flyte.api.v1.WorkflowTemplate;

/** An execution node is a compiled node, ready to be executed */
@AutoValue
public abstract class ExecutionNode {

  /** Node identifier used to uniquely identify the execution node */
  public abstract String nodeId();

  /** Dependent node ids for this execution node */
  public abstract List<String> upstreamNodeIds();

  /** [Optional] Input bindings for this node */
  @Nullable
  public abstract List<Binding> bindings();

  /**
   * [Optional] Inner runnable node that will be used to run this execution node, if not null. An
   * ExecutionNode must have one of the following:
   *
   * <ul>
   *   <li>{@link RunnableNode} runnableNode()
   *   <li>{@link WorkflowTemplate} subWorkflow()
   *   <li>{@link ExecutionBranchNode} branchNode()
   * </ul>
   */
  @Nullable
  public abstract RunnableNode runnableNode();

  /**
   * [Optional] Inner subworkflow that will be used to run this execution node, if not null. An
   * ExecutionNode must have one of the following:
   *
   * <ul>
   *   <li>{@link RunnableNode} runnableNode()
   *   <li>{@link WorkflowTemplate} subWorkflow()
   *   <li>{@link ExecutionBranchNode} branchNode()
   * </ul>
   */
  @Nullable
  public abstract WorkflowTemplate subWorkflow();

  /**
   * [Optional] Inner branch node that will be used to run this execution node, if not null. An
   * ExecutionNode must have one of the following:
   *
   * <ul>
   *   <li>{@link RunnableNode} runnableNode()
   *   <li>{@link WorkflowTemplate} subWorkflow()
   *   <li>{@link ExecutionBranchNode} branchNode()
   * </ul>
   */
  @Nullable
  public abstract ExecutionBranchNode branchNode();

  /** Maximum number of retries to execute this node */
  public abstract int attempts();

  static Builder builder() {
    return new AutoValue_ExecutionNode.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    abstract Builder nodeId(String nodeId);

    abstract Builder upstreamNodeIds(List<String> upstreamNodeIds);

    abstract Builder bindings(List<Binding> bindings);

    abstract Builder runnableNode(RunnableNode runnableNode);

    abstract Builder branchNode(ExecutionBranchNode branchNode);

    abstract Builder subWorkflow(WorkflowTemplate subWorkflow);

    abstract Builder attempts(int attempts);

    abstract ExecutionNode build();
  }
}
