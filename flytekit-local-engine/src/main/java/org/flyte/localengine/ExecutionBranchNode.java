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
import org.flyte.api.v1.NodeError;

/**
 * BranchNode is a special node that alters the flow of the workflow graph. It allows the control
 * flow to branch at runtime based on a series of conditions that get evaluated on various
 * parameters (e.g. inputs, primitives).
 */
@AutoValue
abstract class ExecutionBranchNode {
  abstract List<ExecutionIfBlock> ifNodes();

  @Nullable
  abstract ExecutionNode elseNode();

  @Nullable
  abstract NodeError error();

  public static Builder builder() {
    return new AutoValue_ExecutionBranchNode.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    /** Indicates the {@link ExecutionIfBlock} nodes as if conditions to build this branch node */
    public abstract Builder ifNodes(List<ExecutionIfBlock> ifNodes);

    /** Indicates the node to execute in case the if conditions are not satisfied */
    public abstract Builder elseNode(ExecutionNode elseNode);

    /** Indicates the error node to throw in case none of the branches were taken. */
    public abstract Builder error(NodeError error);

    abstract ExecutionBranchNode autoBuild();

    public final ExecutionBranchNode build() {
      ExecutionBranchNode node = autoBuild();

      if (node.ifNodes().isEmpty()) {
        throw new IllegalStateException("There must be at least one if-then node");
      }

      if ((node.elseNode() == null) == (node.error() == null)) {
        throw new IllegalStateException(
            "Must specify either elseNode or errorNode, both cannot be null nor but cannot be specified");
      }
      return node;
    }
  }
}
