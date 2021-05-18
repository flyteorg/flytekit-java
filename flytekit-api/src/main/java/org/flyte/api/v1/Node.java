/*
 * Copyright 2020 Spotify AB.
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

  public abstract String id();

  @Nullable
  public abstract TaskNode taskNode();

  @Nullable
  public abstract BranchNode branchNode();

  @Nullable
  public abstract WorkflowNode workflowNode();

  public abstract List<Binding> inputs();

  public abstract List<String> upstreamNodeIds();

  public abstract Builder toBuilder();

  public static Builder builder() {
    return new AutoValue_Node.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder id(String id);

    public abstract Builder taskNode(TaskNode taskNode);

    public abstract Builder inputs(List<Binding> inputs);

    public abstract Builder upstreamNodeIds(List<String> upstreamNodeIds);

    public abstract Builder branchNode(BranchNode branchNode);

    public abstract Builder workflowNode(WorkflowNode workflowNode);

    public abstract Node build();
  }
}
