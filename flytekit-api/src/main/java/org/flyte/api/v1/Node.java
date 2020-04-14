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

/**
 * A Workflow graph Node. One unit of execution in the graph. Each node can be linked to a Task, a
 * Workflow or a BranchNode.
 */
@AutoValue
public abstract class Node {

  public abstract String id();

  public abstract TaskNode taskNode();

  public abstract List<Binding> inputs();

  public static Node create(String id, TaskNode taskNode, List<Binding> inputs) {
    return new AutoValue_Node(id, taskNode, inputs);
  }
}
