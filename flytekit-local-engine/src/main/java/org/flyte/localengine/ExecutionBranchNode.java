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

/**
 * BranchNode is a special node that alter the flow of the workflow graph. It allows the control
 * flow to branch at runtime based on a series of conditions that get evaluated on various
 * parameters (e.g. inputs, primitives).
 */
@AutoValue
abstract class ExecutionBranchNode {
  abstract List<ExecutionIfBlock> ifNodes();
  @Nullable
  abstract ExecutionNode elseNode();

  //XXX support node error

  public static ExecutionBranchNode create(List<ExecutionIfBlock> ifNodes, ExecutionNode elseNode) {
    return new AutoValue_ExecutionBranchNode(ifNodes, elseNode);
  }
}
