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
package org.flyte.flytekit;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.flyte.api.v1.PartialWorkflowIdentifier;
import org.flyte.api.v1.Variable;
import org.flyte.api.v1.WorkflowNode;

public abstract class SdkWorkflow extends SdkTransform {

  public String getName() {
    return getClass().getName();
  }

  public abstract void expand(SdkWorkflowBuilder builder);

  @Override
  public SdkNode apply(
      SdkWorkflowBuilder builder,
      String nodeId,
      List<String> upstreamNodeIds,
      @Nullable SdkNodeMetadata metadata,
      Map<String, SdkBindingData> inputs) {

    PartialWorkflowIdentifier workflowId =
        PartialWorkflowIdentifier.builder().name(getName()).build();

    SdkWorkflowBuilder innerBuilder = new SdkWorkflowBuilder();
    expand(innerBuilder);

    Map<String, Variable> inputVariableMap = WorkflowTemplateIdl.getInputVariableMap(innerBuilder);
    List<CompilerError> errors = Compiler.validateApply(nodeId, inputs, inputVariableMap);

    if (!errors.isEmpty()) {
      throw new CompilerException(errors);
    }

    WorkflowNode workflowNode =
        WorkflowNode.builder()
            .reference(WorkflowNode.Reference.ofSubWorkflowRef(workflowId))
            .build();

    Map<String, SdkBindingData> outputs =
        innerBuilder.getOutputs().entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    e ->
                        SdkBindingData.ofOutputReference(nodeId, e.getKey(), e.getValue().type())));

    return new SdkWorkflowNode(
        builder, nodeId, upstreamNodeIds, metadata, workflowNode, inputs, outputs);
  }
}
