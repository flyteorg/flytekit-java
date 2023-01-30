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

import static org.flyte.api.v1.Node.START_NODE_ID;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.flyte.api.v1.PartialWorkflowIdentifier;
import org.flyte.api.v1.Variable;
import org.flyte.api.v1.WorkflowNode;
import org.flyte.api.v1.WorkflowTemplate;

public abstract class SdkWorkflow<InputT, OutputT> extends SdkTransform<InputT, OutputT> {
  private final SdkType<InputT> inputType;
  private final SdkType<OutputT> outputType;
  private final InputT inputPromise;

  protected SdkWorkflow(SdkType<InputT> inputType, SdkType<OutputT> outputType) {
    this.inputType = inputType;
    this.outputType = outputType;
    this.inputPromise = getInputType().promiseFor(START_NODE_ID);
  }

  public abstract OutputT expand(SdkWorkflowBuilder builder, InputT input);

  @Override
  public SdkNode<OutputT> apply(
      SdkWorkflowBuilder builder,
      String nodeId,
      List<String> upstreamNodeIds,
      @Nullable SdkNodeMetadata metadata,
      Map<String, SdkBindingData<?>> inputs) {

    PartialWorkflowIdentifier workflowId =
        PartialWorkflowIdentifier.builder().name(getName()).build();

    SdkWorkflowBuilder innerBuilder = new SdkWorkflowBuilder();

    OutputT output = expand(innerBuilder, inputPromise);

    Map<String, Variable> inputVariableMap = getInputType().getVariableMap();

    List<CompilerError> errors = Compiler.validateApply(nodeId, inputs, inputVariableMap);

    if (!errors.isEmpty()) {
      throw new CompilerException(errors);
    }

    WorkflowNode workflowNode =
        WorkflowNode.builder()
            .reference(WorkflowNode.Reference.ofSubWorkflowRef(workflowId))
            .build();

    Map<String, SdkBindingData<?>> outputs =
        getOutputType().toSdkBindingMap(output).entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    e ->
                        SdkBindingData.ofOutputReference(nodeId, e.getKey(), e.getValue().type())));

    OutputT promise = getOutputType().promiseFor(nodeId);

    return new SdkWorkflowNode<>(
        builder, nodeId, upstreamNodeIds, metadata, workflowNode, inputs, outputs, promise);
  }

  @Override
  public SdkType<InputT> getInputType() {
    return inputType;
  }

  @Override
  public SdkType<OutputT> getOutputType() {
    return outputType;
  }

  protected Map<String, SdkBindingData<?>> getInputSdkBindingMap() {
    return getInputType().toSdkBindingMap(inputPromise);
  }

  protected InputT getInputPromise() {
    return inputPromise;
  }

  public WorkflowTemplate expandAndConvertToIdlTemplate() {
    SdkWorkflowBuilder builder = new SdkWorkflowBuilder();
    OutputT output = this.expand(builder, inputPromise);
    return toIdlTemplate(builder, inputPromise, output);
  }

  public WorkflowTemplate toIdlTemplate(
      SdkWorkflowBuilder builder, InputT inputs, OutputT outputs) {
    return WorkflowTemplateIdl.ofBuilder(
        builder, getInputType().toSdkBindingMap(inputs), getOutputType().toSdkBindingMap(outputs));
  }
}
