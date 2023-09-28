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

/**
 * A Flyte workflow. A workflow is made of nodes conforming a DAG. The nodes could refer to tasks,
 * branches or other workflows.
 *
 * @param <InputT> input type of the workflow
 * @param <OutputT> output type of the workflow
 */
public abstract class SdkWorkflow<InputT, OutputT> extends SdkTransform<InputT, OutputT> {
  private final SdkType<InputT> inputType;
  private final SdkType<OutputT> outputType;

  /**
   * Called by subclasses passing the {@link SdkType}s for inputs and outputs.
   *
   * @param inputType type for inputs.
   * @param outputType type for outputs.
   */
  protected SdkWorkflow(SdkType<InputT> inputType, SdkType<OutputT> outputType) {
    this.inputType = inputType;
    this.outputType = outputType;
  }

  /**
   * Expands the workflow into the builder.
   *
   * @param builder workflow builder that this workflow expands into.
   * @param input workflow input.
   * @return the workflow output.
   */
  protected abstract OutputT expand(SdkWorkflowBuilder builder, InputT input);

  /**
   * Expands the workflow into the builder.
   *
   * @param builder workflow builder that this workflow expands into. \
   */
  public final void expand(SdkWorkflowBuilder builder) {
    var literalTypes = inputType.toLiteralTypes();
    inputType
        .getVariableMap()
        .forEach(
            (name, variable) ->
                builder.setInput(
                    literalTypes.get(name),
                    name,
                    variable.description() == null ? "" : variable.description()));

    OutputT output = expand(builder, inputType.promiseFor(START_NODE_ID));

    Map<String, Variable> outputVariableMap = outputType.getVariableMap();
    outputType
        .toSdkBindingMap(output)
        .forEach(
            (name, variable) ->
                builder.output(
                    name,
                    variable,
                    outputVariableMap.get(name).description() == null
                        ? ""
                        : outputVariableMap.get(name).description()));
  }

  /** {@inheritDoc} */
  @Override
  public SdkNode<OutputT> apply(
      SdkWorkflowBuilder builder,
      String nodeId,
      List<String> upstreamNodeIds,
      @Nullable SdkNodeMetadata metadata,
      Map<String, SdkBindingData<?>> inputs) {

    var workflowId = PartialWorkflowIdentifier.builder().name(getName()).build();

    var innerBuilder = new SdkWorkflowBuilder();
    expand(innerBuilder);

    var inputVariableMap = WorkflowTemplateIdl.getInputVariableMap(innerBuilder);
    var errors = Compiler.validateApply(nodeId, inputs, inputVariableMap);

    if (!errors.isEmpty()) {
      throw new CompilerException(errors);
    }

    var workflowNode =
        WorkflowNode.builder()
            .reference(WorkflowNode.Reference.ofSubWorkflowRef(workflowId))
            .build();

    Map<String, SdkBindingData<?>> outputs =
        innerBuilder.getOutputs().entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    e -> SdkBindingData.promise(e.getValue().type(), nodeId, e.getKey())));

    var promise = getOutputType().promiseFor(nodeId);
    return new SdkWorkflowNode<>(
        builder, nodeId, upstreamNodeIds, metadata, workflowNode, inputs, outputs, promise);
  }

  /** {@inheritDoc} */
  @Override
  public SdkType<InputT> getInputType() {
    return inputType;
  }

  /** {@inheritDoc} */
  @Override
  public SdkType<OutputT> getOutputType() {
    return outputType;
  }

  /** Returns the {@link WorkflowTemplate} for this workflow. */
  public WorkflowTemplate toIdlTemplate() {
    SdkWorkflowBuilder builder = new SdkWorkflowBuilder();
    this.expand(builder);

    return builder.toIdlTemplate();
  }
}
