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

import static org.flyte.flytekit.MoreCollectors.toUnmodifiableMap;

import com.google.auto.value.AutoValue;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.flyte.api.v1.PartialLaunchPlanIdentifier;
import org.flyte.api.v1.WorkflowNode;

/** Reference to a LaunchPlan deployed in flyte, a remote LaunchPlan. */
@AutoValue
public abstract class SdkRemoteLaunchPlan<InputT, OutputT, OutputTransformerT extends OutputTransformer>
    extends SdkTransform<OutputTransformerT> {

  @Nullable
  public abstract String domain();

  public abstract String project();

  public abstract String name();

  @Nullable
  public abstract String version();

  public abstract SdkType<InputT> inputs();

  public abstract SdkType<OutputT> outputs();

  public static <InputT, OutputT, OutputTransformerT extends OutputTransformer>
      SdkRemoteLaunchPlan<InputT, OutputT, OutputTransformerT> create(
          String domain,
          String project,
          String name,
          SdkType<InputT> inputs,
          SdkType<OutputT> outputs) {
    return SdkRemoteLaunchPlan.<InputT, OutputT, OutputTransformerT>builder()
        .domain(domain)
        .project(project)
        .name(name)
        .inputs(inputs)
        .outputs(outputs)
        .build();
  }

  @Override
  public SdkNode<OutputTransformerT> apply(
      SdkWorkflowBuilder builder,
      String nodeId,
      List<String> upstreamNodeIds,
      @Nullable SdkNodeMetadata metadata,
      Map<String, SdkBindingData> inputs) {
    PartialLaunchPlanIdentifier workflowId =
        PartialLaunchPlanIdentifier.builder()
            .name(name())
            .project(project())
            .domain(domain())
            .version(version())
            .build();
    List<CompilerError> errors = Compiler.validateApply(nodeId, inputs, inputs().getVariableMap());

    if (!errors.isEmpty()) {
      throw new CompilerException(errors);
    }

    Map<String, SdkBindingData> outputs =
        outputs().getVariableMap().entrySet().stream()
            .collect(
                toUnmodifiableMap(
                    Map.Entry::getKey,
                    entry ->
                        SdkBindingData.ofOutputReference(
                            nodeId, entry.getKey(), entry.getValue().literalType())));

    return new SdkWorkflowNode<>(
        builder,
        nodeId,
        upstreamNodeIds,
        metadata,
        WorkflowNode.builder()
            .reference(WorkflowNode.Reference.ofLaunchPlanRef(workflowId))
            .build(),
        inputs,
        outputs,
        getOutputTransformerClass());
  }

  public static <InputT, OutputT, OutputTransformerT extends OutputTransformer>
      Builder<InputT, OutputT, OutputTransformerT> builder() {
    return new AutoValue_SdkRemoteLaunchPlan.Builder<>();
  }

  @AutoValue.Builder
  public abstract static class Builder<InputT, OutputT, OutputTransformerT extends OutputTransformer> {

    public abstract Builder<InputT, OutputT, OutputTransformerT> domain(String domain);

    public abstract Builder<InputT, OutputT, OutputTransformerT> project(String project);

    public abstract Builder<InputT, OutputT, OutputTransformerT> name(String name);

    public abstract Builder<InputT, OutputT, OutputTransformerT> version(String version);

    public abstract Builder<InputT, OutputT, OutputTransformerT> inputs(SdkType<InputT> inputs);

    public abstract Builder<InputT, OutputT, OutputTransformerT> outputs(SdkType<OutputT> outputs);

    public abstract SdkRemoteLaunchPlan<InputT, OutputT, OutputTransformerT> build();
  }
}
