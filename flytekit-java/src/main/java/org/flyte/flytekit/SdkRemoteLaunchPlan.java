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

import com.google.auto.value.AutoValue;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.flyte.api.v1.PartialLaunchPlanIdentifier;
import org.flyte.api.v1.WorkflowNode;

// TODO: Consider removing the autovalue"ness" so we can create subclasses by calling super
// constructor
/** Reference to a LaunchPlan deployed in flyte, a remote LaunchPlan. */
@AutoValue
public abstract class SdkRemoteLaunchPlan<InputT, OutputT> extends SdkTransform<InputT, OutputT> {

  /** Returns the domain of the remote launch plan. */
  @Nullable
  public abstract String domain();

  /** Returns the project of the remote launch plan. */
  public abstract String project();

  /** Returns the name of the remote launch plan. */
  public abstract String name();

  /**
   * Returns the version of the remote launch plan. Null means that the latest version should be
   * fetched
   */
  @Nullable
  public abstract String version();

  /** See {@link #getInputType()}. */
  public abstract SdkType<InputT> inputs();

  /** See {@link #getOutputType()}. */
  public abstract SdkType<OutputT> outputs();

  /** {@inheritDoc} */
  @Override
  public SdkType<InputT> getInputType() {
    // TODO consider break backward compatibility to unify the names and avoid this bridge method
    return inputs();
  }

  /** {@inheritDoc} */
  @Override
  public SdkType<OutputT> getOutputType() {
    // TODO consider break backward compatibility to unify the names and avoid this bridge method
    return outputs();
  }

  /**
   * Create a remote launch plan, a reference to a launch plan that have been deployed previously.
   *
   * @param domain the domain of the remote launch plan
   * @param project the project of the remote launch plan
   * @param name the name of the remote launch plan
   * @param inputs the {@link SdkType} for the inputs of the remote launch plan
   * @param outputs the {@link SdkType} for the outputs of the remote launch plan
   * @return the remote launch plan
   */
  public static <InputT, OutputT> SdkRemoteLaunchPlan<InputT, OutputT> create(
      String domain,
      String project,
      String name,
      SdkType<InputT> inputs,
      SdkType<OutputT> outputs) {
    return SdkRemoteLaunchPlan.<InputT, OutputT>builder()
        .domain(domain)
        .project(project)
        .name(name)
        .inputs(inputs)
        .outputs(outputs)
        .build();
  }

  /**
   * Create a remote launch plan, a reference to a launch plan that have been deployed previously.
   *
   * @param domain the domain of the remote launch plan
   * @param project the project of the remote launch plan
   * @param name the name of the remote launch plan
   * @param version of the remote launch plan
   * @param inputs the {@link SdkType} for the inputs of the remote launch plan
   * @param outputs the {@link SdkType} for the outputs of the remote launch plan
   * @return the remote launch plan
   */
  public static <InputT, OutputT> SdkRemoteLaunchPlan<InputT, OutputT> create(
      String domain,
      String project,
      String name,
      String version,
      SdkType<InputT> inputs,
      SdkType<OutputT> outputs) {
    return SdkRemoteLaunchPlan.<InputT, OutputT>builder()
        .domain(domain)
        .project(project)
        .name(name)
        .version(version)
        .inputs(inputs)
        .outputs(outputs)
        .build();
  }

  /** {@inheritDoc} */
  @Override
  public String getName() {
    return name();
  }

  /** {@inheritDoc} */
  @Override
  public SdkNode<OutputT> apply(
      SdkWorkflowBuilder builder,
      String nodeId,
      List<String> upstreamNodeIds,
      @Nullable SdkNodeMetadata metadata,
      Map<String, SdkBindingData<?>> inputs) {
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

    Map<String, SdkBindingData<?>> outputs = outputs().promiseMapFor(nodeId);
    OutputT promise = getOutputType().promiseFor(nodeId);

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
        promise);
  }

  static <InputT, OutputT> Builder<InputT, OutputT> builder() {
    return new AutoValue_SdkRemoteLaunchPlan.Builder<>();
  }

  @AutoValue.Builder
  abstract static class Builder<InputT, OutputT> {

    public abstract Builder<InputT, OutputT> domain(String domain);

    public abstract Builder<InputT, OutputT> project(String project);

    public abstract Builder<InputT, OutputT> name(String name);

    public abstract Builder<InputT, OutputT> version(String version);

    public abstract Builder<InputT, OutputT> inputs(SdkType<InputT> inputs);

    public abstract Builder<InputT, OutputT> outputs(SdkType<OutputT> outputs);

    public abstract SdkRemoteLaunchPlan<InputT, OutputT> build();
  }
}
