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

/**
 * Remote workflow execution is not intended to be supported without a launch plan. This code
 * "glues" the SdkRemoteWorkflow by referencing the default Flyte LaunchPlan instead. Consider using
 * SdkRemoteLaunchPlan instead
 */
@AutoValue
public abstract class SdkRemoteWorkflow<InputT, OutputT> extends SdkTransform {

  @Nullable
  public abstract String domain();

  public abstract String project();

  public abstract String name();

  @Nullable
  public String version() {
    return null;
  }

  public abstract SdkType<InputT> inputs();

  public abstract SdkType<OutputT> outputs();

  public static <InputT, OutputT> SdkRemoteWorkflow<InputT, OutputT> create(
      String domain,
      String project,
      String name,
      SdkType<InputT> inputs,
      SdkType<OutputT> outputs) {
    return SdkRemoteWorkflow.<InputT, OutputT>builder()
        .domain(domain)
        .project(project)
        .name(name)
        .inputs(inputs)
        .outputs(outputs)
        .build();
  }

  @Override
  public SdkNode apply(
      SdkWorkflowBuilder builder,
      String nodeId,
      List<String> upstreamNodeIds,
      @Nullable SdkNodeMetadata metadata,
      Map<String, SdkBindingData> inputs) {
    SdkRemoteLaunchPlan<InputT, OutputT> defaultLaunchPlan =
        AutoValue_SdkRemoteLaunchPlan.<InputT, OutputT>builder()
            .project(project())
            .domain(domain())
            .name(name())
            .version(version())
            .inputs(inputs())
            .outputs(outputs())
            .build();

    return defaultLaunchPlan.apply(builder, nodeId, upstreamNodeIds, metadata, inputs);
  }

  public static <InputT, OutputT> Builder<InputT, OutputT> builder() {
    return new AutoValue_SdkRemoteWorkflow.Builder<>();
  }

  @AutoValue.Builder
  public abstract static class Builder<InputT, OutputT> {

    public abstract Builder<InputT, OutputT> domain(String domain);

    public abstract Builder<InputT, OutputT> project(String project);

    public abstract Builder<InputT, OutputT> name(String name);

    public abstract Builder<InputT, OutputT> inputs(SdkType<InputT> inputs);

    public abstract Builder<InputT, OutputT> outputs(SdkType<OutputT> outputs);

    public abstract SdkRemoteWorkflow<InputT, OutputT> build();
  }
}
