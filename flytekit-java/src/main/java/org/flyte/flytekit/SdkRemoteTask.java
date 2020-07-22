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
package org.flyte.flytekit;

import com.google.auto.value.AutoValue;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.flyte.api.v1.PartialTaskIdentifier;

/** Reference to a task deployed in flyte, a remote Task. */
@AutoValue
public abstract class SdkRemoteTask<InputT, OutputT> extends SdkTransform {

  @Nullable
  public abstract String domain();

  public abstract String project();

  public abstract String name();

  public abstract SdkType<InputT> inputs();

  public abstract SdkType<OutputT> outputs();

  public static <InputT, OutputT> SdkRemoteTask<InputT, OutputT> create(
      String domain,
      String project,
      String name,
      SdkType<InputT> inputs,
      SdkType<OutputT> outputs) {
    return SdkRemoteTask.<InputT, OutputT>builder()
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
      Map<String, SdkBindingData> inputs) {
    PartialTaskIdentifier taskId =
        PartialTaskIdentifier.builder().name(name()).project(project()).domain(domain()).build();
    List<CompilerError> errors = Compiler.validateApply(nodeId, inputs, inputs().getVariableMap());

    if (!errors.isEmpty()) {
      throw new CompilerException(errors);
    }

    return new SdkTaskNode(
        builder, nodeId, taskId, upstreamNodeIds, inputs, outputs().getVariableMap());
  }

  public static <InputT, OutputT> Builder<InputT, OutputT> builder() {
    return new AutoValue_SdkRemoteTask.Builder<>();
  }

  @AutoValue.Builder
  public abstract static class Builder<InputT, OutputT> {

    public abstract Builder<InputT, OutputT> domain(String domain);

    public abstract Builder<InputT, OutputT> project(String project);

    public abstract Builder<InputT, OutputT> name(String name);

    public abstract Builder<InputT, OutputT> inputs(SdkType<InputT> inputs);

    public abstract Builder<InputT, OutputT> outputs(SdkType<OutputT> outputs);

    public abstract SdkRemoteTask<InputT, OutputT> build();
  }
}
