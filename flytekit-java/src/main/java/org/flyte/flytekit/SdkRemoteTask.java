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
import org.flyte.api.v1.PartialTaskIdentifier;

/** Reference to a task deployed in flyte, a remote Task. */
@AutoValue
public abstract class SdkRemoteTask<InputT, OutputT, NamedOutputT extends NamedOutput>
    extends SdkTransform<NamedOutputT> {

  @Nullable
  public abstract String domain();

  public abstract String project();

  public abstract String name();

  // FIXME should be auto-value property, but doing so breaks backwards-compatibility, to be fixed
  // in 0.3.0

  @Nullable
  public String version() {
    return null;
  }

  public abstract SdkType<InputT> inputs();

  public abstract SdkType<OutputT> outputs();

  public static <InputT, OutputT, NamedOutputT extends NamedOutput>
      SdkRemoteTask<InputT, OutputT, NamedOutputT> create(
          String domain,
          String project,
          String name,
          SdkType<InputT> inputs,
          SdkType<OutputT> outputs) {
    return SdkRemoteTask.<InputT, OutputT, NamedOutputT>builder()
        .domain(domain)
        .project(project)
        .name(name)
        .inputs(inputs)
        .outputs(outputs)
        .build();
  }

  @Override
  public SdkNode<NamedOutputT> apply(
      SdkWorkflowBuilder builder,
      String nodeId,
      List<String> upstreamNodeIds,
      @Nullable SdkNodeMetadata metadata,
      Map<String, SdkBindingData> inputs) {
    PartialTaskIdentifier taskId =
        PartialTaskIdentifier.builder()
            .name(name())
            .project(project())
            .domain(domain())
            .version(version())
            .build();
    List<CompilerError> errors = Compiler.validateApply(nodeId, inputs, inputs().getVariableMap());

    if (!errors.isEmpty()) {
      throw new CompilerException(errors);
    }

    return new SdkTaskNode<>(
        builder,
        nodeId,
        taskId,
        upstreamNodeIds,
        metadata,
        inputs,
        outputs().getVariableMap(),
        getNamedOutputClass());
  }

  public static <InputT, OutputT, NamedOutputT extends NamedOutput>
      Builder<InputT, OutputT, NamedOutputT> builder() {
    return new AutoValue_SdkRemoteTask.Builder<>();
  }

  @AutoValue.Builder
  public abstract static class Builder<InputT, OutputT, NamedOutputT extends NamedOutput> {

    public abstract Builder<InputT, OutputT, NamedOutputT> domain(String domain);

    public abstract Builder<InputT, OutputT, NamedOutputT> project(String project);

    public abstract Builder<InputT, OutputT, NamedOutputT> name(String name);

    public abstract Builder<InputT, OutputT, NamedOutputT> inputs(SdkType<InputT> inputs);

    public abstract Builder<InputT, OutputT, NamedOutputT> outputs(SdkType<OutputT> outputs);

    public abstract SdkRemoteTask<InputT, OutputT, NamedOutputT> build();
  }
}
