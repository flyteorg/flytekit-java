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
import java.util.Map;
import javax.annotation.Nullable;
import org.flyte.api.v1.PartialTaskIdentifier;
import org.flyte.api.v1.Variable;

/** Reference to a task deployed in flyte, a remote Task. */
@AutoValue
public abstract class SdkRemoteTask extends SdkTransform {

  @Nullable
  public abstract String domain();

  public abstract String project();

  public abstract String name();

  public abstract Map<String, Variable> outputs();

  public abstract Map<String, Variable> inputs();

  @Override
  public SdkNode apply(
      SdkWorkflowBuilder builder, String nodeId, Map<String, SdkBindingData> inputs) {
    PartialTaskIdentifier taskId =
        PartialTaskIdentifier.builder().name(name()).project(project()).domain(domain()).build();

    // TODO put type checking here

    return new SdkTaskNode(builder, nodeId, taskId, inputs, outputs());
  }

  public static Builder builder() {
    return new AutoValue_SdkRemoteTask.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder domain(String domain);

    public abstract Builder project(String project);

    public abstract Builder name(String name);

    public abstract Builder outputs(Map<String, Variable> outputs);

    public abstract Builder inputs(Map<String, Variable> inputs);

    public abstract SdkRemoteTask build();
  }
}
