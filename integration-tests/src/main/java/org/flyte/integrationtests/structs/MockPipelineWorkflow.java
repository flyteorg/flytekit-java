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
package org.flyte.integrationtests.structs;

import static org.flyte.flytekit.SdkBindingDataFactory.of;

import com.google.auto.value.AutoValue;
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkBindingDataFactory;
import org.flyte.flytekit.SdkWorkflow;
import org.flyte.flytekit.SdkWorkflowBuilder;
import org.flyte.flytekit.jackson.JacksonSdkType;

// This workflow relays on SdkBinding<BQReference> that should be serialized
// as Struct. By going to typed inputs and outputs, we have de-scoped the support
// of structs.
// @AutoService(SdkWorkflow.class)
public class MockPipelineWorkflow
    extends SdkWorkflow<MockPipelineWorkflow.Input, MockPipelineWorkflow.Output> {
  public MockPipelineWorkflow() {
    super(
        JacksonSdkType.of(MockPipelineWorkflow.Input.class),
        JacksonSdkType.of(MockPipelineWorkflow.Output.class));
  }

  @Override
  public Output expand(SdkWorkflowBuilder builder, Input input) {
    SdkBindingData<BQReference> ref =
        builder
            .apply(
                "build-ref",
                new BuildBqReference(),
                BuildBqReference.Input.create(
                    of("styx-1265"), of("styx-insights"), input.tableName()))
            .getOutputs()
            .ref();
    SdkBindingData<Boolean> exists =
        builder
            .apply(
                "lookup",
                new MockLookupBqTask(),
                MockLookupBqTask.Input.create(ref, SdkBindingDataFactory.of(true)))
            .getOutputs()
            .exists();
    return Output.create(exists);
  }

  @AutoValue
  public abstract static class Input {
    public abstract SdkBindingData<String> tableName();

    public static Input create(SdkBindingData<String> tableName) {
      return new AutoValue_MockPipelineWorkflow_Input(tableName);
    }
  }

  @AutoValue
  public abstract static class Output {
    public abstract SdkBindingData<Boolean> exists();

    public static Output create(SdkBindingData<Boolean> exists) {
      return new AutoValue_MockPipelineWorkflow_Output(exists);
    }
  }
}
