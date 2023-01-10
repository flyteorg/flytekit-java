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

import static org.flyte.flytekit.SdkBindingData.ofBoolean;
import static org.flyte.flytekit.SdkBindingData.ofString;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkWorkflow;
import org.flyte.flytekit.SdkWorkflowBuilder;
import org.flyte.flytekit.jackson.JacksonSdkType;

@AutoService(SdkWorkflow.class)
public class MockPipelineWorkflow extends SdkWorkflow<MockPipelineWorkflow.Output> {
  public MockPipelineWorkflow() {
    super(JacksonSdkType.of(MockPipelineWorkflow.Output.class));
  }

  @Override
  public void expand(SdkWorkflowBuilder builder) {
    SdkBindingData<String> tableName = builder.inputOfString("tableName");
    SdkBindingData<BQReference> ref =
        builder
            .apply(
                "build-ref",
                new BuildBqReference()
                    .withInput("project", ofString("styx-1265"))
                    .withInput("dataset", ofString("styx-insights"))
                    .withInput("tableName", tableName))
            .getOutputs()
            .ref();
    SdkBindingData<Boolean> exists =
        builder
            .apply(
                "lookup",
                new MockLookupBqTask()
                    .withInput("ref", ref)
                    .withInput("checkIfExists", ofBoolean(true)))
            .getOutputs()
            .exists();
    builder.output("exists", exists);
  }

  @AutoValue
  public abstract static class Output {
    public abstract SdkBindingData<Boolean> exists();

    public static Output create(Boolean exists) {
      return new AutoValue_MockPipelineWorkflow_Output(SdkBindingData.ofBoolean(exists));
    }
  }
}
