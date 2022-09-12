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

import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkWorkflow;
import org.flyte.flytekit.SdkWorkflowBuilder;

public class MockPipelineWorkflow extends SdkWorkflow {
  @Override
  public void expand(SdkWorkflowBuilder builder) {
    SdkBindingData tableName = builder.inputOfString("tableName");
    SdkBindingData ref =
        builder
            .apply(
                "build-ref",
                new BuildBqReference()
                    .withInput("project", ofString("styx-1265"))
                    .withInput("dataset", ofString("styx-insights"))
                    .withInput("tableName", tableName))
            .getOutput("ref");
    SdkBindingData exists =
        builder
            .apply(
                "lookup",
                new MockLookupBqTask()
                    .withInput("ref", ref)
                    .withInput("checkIfExists", ofBoolean(true)))
            .getOutput("exists");
    builder.output("exists", exists);
  }
}
