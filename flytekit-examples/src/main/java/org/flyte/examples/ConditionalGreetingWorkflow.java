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
package org.flyte.examples;

import static org.flyte.flytekit.SdkBindingData.ofString;
import static org.flyte.flytekit.SdkConditions.eq;

import com.google.auto.service.AutoService;
import org.flyte.flytekit.NopOutputTransformer;
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkConditions;
import org.flyte.flytekit.SdkWorkflow;
import org.flyte.flytekit.SdkWorkflowBuilder;

@AutoService(SdkWorkflow.class)
public class ConditionalGreetingWorkflow extends SdkWorkflow<NopOutputTransformer> {
  @Override
  public void expand(SdkWorkflowBuilder builder) {
    SdkBindingData name = builder.inputOfString("name");
    SdkBindingData greeting =
        builder
            .apply(
                "decide",
                SdkConditions.when(
                        "when-empty", eq(name, ofString("")), GreetTask.of(ofString("World")))
                    .otherwise("when-not-empty", GreetTask.of(name)))
            .getOutput("greeting");

    builder.output("greeting", greeting);
  }
}
