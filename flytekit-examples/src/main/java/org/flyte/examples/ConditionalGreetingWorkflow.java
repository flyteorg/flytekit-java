/*
 * Copyright 2020-2023 Flyte Authors
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

import static org.flyte.flytekit.SdkConditions.eq;

import com.google.auto.service.AutoService;
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkBindingDataFactory;
import org.flyte.flytekit.SdkConditions;
import org.flyte.flytekit.SdkWorkflow;
import org.flyte.flytekit.SdkWorkflowBuilder;
import org.flyte.flytekit.jackson.JacksonSdkType;

@AutoService(SdkWorkflow.class)
public class ConditionalGreetingWorkflow extends SdkWorkflow<GreetTask.Input, GreetTask.Output> {
  public ConditionalGreetingWorkflow() {
    super(JacksonSdkType.of(GreetTask.Input.class), JacksonSdkType.of(GreetTask.Output.class));
  }

  @Override
  public GreetTask.Output expand(SdkWorkflowBuilder builder, GreetTask.Input input) {
    SdkBindingData<String> greeting =
        builder
            .apply(
                "decide",
                SdkConditions.when(
                        "when-empty",
                        eq(input.name(), SdkBindingDataFactory.of("")),
                        new GreetTask(),
                        GreetTask.Input.create(SdkBindingDataFactory.of("World")))
                    .otherwise(
                        "when-not-empty", new GreetTask(), GreetTask.Input.create(input.name())))
            .getOutputs()
            .greeting();

    return GreetTask.Output.create(greeting);
  }
}
