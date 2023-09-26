/*
 * Copyright 2020-2023 Flyte Authors.
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

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkWorkflow;
import org.flyte.flytekit.SdkWorkflowBuilder;
import org.flyte.flytekit.jackson.Description;
import org.flyte.flytekit.jackson.JacksonSdkType;

/** Example workflow that takes a name and outputs a welcome message. */
@AutoService(SdkWorkflow.class)
public class WelcomeWorkflow extends SdkWorkflow<WelcomeWorkflow.Input, WelcomeWorkflow.Output> {

  @AutoValue
  public abstract static class Input {
    @Description("Name to be welcomed")
    public abstract SdkBindingData<String> name();

    public static WelcomeWorkflow.Input create(SdkBindingData<String> name) {
      return new AutoValue_WelcomeWorkflow_Input(name);
    }
  }

  @AutoValue
  public abstract static class Output {
    public abstract SdkBindingData<String> greeting();

    public static Output create(SdkBindingData<String> greeting) {
      return new AutoValue_WelcomeWorkflow_Output(greeting);
    }
  }

  public WelcomeWorkflow() {
    super(
        JacksonSdkType.of(WelcomeWorkflow.Input.class),
        JacksonSdkType.of(WelcomeWorkflow.Output.class));
  }

  @Override
  public WelcomeWorkflow.Output expand(SdkWorkflowBuilder builder, Input input) {
    // uses the workflow input as the task input of the GreetTask
    SdkBindingData<String> greeting =
        builder
            .apply("greet", new GreetTask(), GreetTask.Input.create(input.name()))
            .getOutputs()
            .greeting();

    // uses the output of the GreetTask as the task input of the AddQuestionTask
    SdkBindingData<String> greetingWithQuestion =
        builder
            .apply("add-question", new AddQuestionTask(), AddQuestionTask.Input.create(greeting))
            .getOutputs()
            .greeting();

    return WelcomeWorkflow.Output.create(greetingWithQuestion);
  }
}
