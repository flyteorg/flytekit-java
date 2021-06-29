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
package org.flyte.examples;

import com.google.auto.service.AutoService;
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkWorkflow;
import org.flyte.flytekit.SdkWorkflowBuilder;

/** Example workflow that takes a name and outputs a welcome message. */
@AutoService(SdkWorkflow.class)
public class WelcomeWorkflow extends SdkWorkflow {

  @Override
  public void expand(SdkWorkflowBuilder builder) {
    // defines the input of the workflow
    SdkBindingData name = builder.inputOfString("name", "The name for the welcome message");

    // uses the workflow input as the task input of the GreetTask
    SdkBindingData greeting = builder.apply("greet", GreetTask.of(name)).getOutput("greeting");

    // uses the output of the GreetTask as the task input of the AddQuestionTask
    SdkBindingData greetingWithQuestion =
        builder.apply("add-question", AddQuestionTask.of(greeting)).getOutput("greeting");

    // uses the task output of the AddQuestionTask as the output of the workflow
    builder.output("greeting", greetingWithQuestion, "Welcome message");
  }
}
