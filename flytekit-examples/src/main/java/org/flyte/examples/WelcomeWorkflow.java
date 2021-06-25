package org.flyte.examples;

import com.google.auto.service.AutoService;
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkWorkflow;
import org.flyte.flytekit.SdkWorkflowBuilder;

/**
 * Example workflow that takes a name and outputs a welcome message
 */
@AutoService(SdkWorkflow.class)
public class WelcomeWorkflow extends SdkWorkflow {

  @Override
  public void expand(SdkWorkflowBuilder builder) {
    SdkBindingData name = builder.inputOfString("name", "The name for the welcome message");

    SdkBindingData greeting = builder.apply("greet", GreetTask.of(name)).getOutput("greeting");
    SdkBindingData greetingWithQuestion = builder.apply("add-question", AddQuestionTask.of(greeting)).getOutput("greeting");

    builder.output("greeting", greetingWithQuestion, "Welcome message");
  }
}
