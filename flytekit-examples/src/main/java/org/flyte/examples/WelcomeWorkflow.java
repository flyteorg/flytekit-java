package org.flyte.examples;

import com.google.auto.service.AutoService;
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkWorkflow;
import org.flyte.flytekit.SdkWorkflowBuilder;

/** Example workflow that takes a name and outputs a welcome message
 *  +--------------------------------+
 *  |        start of workflow       |
 *  |      input: name(string)       |
 *  +--------------+-----------------+
 *                 |
 *                 |
 *  +--------------v-----------------+
 *  |          GreetTask             |
 *  |      input: name(string)       |
 *  |      output: greeting(string)  |
 *  +--------------+-----------------+
 *                 |
 *                 |
 *  +--------------v-----------------+
 *  |        AddQuestionTask         |
 *  |      input: greeting(string)   |
 *  |      output: greeting(string)  |
 *  +--------------+-----------------+
 *                 |
 *                 |
 *  +--------------v-----------------+
 *  |       end of workflow          |
 *  |     output: greeting(string)   |
 *  +--------------------------------+
 */
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
