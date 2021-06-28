package org.flyte.examples.flytekitscala

import org.flyte.flytekit.{SdkWorkflow, SdkWorkflowBuilder}

/** Example workflow that takes a name and outputs a welcome message
 * +--------------------------------+
 * |        start of workflow       |
 * |      input: name(string)       |
 * +--------------+-----------------+
 *                |
 *                |
 * +--------------v-----------------+
 * |          GreetTask             |
 * |      input: name(string)       |
 * |      output: greeting(string)  |
 * +--------------+-----------------+
 *                |
 *                |
 * +--------------v-----------------+
 * |        AddQuestionTask         |
 * |      input: greeting(string)   |
 * |      output: greeting(string)  |
 * +--------------+-----------------+
 *                |
 *                |
 * +--------------v-----------------+
 * |       end of workflow          |
 * |     output: greeting(string)   |
 * +--------------------------------+
 */
class WelcomeWorkflow extends SdkWorkflow {

  def expand(builder: SdkWorkflowBuilder): Unit = {
    // defines the input of the workflow
    val name = builder.inputOfString("name", "The name for the welcome message")

    // uses the workflow input as the task input of the GreetTask
    val greeting = builder.apply("greet", GreetTask(name)).getOutput("greeting")

    // uses the output of the GreetTask as the task input of the AddQuestionTask
    val greetingWithQuestion = builder.apply("add-question", AddQuestionTask(greeting)).getOutput("greeting")

    // uses the task output of the AddQuestionTask as the output of the workflow
    builder.output("greeting", greetingWithQuestion, "Welcome message")
  }
}
