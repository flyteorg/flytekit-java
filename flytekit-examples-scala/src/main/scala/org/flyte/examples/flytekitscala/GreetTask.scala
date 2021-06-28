package org.flyte.examples.flytekitscala

import org.flyte.flytekit.{SdkBindingData, SdkRunnableTask, SdkTransform}
import org.flyte.flytekitscala.SdkScalaType

case class GreetTaskInput(name: String)
case class GreetTaskOutput(greeting: String)

/** Example Flyte task that takes a name as the input and outputs a simple greeting message */
class GreetTask
    extends SdkRunnableTask(
      SdkScalaType[GreetTaskInput],
      SdkScalaType[GreetTaskOutput]
    ) {

  /**
   * Defines task behavior. This task takes a name as the input, wraps it in a welcome message, and
   * outputs the message.
   *
   * @param input the name of the person to be greeted
   * @return the welcome message
   */
  override def run(input: GreetTaskInput): GreetTaskOutput = GreetTaskOutput(s"Welcome, ${input.name}!")
}

object GreetTask {
  /**
   * Binds input data to this task
   *
   * @param name the input name
   * @return 1 transformed instance of this class with input data
   */
  def apply(name: SdkBindingData): SdkTransform =
    new GreetTask().withInput("name", name)
}
