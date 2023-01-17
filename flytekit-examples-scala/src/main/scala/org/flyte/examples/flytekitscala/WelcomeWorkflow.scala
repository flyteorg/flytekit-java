/*
 * Copyright 2021 Flyte Authors.
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
package org.flyte.examples.flytekitscala

import org.flyte.flytekit.{SdkBindingData, SdkWorkflow, SdkWorkflowBuilder}
import org.flyte.flytekitscala.Implicits._
import org.flyte.flytekitscala.SdkScalaType

/** Example workflow that takes a name and outputs a welcome message
  * |  start of workflow  |
  * |:-------------------:|
  * | input: name(string) |
  * |
  * |
  * +--------------v-----------------+
  * | GreetTask                |
  * |:-------------------------|
  * | input: name(string)      |
  * | output: greeting(string) |
  * |
  * |
  * +--------------v-----------------+
  * |     AddQuestionTask      |
  * |:------------------------:|
  * | input: greeting(string)  |
  * | output: greeting(string) |
  * |
  * |
  * +--------------v-----------------+
  * | end of workflow          |
  * |:-------------------------|
  * | output: greeting(string) |
  */

case class WelcomeWorkflowOutput(greeting: SdkBindingData[String])

class WelcomeWorkflow
    extends SdkWorkflow[WelcomeWorkflowOutput](
      SdkScalaType[WelcomeWorkflowOutput]
    ) {

  def expand(builder: SdkWorkflowBuilder): Unit = {
    implicit val b = builder

    // defines the input of the workflow
    val name = inputOfString("name", "The name for the welcome message")

    // uses the workflow input as the task input of the GreetTask
    val greeting = GreetTask(name).getOutputs.greeting

    // uses the output of the GreetTask as the task input of the AddQuestionTask
    val greetingWithQuestion = AddQuestionTask(greeting).getOutputs.greeting

    // uses the task output of the AddQuestionTask as the output of the workflow
    builder.output("greeting", greetingWithQuestion, "Welcome message")
  }
}
