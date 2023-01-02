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

import org.flyte.examples.flytekitscala.generated.{GreetOutput, GreetTaskBuilder}
import org.flyte.flytekit.{FlyteNode, SdkBindingData, SdkRunnableTask, SdkWorkflow, SdkWorkflowBuilder}
import org.flyte.flytekitscala.SdkScalaType

class MyWorkflow extends SdkWorkflow {

  def expand(builder: SdkWorkflowBuilder): Unit = {

    val name: SdkBindingData = builder.inputOfString("name")

    val node: FlyteNode[GreetOutput] =  builder.apply(
      new GreetTaskBuilder().withName(name).build)

    val greetNode: FlyteNode[GreetOutput] = builder.apply(
      new GreetTaskBuilder().withName("Donald Trump").build.withUpstreamNode(node.get))

    builder.output("greeting", greetNode.getOutputs.greeting())
  }
}

case class GreetTaskInput(name: String)
case class GreetTaskOutput(greeting: String)

// Annotation generating code showcased in the generated folder
//@FlyteBuilder
class GreetingTask
  extends SdkRunnableTask(
    SdkScalaType[GreetTaskInput],
    SdkScalaType[GreetTaskOutput]
  ) {
  override def run(input: GreetTaskInput): GreetTaskOutput =
    GreetTaskOutput(s"Welcome, ${input.name}!")
}