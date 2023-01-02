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

import org.flyte.flytekit.{FlyteBuilder, FlyteBuilder2, FlyteNode, FlyteTransform, SdkBindingData, SdkNode, SdkRunnableTask, SdkTransform, SdkWorkflow, SdkWorkflowBuilder}
import org.flyte.flytekitscala.SdkScalaType


// Users would write

case class GreetTaskInput(name: String)
case class GreetTaskOutput(greeting: String)
// @FlyteTask
class GreetTask
    extends SdkRunnableTask(
      SdkScalaType[GreetTaskInput],
      SdkScalaType[GreetTaskOutput]
    ) {
  override def run(input: GreetTaskInput): GreetTaskOutput =
    GreetTaskOutput(s"Welcome, ${input.name}!")
}


class WelcomeWorkflow extends SdkWorkflow {

  def expand(builder: SdkWorkflowBuilder): Unit = {

    val greetNode: SdkNode = builder.apply(
      new GreetTaskBuilder().withName("Donald Trump").build)

    val greetOutputs: GreetOutput = GreetTaskBuilder.getOutputs(greetNode)

    builder.output("greeting", greetOutputs.greeting())
  }
}


class WelcomeWorkflow2 extends SdkWorkflow {

  def expand(builder: SdkWorkflowBuilder): Unit = {

    // Problem: we need to get the node here to potentially modify it
    // with upstream node
    val greetOutput: GreetOutput = builder.add(
      new GreetTaskBuilder().withName("Donald Trump"))
    val greetOutput2: FlyteNode[GreetOutput] = builder.addd(
      new GreetTaskBuilder().withName("Donald Trump"))
    val greetOutput3: FlyteNode[GreetOutput] = builder.adddd(
      new GreetTaskBuilder2().withName("Donald Trump").build)

    greetOutput3.getOutputs.greeting

    greetOutput3.getNode

    greetOutput2.getOutputs

    greetOutput.greeting()



  }
}


class GreetOutput(node: SdkNode)  {

  def this(node: SdkNode) {
    this(node)
  }

  def greeting(): SdkBindingData = {
    node.getOutputs.get("greeting")
  }

}

object GreetTaskBuilder  {

  def getOutputs(node: SdkNode): GreetOutput = {
    new GreetOutput(node = node)
  }

}

// We would generate and write GreetTask to META-INF
class GreetTaskBuilder extends FlyteBuilder[GreetOutput] {
  var name: SdkBindingData = SdkBindingData.ofString("")

  def withName(name:String): GreetTaskBuilder = {
    this.name = SdkBindingData.ofString(name)
    this
  }

  def withName(name: SdkBindingData): GreetTaskBuilder = {
    this.name = name
    this
  }

  @Override
  def build: SdkTransform = new GreetTask().withInput("name", name)


  @Override
  def getOutputs(node: SdkNode): GreetOutput = {
    new GreetOutput(node = node)
  }
}

// We would generate and write GreetTask to META-INF
class GreetTaskBuilder2 extends FlyteBuilder2[GreetOutput] {
  var name: SdkBindingData = SdkBindingData.ofString("")


  def withName(name:String): GreetTaskBuilder2 = {
    this.name = SdkBindingData.ofString(name)
    this
  }

  def withName(name: SdkBindingData): GreetTaskBuilder2 = {
    this.name = name
    this
  }

  @Override
  def build: FlyteTransform[GreetOutput] = new FlyteTransform[GreetOutput](new GreetTask().withInput("name", name), this)

  @Override
  def getOutputs(node: SdkNode): GreetOutput = {
    new GreetOutput(node = node)
  }
}
