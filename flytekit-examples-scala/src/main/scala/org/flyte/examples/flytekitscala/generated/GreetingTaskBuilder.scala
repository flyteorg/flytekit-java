package org.flyte.examples.flytekitscala.generated

import org.flyte.examples.flytekitscala.{GreetOutput, GreetingTask}
import org.flyte.flytekit.{SdkBindingData, SdkNode, SdkTransform}

object GreetingTaskBuilder {

  def getOutputs(node: SdkNode): GreetOutput = {
    new GreetOutput(node = node)
  }

}

class GreetingTaskBuilder {
  var name: SdkBindingData = SdkBindingData.ofString("")

  def withName(name:String): GreetingTaskBuilder = {
    this.name = SdkBindingData.ofString(name)
    this
  }

  def withName(name: SdkBindingData): GreetingTaskBuilder = {
    this.name = name
    this
  }

  def build: SdkTransform = new GreetingTask().withInput("name", name)
}
