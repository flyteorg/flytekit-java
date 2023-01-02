package org.flyte.examples.flytekitscala.generated

import org.flyte.examples.flytekitscala.GreetingTask
import org.flyte.flytekit.{FlyteBuilder, FlyteTransform, SdkBindingData, SdkNode}

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
  def build: FlyteTransform[GreetOutput] = new FlyteTransform[GreetOutput](new GreetingTask().withInput("name", name), this)

  @Override
  def getOutputs(node: SdkNode): GreetOutput = {
    new GreetOutput(node = node)
  }
}