package org.flyte.examples.flytekitscala.generated

import org.flyte.flytekit.{SdkBindingData, SdkNode}

class GreetOutput(node: SdkNode)  {

  def this(node: SdkNode) {
    this(node)
  }

  def greeting(): SdkBindingData = {
    node.getOutputs.get("greeting")
  }

}
