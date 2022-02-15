package org.flyte.examples.flytekitscala

import org.flyte.flytekit.{SdkWorkflow, SdkWorkflowBuilder}

class DynamicFibonacciWorkflow extends SdkWorkflow {

  override def expand(builder: SdkWorkflowBuilder): Unit = {
    val n = builder.inputOfInteger("n")
    val fibonacci = builder.apply("fibonacci", new DynamicFibonacciWorkflowTask().withInput("n", n))
    builder.output("output", fibonacci.getOutput("output"))
  }

}
