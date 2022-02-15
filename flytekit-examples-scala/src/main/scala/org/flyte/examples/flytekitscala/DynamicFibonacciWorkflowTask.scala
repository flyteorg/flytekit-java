package org.flyte.examples.flytekitscala

import org.flyte.flytekit.{SdkBindingData, SdkDynamicWorkflowTask, SdkWorkflowBuilder}
import org.flyte.flytekitscala.SdkScalaType

import scala.annotation.tailrec

case class DynamicFibonacciWorkflowTaskInput(n: Long)
case class DynamicFibonacciWorkflowTaskOutput(output: Long)

class DynamicFibonacciWorkflowTask
    extends SdkDynamicWorkflowTask(
      SdkScalaType[DynamicFibonacciWorkflowTaskInput],
      SdkScalaType[DynamicFibonacciWorkflowTaskOutput]
    ) {

  override def run(builder: SdkWorkflowBuilder, input: DynamicFibonacciWorkflowTaskInput): Unit = {

    @tailrec
    def fib(n: Long, value: SdkBindingData, prev: SdkBindingData): SdkBindingData = {
      if (n == input.n) value
      else fib(n + 1, builder(s"fib-${n + 1}", SumTask(value, prev)).getOutput("c"), value)
    }

    require(input.n > 0, "n < 0")
    val value = if (input.n == 0) {
      SdkBindingData.ofInteger(0)
    } else {
      fib(1, SdkBindingData.ofInteger(1), SdkBindingData.ofInteger(0))
    }
    builder.output("output", value)
  }

}
