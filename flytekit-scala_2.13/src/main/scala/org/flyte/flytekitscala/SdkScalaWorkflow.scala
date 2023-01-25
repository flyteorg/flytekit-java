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
package org.flyte.flytekitscala

import org.flyte.api.v1.{LiteralType, SimpleType, WorkflowTemplate}
import org.flyte.flytekit.{
  SdkBindingData => SdkJavaBindingData,
  SdkNode,
  SdkTransform,
  SdkType,
  SdkWorkflow,
  SdkWorkflowBuilder
}

import java.time.{Duration, Instant}
import scala.collection.JavaConverters._

abstract class SdkScalaWorkflow[InputT, OutputT](
    inputType: SdkType[InputT],
    outputType: SdkType[OutputT]
) extends SdkWorkflow[InputT, OutputT](inputType, outputType) {
  final override def expand(builder: SdkWorkflowBuilder): Unit = {
    expand(new SdkScalaWorkflowBuilder(builder))
  }

  def expand(builder: SdkScalaWorkflowBuilder): Unit
}

class SdkScalaWorkflowBuilder(builder: SdkWorkflowBuilder) {
  def inputOfInteger(
      name: String,
      help: String = ""
  ): SdkJavaBindingData[Long] =
    builder.inputOf[Long](
      name,
      LiteralType.ofSimpleType(SimpleType.INTEGER),
      help
    )

  def inputOfString(
      name: String,
      help: String = ""
  ): SdkJavaBindingData[String] =
    builder.inputOfString(name, help)

  def inputOfBoolean(
      name: String,
      help: String = ""
  ): SdkJavaBindingData[Boolean] =
    builder.inputOf[Boolean](
      name,
      LiteralType.ofSimpleType(SimpleType.BOOLEAN),
      help
    )

  def inputOfDatetime(
      name: String,
      help: String = ""
  ): SdkJavaBindingData[Instant] =
    builder.inputOfDatetime(name, help)

  def inputOfDuration(
      name: String,
      help: String = ""
  ): SdkJavaBindingData[Duration] =
    builder.inputOfDuration(name, help)

  def inputOfFloat(
      name: String,
      help: String = ""
  ): SdkJavaBindingData[Double] =
    builder.inputOf[Double](
      name,
      LiteralType.ofSimpleType(SimpleType.FLOAT),
      help
    )

  def inputOf[T](
      name: String,
      literalType: LiteralType,
      help: String = ""
  ): SdkJavaBindingData[T] = builder.inputOf(name, literalType, help)

  def getNodes: Map[String, SdkNode[_]] = builder.getNodes.asScala.toMap

  def getInputs: Map[String, SdkJavaBindingData[_]] =
    builder.getInputs.asScala.toMap

  def getInputDescription(name: String): String =
    builder.getInputDescription(name)

  def getOutputDescription(name: String): String =
    builder.getOutputDescription(name)

  def output(
      name: String,
      value: SdkJavaBindingData[_],
      help: String = ""
  ): Unit =
    builder.output(name, value, help)

  def toIdlTemplate: WorkflowTemplate = builder.toIdlTemplate

  def apply[OutputT](
      nodeId: String,
      transform: SdkTransform[Unit, OutputT]
  ): SdkNode[OutputT] =
    builder.apply(nodeId, transform, ())

  def apply[InputT, OutputT](
      nodeId: String,
      transform: SdkTransform[InputT, OutputT],
      inputs: InputT
  ): SdkNode[OutputT] = builder.apply(nodeId, transform, inputs)

  def apply[OutputT](transform: SdkTransform[Unit, OutputT]): SdkNode[OutputT] =
    builder.apply(transform, ())

  def apply[InputT, OutputT](
      transform: SdkTransform[InputT, OutputT],
      inputs: InputT
  ): SdkNode[OutputT] = builder.apply(transform, inputs)

}
