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
  SdkBindingData,
  SdkNode,
  SdkTransform,
  SdkType,
  SdkWorkflow,
  SdkWorkflowBuilder
}

import java.time.{Duration, Instant}
import scala.collection.JavaConverters._

abstract class SdkScalaWorkflow[T](outputType: SdkType[T])
    extends SdkWorkflow[T](outputType) {
  final override def expand(builder: SdkWorkflowBuilder): Unit = {
    expand(new SdkScalaWorkflowBuilder(builder))
  }

  def expand(builder: SdkScalaWorkflowBuilder): Unit
}

class SdkScalaWorkflowBuilder(builder: SdkWorkflowBuilder) {
  def inputOfInteger(name: String, help: String = ""): SdkBindingData[Long] =
    builder.inputOf[Long](
      name,
      LiteralType.ofSimpleType(SimpleType.INTEGER),
      help
    )

  def inputOfString(name: String, help: String = ""): SdkBindingData[String] =
    builder.inputOfString(name, help)

  def inputOfBoolean(name: String, help: String = ""): SdkBindingData[Boolean] =
    builder.inputOf[Boolean](
      name,
      LiteralType.ofSimpleType(SimpleType.BOOLEAN),
      help
    )

  def inputOfDatetime(
      name: String,
      help: String = ""
  ): SdkBindingData[Instant] =
    builder.inputOfDatetime(name, help)

  def inputOfDuration(
      name: String,
      help: String = ""
  ): SdkBindingData[Duration] =
    builder.inputOfDuration(name, help)

  def inputOfFloat(name: String, help: String = ""): SdkBindingData[Double] =
    builder.inputOf[Double](
      name,
      LiteralType.ofSimpleType(SimpleType.FLOAT),
      help
    )

  def inputOf[T](
      name: String,
      literalType: LiteralType,
      help: String = ""
  ): SdkBindingData[T] = builder.inputOf(name, literalType, help)

  def getNodes: Map[String, SdkNode[_]] = builder.getNodes.asScala.toMap

  def getInputs: Map[String, SdkBindingData[_]] =
    builder.getInputs.asScala.toMap

  def getInputDescription(name: String): String =
    builder.getInputDescription(name)

  def getOutputDescription(name: String): String =
    builder.getOutputDescription(name)

  def output(name: String, value: SdkBindingData[_], help: String = "") =
    builder.output(name, value, help)

  def toIdlTemplate: WorkflowTemplate = builder.toIdlTemplate

  def apply[T](nodeId: String, transform: SdkTransform[T]): SdkNode[T] =
    builder.apply(nodeId, transform)

  def apply[T](
      nodeId: String,
      transform: SdkTransform[T],
      inputs: Map[String, SdkBindingData[_]]
  ): SdkNode[T] = builder.apply(nodeId, transform, inputs.asJava)

  def apply[T](transform: SdkTransform[T]): SdkNode[T] =
    builder.apply(transform)

  def apply[T](
      transform: SdkTransform[T],
      inputs: Map[String, SdkBindingData[_]]
  ): SdkNode[T] = builder.apply(transform, inputs.asJava)

}
