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

import org.flyte.api.v1.{WorkflowTemplate}
import org.flyte.flytekit.{
  SdkBindingData => SdkJavaBindingData,
  SdkNode,
  SdkTransform,
  SdkType,
  SdkWorkflow,
  SdkWorkflowBuilder
}

import scala.collection.JavaConverters._

abstract class SdkScalaWorkflow[InputT, OutputT](
    inputType: SdkType[InputT],
    outputType: SdkType[OutputT]
) extends SdkWorkflow[InputT, OutputT](inputType, outputType) {
  final override def expand(
      builder: SdkWorkflowBuilder,
      input: InputT
  ): OutputT = {
    expand(new SdkScalaWorkflowBuilder(builder), input)
  }

  def expand(builder: SdkScalaWorkflowBuilder, input: InputT): OutputT
}

class SdkScalaWorkflowBuilder(builder: SdkWorkflowBuilder) {

  def getNodes: Map[String, SdkNode[_]] = builder.getNodes.asScala.toMap

  def getInputs: Map[String, SdkJavaBindingData[_]] =
    builder.getInputs.asScala.toMap

  def getInputDescription(name: String): String =
    builder.getInputDescription(name)

  def getOutputDescription(name: String): String =
    builder.getOutputDescription(name)

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
