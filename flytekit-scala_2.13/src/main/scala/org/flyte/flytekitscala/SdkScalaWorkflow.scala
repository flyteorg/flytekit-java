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

import org.flyte.api.v1.WorkflowTemplate
import org.flyte.flytekit.{
  SdkBindingData => SdkJavaBindingData,
  SdkNode,
  SdkTransform,
  SdkType,
  SdkWorkflow,
  SdkWorkflowBuilder
}

import scala.collection.JavaConverters._

/** This class is used to implement a Flyte workflow. This is an abstract class
  * so you need to override and implement the expand method, where you need to
  * configure the workflow logic.
  *
  * @param inputType
  *   it should be SdkScalaType[InputT].
  * @param outputType
  *   it should be SdkScalaType[OutputT].
  * @tparam InputT
  *   Input case class.
  * @tparam OutputT
  *   Output case class.
  */
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

  /** The expand method must be implement by the workflow developer. The
    * workflow developer must coding the workflow logic on this method.
    * @param builder
    *   The builder which is used to build the workflow DAG.
    * @param input
    *   The workflow input.
    * @return
    *   The workflow output.
    */
  def expand(builder: SdkScalaWorkflowBuilder, input: InputT): OutputT
}

class SdkScalaWorkflowBuilder(builder: SdkWorkflowBuilder) {

  /** Get the nodes applied on the DAG.
    * @return
    *   The workflows node by name.
    */
  def getNodes: Map[String, SdkNode[_]] = builder.getNodes.asScala.toMap

  /** Get a map with name/value of inputs.
    * @return
    *   The workflow inputs by name.
    */
  def getInputs: Map[String, SdkJavaBindingData[_]] =
    builder.getInputs.asScala.toMap

  /** Get an specific input description.
    *
    * @return
    *   The input description.
    */
  def getInputDescription(name: String): String =
    builder.getInputDescription(name)

  /** Get an specific output description.
    *
    * @return
    *   The output description.
    */
  def getOutputDescription(name: String): String =
    builder.getOutputDescription(name)

  /** Transform the current DAG to [[WorkflowTemplate]]
    * @return
    *   A workflow template
    */
  def toIdlTemplate: WorkflowTemplate = builder.toIdlTemplate

  /** Create a new node without inputs and specific ID on the workflow DAG.
    *
    * @param nodeId
    *   The specific node ID.
    * @param transform
    *   The transformation that you want to apply to the DAG.
    * @tparam OutputT
    *   The [[SdkTransform]] and [[SdkNode]] output class.
    * @return
    */
  def apply[OutputT](
      nodeId: String,
      transform: SdkTransform[Unit, OutputT]
  ): SdkNode[OutputT] =
    builder.apply(nodeId, transform, ())

  /** Create a new node with specific ID on the workflow DAG.
    *
    * @param nodeId
    *   The specific node ID.
    * @param transform
    *   The transformation that you want to apply to the DAG.
    * @param inputs
    *   The [[SdkTransform]] inputs.
    * @tparam InputT
    *   The [[SdkTransform]] input class.
    * @tparam OutputT
    *   The [[SdkTransform]] and [[SdkNode]] output class.
    * @return
    */
  def apply[InputT, OutputT](
      nodeId: String,
      transform: SdkTransform[InputT, OutputT],
      inputs: InputT
  ): SdkNode[OutputT] = builder.apply(nodeId, transform, inputs)

  /** Create a new node with specific ID on the workflow DAG.
    *
    * @param nodeId
    *   The specific node ID.
    * @param transform
    *   The transformation that you want to apply to the DAG.
    * @param inputs
    *   The [[SdkTransform]] inputs.
    * @tparam OutputT
    *   The [[SdkTransform]] and [[SdkNode]] output class.
    * @return
    */
  def applyWithInputMap[InputT, OutputT](
      nodeId: String,
      transform: SdkTransform[InputT, OutputT],
      inputs: Map[String, SdkJavaBindingData[_]]
  ): SdkNode[OutputT] =
    builder.applyWithInputMap(nodeId, transform, inputs.asJava)

  /** Create a new node without inputs on the workflow DAG.
   *
   * @param transform
   * The transformation that you want to apply to the DAG.
   * @tparam OutputT
   * The [[SdkTransform]] and [[SdkNode]] output class.
   * @return
   */
  def apply[OutputT](transform: SdkTransform[Void, OutputT]): SdkNode[OutputT] =
    builder.apply(transform, null)

  /** Create a new node without inputs on the workflow DAG.
    *
    * @param transform
    *   The transformation that you want to apply to the DAG.
    * @tparam OutputT
    *   The [[SdkTransform]] and [[SdkNode]] output class.
    * @return
    */
  def apply[OutputT](transform: SdkTransform[Unit, OutputT]): SdkNode[OutputT] =
    builder.apply(transform, ())

  /** Create a new node on the workflow DAG.
    *
    * @param transform
    *   The transformation that you want to apply to the DAG.
    * @param inputs
    *   The [[SdkTransform]] inputs.
    * @tparam InputT
    *   The [[SdkTransform]] input class.
    * @tparam OutputT
    *   The [[SdkTransform]] and [[SdkNode]] output class.
    * @return
    */
  def apply[InputT, OutputT](
      transform: SdkTransform[InputT, OutputT],
      inputs: InputT
  ): SdkNode[OutputT] = builder.apply(transform, inputs)

  /** Create a new node on the workflow DAG.
    *
    * @param transform
    *   The transformation that you want to apply to the DAG.
    * @param inputs
    *   The [[SdkTransform]] inputs.
    * @tparam OutputT
    *   The [[SdkTransform]] and [[SdkNode]] output class.
    * @return
    */
  def applyWithInputMap[InputT, OutputT](
      transform: SdkTransform[InputT, OutputT],
      inputs: Map[String, SdkJavaBindingData[_]]
  ): SdkNode[OutputT] = builder.applyWithInputMap(transform, inputs.asJava)
}
