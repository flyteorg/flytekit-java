/*
 * Copyright 2023 Flyte Authors.
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

import org.flyte.flytekit.{SdkBindingData, SdkRunnableTask, SdkTransform}
import org.flyte.flytekitscala.{
  Description,
  SdkBindingDataFactory,
  SdkScalaType
}

case class NestedNestedNested(string: String)
case class NestedNested(double: Double, nested: Option[NestedNestedNested])
case class Nested(
    boolean: Boolean,
    byte: Byte,
    short: Short,
    int: Int,
    long: Long,
    float: Float,
    double: Double,
    string: String,
    list: List[String],
    listOfNested: List[NestedNested],
    map: Map[String, String],
    mapOfNested: Map[String, NestedNested],
    optBoolean: Option[Boolean],
    optByte: Option[Byte],
    optList: Option[List[String]],
    optMap: Option[Map[String, String]],
    nested: NestedNested
)
case class NestedIOTaskInput(
    @Description("the name of the person to be greeted")
    name: SdkBindingData[String],
    @Description("a nested input")
    generic: SdkBindingData[Nested]
)
case class NestedIOTaskOutput(
    @Description("the name of the person to be greeted")
    name: SdkBindingData[String],
    @Description("a nested input")
    generic: SdkBindingData[Nested]
)

/** Example Flyte task that takes a name as the input and outputs a simple
  * greeting message.
  */
class NestedIOTask
    extends SdkRunnableTask[
      NestedIOTaskInput,
      NestedIOTaskOutput
    ](
      SdkScalaType[NestedIOTaskInput],
      SdkScalaType[NestedIOTaskOutput]
    ) {

  /** Defines task behavior. This task takes a name as the input, wraps it in a
    * welcome message, and outputs the message.
    *
    * @param input
    *   the name of the person to be greeted
    * @return
    *   the welcome message
    */
  override def run(input: NestedIOTaskInput): NestedIOTaskOutput =
    NestedIOTaskOutput(
      input.name,
      input.generic
    )
}
