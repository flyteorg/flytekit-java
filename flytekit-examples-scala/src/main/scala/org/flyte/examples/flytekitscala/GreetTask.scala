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
package org.flyte.examples.flytekitscala

import org.flyte.flytekit.{SdkBindingData, SdkRunnableTask, SdkTransform}
import org.flyte.flytekitscala.SdkScalaType
import org.flyte.flytekitscala.SdkBindingData._

case class GreetTaskInput(name: SdkBindingData[String])
case class GreetTaskOutput(greeting: SdkBindingData[String])

/** Example Flyte task that takes a name as the input and outputs a simple
  * greeting message.
  */
class GreetTask
    extends SdkRunnableTask[
      GreetTaskInput,
      GreetTaskOutput
    ](
      SdkScalaType[GreetTaskInput],
      SdkScalaType[GreetTaskOutput]
    ) {

  /** Defines task behavior. This task takes a name as the input, wraps it in a
    * welcome message, and outputs the message.
    *
    * @param input
    *   the name of the person to be greeted
    * @return
    *   the welcome message
    */
  override def run(input: GreetTaskInput): GreetTaskOutput =
    GreetTaskOutput(ofString(s"Welcome, ${input.name.get()}!"))
}
