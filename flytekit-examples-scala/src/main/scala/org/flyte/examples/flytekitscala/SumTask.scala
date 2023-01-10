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

case class SumTaskInput(
    a: SdkBindingData[java.lang.Long],
    b: SdkBindingData[java.lang.Long]
)
case class SumTaskOutput(c: SdkBindingData[java.lang.Long])

class SumTask
    extends SdkRunnableTask[SumTaskInput, SumTaskOutput](
      SdkScalaType[SumTaskInput],
      SdkScalaType[SumTaskOutput]
    ) {

  override def run(input: SumTaskInput): SumTaskOutput = {
    val result = input.a.get() + input.b.get()
    SumTaskOutput(SdkBindingData.ofInteger(result))
  }

  override def isCached: Boolean = true

  override def getCacheVersion: String = "1"

  override def isCacheSerializable: Boolean = true
}

object SumTask {
  def apply(
      a: SdkBindingData[java.lang.Long],
      b: SdkBindingData[java.lang.Long]
  ): SdkTransform[SumTaskOutput] =
    new SumTask().withInput("a", a).withInput("b", b)
}
