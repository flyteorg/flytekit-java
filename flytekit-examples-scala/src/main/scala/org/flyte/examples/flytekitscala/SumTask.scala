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

import org.flyte.flytekit.{SdkBindingData, SdkRunnableTask}
import org.flyte.flytekitscala.SdkBindingDataFactory._
import org.flyte.flytekitscala.{Description, SdkLiteralTypes, SdkScalaType}

case class SumTaskInput(
    @Description("First operand")
    a: SdkBindingData[Long],
    @Description("Second operand")
    b: SdkBindingData[Long]
)
class SumTask
    extends SdkRunnableTask[SumTaskInput, SdkBindingData[Long]](
      SdkScalaType[SumTaskInput],
      SdkScalaType(SdkLiteralTypes.integers(), "c", "Computed sum")
    ) {

  override def run(input: SumTaskInput): SdkBindingData[Long] = {
    of(input.a.get + input.b.get)
  }

  override def isCached: Boolean = true

  override def getCacheVersion: String = "1"

  override def isCacheSerializable: Boolean = true
}
