/*
 * Copyright 2020 Spotify AB.
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
package org.flyte.examples.flytekitscala;

import com.google.auto.service.AutoService
import org.flyte.flytekit.SdkRunnableTask
import org.flyte.flytekitscala.SdkScalaType

case class Input(a: Long, b: Long)
case class Output(c: Long)

@AutoService(Array(classOf[SdkRunnableTask[_, _]]))
object SumTask
    extends SdkRunnableTask(SdkScalaType[Input], SdkScalaType[Output]) {

  override def run(input: Input): Output = {
    Output(input.a + input.b)
  }
}
