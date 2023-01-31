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

import org.flyte.flytekit.{SdkBindingData, SdkRemoteTask}
import org.flyte.flytekitscala.SdkScalaType

case class RemoteSumTaskInput(
    a: SdkBindingData[Long],
    b: SdkBindingData[Long]
)
case class RemoteSumTaskOutput(c: SdkBindingData[Long])

class RemoteSumTask {
  def create: SdkRemoteTask[RemoteSumTaskInput, RemoteSumTaskOutput] = {
    SdkRemoteTask.create(
      /* domain= */ null,
      /* project= */ "flytesnacks",
      /* name= */ "org.flyte.examples.flytekitscala.SumTask",
      SdkScalaType[RemoteSumTaskInput],
      SdkScalaType[RemoteSumTaskOutput]
    )
  }

}
