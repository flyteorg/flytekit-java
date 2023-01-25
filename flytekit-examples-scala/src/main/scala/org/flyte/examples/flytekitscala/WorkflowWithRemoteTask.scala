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

import org.flyte.flytekit.{SdkBindingData}
import org.flyte.flytekitscala.{
  SdkScalaType,
  SdkScalaWorkflow,
  SdkScalaWorkflowBuilder
}

class WorkflowWithRemoteTask
    extends SdkScalaWorkflow[RemoteSumTaskInput, RemoteSumTaskOutput](
      SdkScalaType[RemoteSumTaskInput],
      SdkScalaType[RemoteSumTaskOutput]
    ) {

  override def expand(builder: SdkScalaWorkflowBuilder): Unit = {
    val a = SdkBindingData.ofInteger(10)
    val b = SdkBindingData.ofInteger(12)

    val c = builder
      .apply(new RemoteSumTask().create, RemoteSumTaskInput(a, b))
      .getOutputs
      .c

    builder.output("c", c)
  }
}
