/*
 * Copyright 2021 Flyte Authors
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
package org.flyte.flytekit.testing;

import com.google.errorprone.annotations.Var;
import java.util.Map;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.Variable;
import org.flyte.flytekit.SdkNode;
import org.flyte.flytekit.SdkTransform;
import org.flyte.flytekit.SdkType;
import org.flyte.flytekit.SdkWorkflow;
import org.flyte.flytekit.SdkWorkflowBuilder;

class TestingWorkflow extends SdkWorkflow {

  private final SdkWorkflow workflow;
  private final SdkType<Map<String, Literal>> inputs;
  private final SdkType<Map<String, Literal>> outputs;

  TestingWorkflow(
      SdkWorkflow workflow,
      SdkType<Map<String, Literal>> inputs,
      SdkType<Map<String, Literal>> outputs) {
    this.workflow = workflow;
    this.inputs = inputs;
    this.outputs = outputs;
  }

  @Override
  public void expand(SdkWorkflowBuilder builder) {
    @Var SdkTransform task = new DummyTask(workflow.getName(), inputs, outputs);
    for (Map.Entry<String, Variable> i : inputs.getVariableMap().entrySet()) {
      task =
          task.withInput(i.getKey(), builder.inputOf(i.getKey(), i.getValue().literalType(), ""));
    }

    SdkNode node = builder.apply(workflow.getName(), task);

    for (String o : outputs.getVariableMap().keySet()) {
      builder.output(o, node.getOutput(o));
    }
  }
}
