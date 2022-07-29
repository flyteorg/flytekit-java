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

import java.util.Map;
import org.flyte.api.v1.BindingData;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.Variable;
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkType;
import org.flyte.flytekit.SdkWorkflow;
import org.flyte.flytekit.SdkWorkflowBuilder;

class TestingWorkflow<InputT, OutputT> extends SdkWorkflow {

  private final SdkType<InputT> inputType;
  private final SdkType<OutputT> outputType;
  private final Map<String, Literal> outputLiterals;

  TestingWorkflow(SdkType<InputT> inputType, SdkType<OutputT> outputType, OutputT output) {
    this.inputType = inputType;
    this.outputType = outputType;
    this.outputLiterals = outputType.toLiteralMap(output);
  }

  @Override
  public void expand(SdkWorkflowBuilder builder) {
    inputType.getVariableMap().forEach((name, var) -> builder.inputOf(name, var.literalType(), ""));

    for (Map.Entry<String, Variable> entries : outputType.getVariableMap().entrySet()) {
      String name = entries.getKey();
      Variable var = entries.getValue();
      BindingData outputBinding = Literals.toBindingData(outputLiterals.get(name));
      SdkBindingData output = SdkBindingData.create(outputBinding, var.literalType());

      builder.output(name, output, "");
    }
  }
}
