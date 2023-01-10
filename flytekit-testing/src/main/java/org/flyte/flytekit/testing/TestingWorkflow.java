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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.Variable;
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkType;
import org.flyte.flytekit.SdkWorkflow;
import org.flyte.flytekit.SdkWorkflowBuilder;

class TestingWorkflow<InputT, OutputT> extends SdkWorkflow<OutputT> {

  private final SdkType<InputT> inputType;
  private final SdkType<OutputT> outputType;
  private final OutputT output;
  private final Map<String, Literal> outputLiterals;

  TestingWorkflow(SdkType<InputT> inputType, SdkType<OutputT> outputType, OutputT output) {
    super(outputType);
    this.inputType = inputType;
    this.outputType = outputType;
    this.output = output;
    this.outputLiterals = outputType.toLiteralMap(output);
  }

  @Override
  public void expand(SdkWorkflowBuilder builder) {
    inputType.getVariableMap().forEach((name, var) -> defineInput(builder, name, var));

    outputType.getVariableMap().forEach((name, var) -> defineOutput(builder, name, var));
  }

  private static void defineInput(SdkWorkflowBuilder builder, String name, Variable var) {
    builder.inputOf(name, var.literalType(), var.description());
  }

  private void defineOutput(SdkWorkflowBuilder builder, String name, Variable var) {
    Literal literal = outputLiterals.get(name);
    SdkBindingData<?> value;
    try {
      Method method = output.getClass().getDeclaredMethod(name);
      method.setAccessible(true);
      value = (SdkBindingData<?>) method.invoke(output);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(String.format("Failure to define output - could not read attribute name %s from type %s", name, output.getClass()), e);
    } catch (InvocationTargetException | IllegalAccessException e) {
      throw new RuntimeException(String.format("Failure to define output - could invoke method %s from type %s", name, output.getClass()), e);
    }
    SdkBindingData<?> output =
        SdkBindingData.create(Literals.toBindingData(literal), var.literalType(), value.get());

    builder.output(name, output, var.description());
  }
}
