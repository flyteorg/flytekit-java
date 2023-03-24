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

import static java.util.Objects.requireNonNull;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.PartialIdentifier;
import org.flyte.api.v1.RunnableNode;
import org.flyte.flytekit.SdkType;

public abstract class TestingRunnableNode<
        IdT extends PartialIdentifier,
        InputT,
        OutputT,
        T extends TestingRunnableNode<IdT, InputT, OutputT, T>>
    implements RunnableNode {
  protected final IdT id;
  protected final SdkType<InputT> inputType;
  protected final SdkType<OutputT> outputType;

  // @Nullable - signal nullable field but without adding the dependency
  protected final Function<InputT, OutputT> runFn;

  protected final Map<InputT, OutputT> fixedOutputs;
  private final Creator<IdT, InputT, OutputT, T> creatorFn;
  private final String type;
  private final String testingSuggestion;

  interface Creator<
      IdT extends PartialIdentifier,
      InputT,
      OutputT,
      T extends TestingRunnableNode<IdT, InputT, OutputT, T>> {
    T create(
        IdT id,
        SdkType<InputT> inputType,
        SdkType<OutputT> outputType,
        Function<InputT, OutputT> runFn,
        Map<InputT, OutputT> fixedOutputs);
  }

  protected TestingRunnableNode(
      IdT id,
      SdkType<InputT> inputType,
      SdkType<OutputT> outputType,
      Function<InputT, OutputT> runFn,
      Map<InputT, OutputT> fixedOutputs,
      Creator<IdT, InputT, OutputT, T> creatorFn,
      String type,
      String testingSuggestion) {
    this.id = requireNonNull(id, "id");
    this.inputType = requireNonNull(inputType, "inputType");
    this.outputType = requireNonNull(outputType, "outputType");
    this.runFn = runFn; // Nullable
    this.fixedOutputs = requireNonNull(fixedOutputs, "fixedOutputs");
    this.creatorFn = requireNonNull(creatorFn, "creatorFn");
    this.type = requireNonNull(type, "type");
    this.testingSuggestion = requireNonNull(testingSuggestion, "testingSuggestion");
  }

  @Override
  public Map<String, Literal> run(Map<String, Literal> inputs) {
    InputT input = inputType.fromLiteralMap(inputs);

    if (fixedOutputs.size() == 0) {
      // No mocking? Run the real stuff
      if (runFn != null) {
        return outputType.toLiteralMap(runFn.apply(input));
      }
    } else if (fixedOutputs.containsKey(input)) {
      return outputType.toLiteralMap(fixedOutputs.get(input));
    }

    String message =
        String.format(
            "Can't find input %s for remote %s [%s] across known %s inputs, "
                + "use %s to provide a test double",
            input, type, getName(), type, testingSuggestion);

    // Not matching inputs and there is nothing to run
    throw new IllegalArgumentException(message);
  }

  @Override
  public String getName() {
    return id.name();
  }

  public T withFixedOutput(InputT input, OutputT output) {
    Map<InputT, OutputT> newFixedOutputs = new HashMap<>(fixedOutputs);
    newFixedOutputs.put(input, output);

    return creatorFn.create(id, inputType, outputType, runFn, newFixedOutputs);
  }

  public T withRunFn(Function<InputT, OutputT> runFn) {
    return creatorFn.create(id, inputType, outputType, runFn, fixedOutputs);
  }
}
