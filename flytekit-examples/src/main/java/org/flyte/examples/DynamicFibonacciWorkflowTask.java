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
package org.flyte.examples;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import java.util.ArrayList;
import java.util.List;
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkDynamicWorkflowTask;
import org.flyte.flytekit.SdkNode;
import org.flyte.flytekit.SdkWorkflowBuilder;
import org.flyte.flytekit.jackson.JacksonSdkType;

@AutoService(SdkDynamicWorkflowTask.class)
public class DynamicFibonacciWorkflowTask
    extends SdkDynamicWorkflowTask<
        DynamicFibonacciWorkflowTask.Input, DynamicFibonacciWorkflowTask.Output> {
  public DynamicFibonacciWorkflowTask() {
    super(JacksonSdkType.of(Input.class), JacksonSdkType.of(Output.class));
  }

  @AutoValue
  abstract static class Input {
    public abstract long n();
  }

  @AutoValue
  abstract static class Output {
    public abstract long output();
  }

  @Override
  public void run(SdkWorkflowBuilder builder, Input input) {
    SdkBindingData fib0 = SdkBindingData.ofInteger(0);
    SdkBindingData fib1 = SdkBindingData.ofInteger(1);
    SdkBindingData fib2 = SdkBindingData.ofInteger(1);

    if (input.n() < 0) {
      throw new IllegalArgumentException("n < 0");
    }

    if (input.n() == 0) {
      builder.output("output", fib0);
      return;
    }

    if (input.n() == 1) {
      builder.output("output", fib1);
      return;
    }

    if (input.n() == 2) {
      builder.output("output", fib2);
      return;
    }

    List<SdkNode> nodes = new ArrayList<>();
    nodes.add(builder.apply("fib-2", SumTask.of(fib0, fib1)));
    nodes.add(builder.apply("fib-3", SumTask.of(fib1, fib2)));

    for (int i = 4; i <= input.n(); i++) {
      SdkBindingData a = nodes.get(nodes.size() - 2).getOutput("c");
      SdkBindingData b = nodes.get(nodes.size() - 1).getOutput("c");

      nodes.add(builder.apply("fib-" + i, SumTask.of(a, b)));
    }

    SdkBindingData output = nodes.get(nodes.size() - 1).getOutput("c");

    builder.output("output", output);
  }
}
