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
package org.flyte.examples;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;
import org.flyte.api.v1.Literal;
import org.flyte.flytekit.testing.Literals;
import org.flyte.flytekit.testing.SdkTestingExecutor;
import org.junit.jupiter.api.Test;

public class WorkflowTest {

  @Test
  public void testSubWorkflow() {
    SdkTestingExecutor.Result result =
        SdkTestingExecutor.of(new UberWorkflow())
            .withFixedInput("a", 1)
            .withFixedInput("b", 2)
            .withFixedInput("c", 3)
            .withFixedInput("d", 4)
            .execute();

    assertEquals(10L, result.getIntegerOutput("total"));
  }

  @Test
  public void testMockTasks() {
    SdkTestingExecutor.Result result =
        SdkTestingExecutor.of(new UberWorkflow())
            .withFixedInput("a", 1)
            .withFixedInput("b", 2)
            .withFixedInput("c", 3)
            .withFixedInput("d", 4)
            .withTaskOutput(
                new SumTask(), SumTask.SumInput.create(1L, 2L), SumTask.SumOutput.create(0L))
            .withTaskOutput(
                new SumTask(), SumTask.SumInput.create(0L, 3L), SumTask.SumOutput.create(0L))
            .withTaskOutput(
                new SumTask(), SumTask.SumInput.create(0L, 4L), SumTask.SumOutput.create(42L))
            .execute();

    assertEquals(42L, result.getIntegerOutput("total"));
  }

  @Test
  public void testMockSubWorkflow() {
    Map<String, Literal> sub1Inputs = new HashMap<>();
    sub1Inputs.put("left", Literals.ofInteger(1));
    sub1Inputs.put("right", Literals.ofInteger(2));
    Map<String, Literal> sub1Outputs = new HashMap<>();
    sub1Outputs.put("result", Literals.ofInteger(0));

    Map<String, Literal> sub2Inputs = new HashMap<>();
    sub2Inputs.put("left", Literals.ofInteger(0));
    sub2Inputs.put("right", Literals.ofInteger(3));
    Map<String, Literal> sub2Outputs = new HashMap<>();
    sub2Outputs.put("result", Literals.ofInteger(0));

    SdkTestingExecutor.Result result =
        SdkTestingExecutor.of(new UberWorkflow())
            .withFixedInput("a", 1)
            .withFixedInput("b", 2)
            .withFixedInput("c", 3)
            .withFixedInput("d", 4)
            .withWorkflowOutput(new SubWorkflow(), sub1Inputs, sub1Outputs)
            .withWorkflowOutput(new SubWorkflow(), sub2Inputs, sub2Outputs)
            .withTaskOutput(
                new SumTask(), SumTask.SumInput.create(0L, 4L), SumTask.SumOutput.create(42L))
            .execute();

    assertEquals(42L, result.getIntegerOutput("total"));
  }
}
