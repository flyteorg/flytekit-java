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

import org.flyte.examples.SubWorkflow.Input;
import org.flyte.examples.SumTask.SumInput;
import org.flyte.examples.SumTask.SumOutput;
import org.flyte.flytekit.jackson.JacksonSdkType;
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
    JacksonSdkType<Input> sdkType = JacksonSdkType.of(Input.class);
    System.out.println(sdkType.toLiteralMap(SubWorkflow.Input.create(1, 2)));
    SdkTestingExecutor.Result result =
        SdkTestingExecutor.of(new UberWorkflow())
            .withFixedInput("a", 1)
            .withFixedInput("b", 2)
            .withFixedInput("c", 3)
            .withFixedInput("d", 4)
            // Deliberately mock with absurd values to make sure that we are not picking the
            // SumTask implementation
            .withWorkflowOutput(
                new SubWorkflow(),
                JacksonSdkType.of(SubWorkflow.Input.class),
                SubWorkflow.Input.create(1L, 2L),
                JacksonSdkType.of(SubWorkflow.Output.class),
                SubWorkflow.Output.create(5L))
            .withWorkflowOutput(
                new SubWorkflow(),
                JacksonSdkType.of(SubWorkflow.Input.class),
                SubWorkflow.Input.create(5L, 3L),
                JacksonSdkType.of(SubWorkflow.Output.class),
                SubWorkflow.Output.create(10L))
            .withTaskOutput(new SumTask(), SumInput.create(10L, 4L), SumOutput.create(15L))
            .execute();

    assertEquals(15L, result.getIntegerOutput("total"));
  }
}
