/*
 * Copyright 2025 Flyte Authors.
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

import static org.flyte.flytekit.SdkBindingDataFactory.of;
import static org.junit.jupiter.api.Assertions.*;

import org.flyte.flytekit.jackson.JacksonSdkType;
import org.junit.jupiter.api.Test;

public class SdkDynamicWorkflowTaskDelegatingWorkflowTest {
  @Test
  public void testDelegatingWorkflow_EvenValues() {
    int expected = 6;

    SumIfEvenDynamicWorkflowTask.Output output =
        SdkTestingExecutor.of(
                new SumIfEvenDynamicWorkflowTask(),
                SumIfEvenDynamicWorkflowTask.Input.create(of(2), of(4)),
                JacksonSdkType.of(SumIfEvenDynamicWorkflowTask.Output.class))
            .withTaskOutput(
                new SumTask(),
                SumTask.SumInput.create(of(2), of(4)),
                SumTask.SumOutput.create(of(expected)))
            .execute()
            .getOutputAs(JacksonSdkType.of(SumIfEvenDynamicWorkflowTask.Output.class));
    assertEquals(expected, output.c().get());
  }

  @Test
  public void testDelegatingWorkflow_Odd() {
    int expected = 0;

    SumIfEvenDynamicWorkflowTask.Output output =
        SdkTestingExecutor.of(
                new SumIfEvenDynamicWorkflowTask(),
                SumIfEvenDynamicWorkflowTask.Input.create(of(1), of(4)),
                JacksonSdkType.of(SumIfEvenDynamicWorkflowTask.Output.class))
            .withTaskOutput(
                new SumTask(),
                SumTask.SumInput.create(of(0), of(0)),
                SumTask.SumOutput.create(of(expected)))
            .execute()
            .getOutputAs(JacksonSdkType.of(SumIfEvenDynamicWorkflowTask.Output.class));
    assertEquals(expected, output.c().get());
  }
}
