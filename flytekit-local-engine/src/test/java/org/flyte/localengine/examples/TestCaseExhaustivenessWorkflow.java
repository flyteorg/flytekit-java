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
package org.flyte.localengine.examples;

import static org.flyte.flytekit.SdkBindingData.ofInteger;
import static org.flyte.flytekit.SdkConditions.eq;
import static org.flyte.flytekit.SdkConditions.when;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkRunnableTask;
import org.flyte.flytekit.SdkTransform;
import org.flyte.flytekit.SdkWorkflow;
import org.flyte.flytekit.SdkWorkflowBuilder;
import org.flyte.flytekit.jackson.JacksonSdkType;

// 3X+1
@AutoService(SdkWorkflow.class)
public class TestCaseExhaustivenessWorkflow extends SdkWorkflow {

  @Override
  public void expand(SdkWorkflowBuilder builder) {
    SdkBindingData x = builder.inputOfInteger("x");
    SdkBindingData nextX =
        builder
            .apply(
                "decide",
                when("eq_1", eq(ofInteger(1L), x), NoOp.of(x))
                    .when("eq_2", eq(ofInteger(2L), x), NoOp.of(x)))
            .getOutput("x");

    builder.output("nextX", nextX);
  }

  @AutoService(SdkRunnableTask.class)
  public static class NoOp extends SdkRunnableTask<NoOpInput, NoOpOutput> {

    public NoOp() {
      super(JacksonSdkType.of(NoOpInput.class), JacksonSdkType.of(NoOpOutput.class));
    }

    @Override
    public NoOpOutput run(NoOpInput input) {
      return NoOpOutput.create(input.x());
    }

    static SdkTransform of(SdkBindingData x) {
      return new NoOp().withInput("x", x);
    }
  }

  @AutoValue
  public abstract static class NoOpInput {
    abstract long x();

    public static NoOpInput create(long x) {
      return new AutoValue_TestCaseExhaustivenessWorkflow_NoOpInput(x);
    }
  }

  @AutoValue
  public abstract static class NoOpOutput {
    abstract long x();

    public static NoOpOutput create(long x) {
      return new AutoValue_TestCaseExhaustivenessWorkflow_NoOpOutput(x);
    }
  }
}
