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

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkWorkflow;
import org.flyte.flytekit.SdkWorkflowBuilder;
import org.flyte.flytekit.jackson.JacksonSdkType;

@AutoService(SdkWorkflow.class)
public class SubWorkflow extends SdkWorkflow<SubWorkflow.Output> {

  public SubWorkflow() {
    super(JacksonSdkType.of(SubWorkflow.Output.class));
  }

  @Override
  public void expand(SdkWorkflowBuilder builder) {
    SdkBindingData<Long> left = builder.inputOfInteger("left");
    SdkBindingData<Long> right = builder.inputOfInteger("right");
    SdkBindingData<Long> result = builder.apply("sum", SumTask.of(left, right)).getOutputs().c();
    builder.output("result", result);
  }

  // TODO verify why this was here. It is not used
  @AutoValue
  public abstract static class Input {
    abstract long left();

    abstract long right();

    public static Input create(long left, long right) {
      return new AutoValue_SubWorkflow_Input(left, right);
    }
  }

  @AutoValue
  public abstract static class Output {
    abstract SdkBindingData<Long> result();

    public static Output create(long result) {
      return new AutoValue_SubWorkflow_Output(SdkBindingData.ofInteger(result));
    }
  }
}
