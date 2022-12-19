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
import org.flyte.flytekit.NopNamedOutput;
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkWorkflow;
import org.flyte.flytekit.SdkWorkflowBuilder;

@AutoService(SdkWorkflow.class)
public class SubWorkflow extends SdkWorkflow<NopNamedOutput> {

  @Override
  public void expand(SdkWorkflowBuilder builder) {
    SdkBindingData left = builder.inputOfInteger("left");
    SdkBindingData right = builder.inputOfInteger("right");
    SdkBindingData result = builder.apply("sum", SumTask.of(left, right)).getOutput("c");
    builder.output("result", result);
  }

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
    abstract long result();

    public static Output create(long result) {
      return new AutoValue_SubWorkflow_Output(result);
    }
  }
}
