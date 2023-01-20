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
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkWorkflow;
import org.flyte.flytekit.SdkWorkflowBuilder;
import org.flyte.flytekit.jackson.JacksonSdkType;

@AutoService(SdkWorkflow.class)
public class UberWorkflow extends SdkWorkflow<SubWorkflow.Output> {

  public UberWorkflow() {
    super(JacksonSdkType.of(SubWorkflow.Output.class));
  }

  @Override
  public void expand(SdkWorkflowBuilder builder) {
    SdkBindingData<Long> a = builder.inputOfInteger("a");
    SdkBindingData<Long> b = builder.inputOfInteger("b");
    SdkBindingData<Long> c = builder.inputOfInteger("c");
    SdkBindingData<Long> d = builder.inputOfInteger("d");
    SdkBindingData<Long> ab =
        builder
            .apply("sub-1", new SubWorkflow().withInput("left", a).withInput("right", b))
            .getOutputs()
            .result();
    SdkBindingData<Long> abc =
        builder
            .apply("sub-2", new SubWorkflow().withInput("left", ab).withInput("right", c))
            .getOutputs()
            .result();
    SdkBindingData<Long> abcd = builder.apply("post-sum", SumTask.of(abc, d)).getOutputs().c();
    builder.output("result", abcd);
  }
}
