/*
 * Copyright 2020-2023 Flyte Authors.
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

import com.google.auto.service.AutoService;
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkWorkflow;
import org.flyte.flytekit.SdkWorkflowBuilder;
import org.flyte.flytekit.jackson.JacksonSdkType;

@AutoService(SdkWorkflow.class)
public class InnerSubWorkflow extends SdkWorkflow<SumTask.Input, TestUnaryIntegerOutput> {
  public InnerSubWorkflow() {
    super(JacksonSdkType.of(SumTask.Input.class), JacksonSdkType.of(TestUnaryIntegerOutput.class));
  }

  @Override
  public TestUnaryIntegerOutput expand(SdkWorkflowBuilder builder, SumTask.Input input) {
    SdkBindingData<Long> c =
        builder
            .apply("inner-sum-a-b", new SumTask(), SumTask.Input.create(input.a(), input.b()))
            .getOutputs()
            .o();

    return TestUnaryIntegerOutput.create(c);
  }
}
