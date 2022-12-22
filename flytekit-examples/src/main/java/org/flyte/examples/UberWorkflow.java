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
import org.flyte.flytekit.NopOutputTransformer;
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkWorkflow;
import org.flyte.flytekit.SdkWorkflowBuilder;

@AutoService(SdkWorkflow.class)
public class UberWorkflow extends SdkWorkflow<NopOutputTransformer> {

  @Override
  public void expand(SdkWorkflowBuilder builder) {
    SdkBindingData a = builder.inputOfInteger("a");
    SdkBindingData b = builder.inputOfInteger("b");
    SdkBindingData c = builder.inputOfInteger("c");
    SdkBindingData d = builder.inputOfInteger("d");
    SdkBindingData ab =
        builder
            .apply("sub-1", new SubWorkflow().withInput("left", a).withInput("right", b))
            .getOutput("result");
    SdkBindingData abc =
        builder
            .apply("sub-2", new SubWorkflow().withInput("left", ab).withInput("right", c))
            .getOutput("result");
    SdkBindingData abcd = builder.apply("post-sum", SumTask.of(abc, d)).getOutput("c");
    builder.output("total", abcd);
  }
}
