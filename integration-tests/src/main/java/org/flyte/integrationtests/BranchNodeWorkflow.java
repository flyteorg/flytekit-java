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
package org.flyte.integrationtests;

import static org.flyte.flytekit.SdkConditions.eq;
import static org.flyte.flytekit.SdkConditions.gt;
import static org.flyte.flytekit.SdkConditions.lt;
import static org.flyte.flytekit.SdkConditions.when;

import com.google.auto.service.AutoService;
import org.flyte.flytekit.NopOutputTransformer;
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkCondition;
import org.flyte.flytekit.SdkWorkflow;
import org.flyte.flytekit.SdkWorkflowBuilder;
import org.flyte.flytekit.jackson.JacksonSdkType;

@AutoService(SdkWorkflow.class)
public class BranchNodeWorkflow extends SdkWorkflow<ConstStringTask.Output> {
  public BranchNodeWorkflow() {
    super(JacksonSdkType.of(ConstStringTask.Output.class));
  }

  @Override
  public void expand(SdkWorkflowBuilder builder) {
    SdkBindingData<Long> a = builder.inputOfInteger("a");
    SdkBindingData<Long> b = builder.inputOfInteger("b");
    SdkBindingData<Long> c = builder.inputOfInteger("c");
    SdkBindingData<Long> d = builder.inputOfInteger("d");

    SdkCondition<ConstStringTask.Output> condition =
        when(
                "a-equal-b",
                eq(a, b),
                when("c-equal-d", eq(c, d), ConstStringTask.of("a == b && c == d"))
                    .when("c-greater-d", gt(c, d), ConstStringTask.of("a == b && c > d"))
                    .when("c-less-d", lt(c, d), ConstStringTask.of("a == b && c < d")))
            .when(
                "a-less-b",
                lt(a, b),
                when("c-equal-d", eq(c, d), ConstStringTask.of("a < b && c == d"))
                    .when("c-greater-d", gt(c, d), ConstStringTask.of("a < b && c > d"))
                    .when("c-less-d", lt(c, d), ConstStringTask.of("a < b && c < d")))
            .when(
                "a-greater-b",
                gt(a, b),
                when("c-equal-d", eq(c, d), ConstStringTask.of("a > b && c == d"))
                    .when("c-greater-d", gt(c, d), ConstStringTask.of("a > b && c > d"))
                    .when("c-less-d", lt(c, d), ConstStringTask.of("a > b && c < d")));

    SdkBindingData<String> value = builder.apply("condition", condition).getOutputs().value();

    builder.output("value", value);
  }
}
