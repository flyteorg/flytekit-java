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
import com.google.auto.value.AutoValue;
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkBindingDataFactory;
import org.flyte.flytekit.SdkCondition;
import org.flyte.flytekit.SdkWorkflow;
import org.flyte.flytekit.SdkWorkflowBuilder;
import org.flyte.flytekit.jackson.JacksonSdkType;

@AutoService(SdkWorkflow.class)
public class BranchNodeWorkflow
    extends SdkWorkflow<BranchNodeWorkflow.Input, ConstStringTask.Output> {

  @AutoValue
  abstract static class Input {
    abstract SdkBindingData<Long> a();

    abstract SdkBindingData<Long> b();

    abstract SdkBindingData<Long> c();

    abstract SdkBindingData<Long> d();

    public static BranchNodeWorkflow.Input create(
        SdkBindingData<Long> a,
        SdkBindingData<Long> b,
        SdkBindingData<Long> c,
        SdkBindingData<Long> d) {
      return new AutoValue_BranchNodeWorkflow_Input(a, b, c, d);
    }
  }

  public BranchNodeWorkflow() {
    super(
        JacksonSdkType.of(BranchNodeWorkflow.Input.class),
        JacksonSdkType.of(ConstStringTask.Output.class));
  }

  @Override
  public ConstStringTask.Output expand(SdkWorkflowBuilder builder, BranchNodeWorkflow.Input input) {
    SdkBindingData<Long> a = input.a();
    SdkBindingData<Long> b = input.b();
    SdkBindingData<Long> c = input.c();
    SdkBindingData<Long> d = input.d();

    SdkCondition<ConstStringTask.Output> condition =
        when(
                "a-equal-b",
                eq(a, b),
                when(
                        "c-equal-d",
                        eq(c, d),
                        new ConstStringTask(),
                        ConstStringTask.Input.create(SdkBindingDataFactory.of("a == b && c == d")))
                    .when(
                        "c-greater-d",
                        gt(c, d),
                        new ConstStringTask(),
                        ConstStringTask.Input.create(SdkBindingDataFactory.of("a == b && c > d")))
                    .when(
                        "c-less-d",
                        lt(c, d),
                        new ConstStringTask(),
                        ConstStringTask.Input.create(SdkBindingDataFactory.of("a == b && c < d"))))
            .when(
                "a-less-b",
                lt(a, b),
                when(
                        "c-equal-d",
                        eq(c, d),
                        new ConstStringTask(),
                        ConstStringTask.Input.create(SdkBindingDataFactory.of("a < b && c == d")))
                    .when(
                        "c-greater-d",
                        gt(c, d),
                        new ConstStringTask(),
                        ConstStringTask.Input.create(SdkBindingDataFactory.of("a < b && c > d")))
                    .when(
                        "c-less-d",
                        lt(c, d),
                        new ConstStringTask(),
                        ConstStringTask.Input.create(SdkBindingDataFactory.of("a < b && c < d"))))
            .when(
                "a-greater-b",
                gt(a, b),
                when(
                        "c-equal-d",
                        eq(c, d),
                        new ConstStringTask(),
                        ConstStringTask.Input.create(SdkBindingDataFactory.of("a > b && c == d")))
                    .when(
                        "c-greater-d",
                        gt(c, d),
                        new ConstStringTask(),
                        ConstStringTask.Input.create(SdkBindingDataFactory.of("a > b && c > d")))
                    .when(
                        "c-less-d",
                        lt(c, d),
                        new ConstStringTask(),
                        ConstStringTask.Input.create(SdkBindingDataFactory.of("a > b && c < d"))));

    SdkBindingData<String> value = builder.apply("condition", condition).getOutputs().value();

    return ConstStringTask.Output.create(value);
  }
}
