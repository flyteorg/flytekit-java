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
import java.util.List;
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkBindingDataFactory;
import org.flyte.flytekit.SdkLiteralTypes;
import org.flyte.flytekit.SdkNode;
import org.flyte.flytekit.SdkTypes;
import org.flyte.flytekit.SdkWorkflow;
import org.flyte.flytekit.SdkWorkflowBuilder;
import org.flyte.flytekit.jackson.JacksonSdkType;

@AutoService(SdkWorkflow.class)
public class ListWorkflow extends SdkWorkflow<Void, ListTask.Output> {
  public ListWorkflow() {
    super(SdkTypes.nulls(), JacksonSdkType.of(ListTask.Output.class));
  }

  @Override
  public ListTask.Output expand(SdkWorkflowBuilder builder, Void noInput) {
    SdkNode<TestUnaryIntegerOutput> sum1 =
        builder.apply(
            "sum-1",
            new SumTask(),
            SumTask.Input.create(SdkBindingDataFactory.of(1), SdkBindingDataFactory.of(2)));
    SdkNode<TestUnaryIntegerOutput> sum2 =
        builder.apply(
            "sum-2",
            new SumTask(),
            SumTask.Input.create(SdkBindingDataFactory.of(3), SdkBindingDataFactory.of(4)));

    SdkBindingData<List<Long>> list =
        SdkBindingDataFactory.ofBindingCollection(
            SdkLiteralTypes.integers(), List.of(sum1.getOutputs().o(), sum2.getOutputs().o()));

    SdkNode<ListTask.Output> list1 =
        builder.apply("list-1", new ListTask(), ListTask.Input.create(list));

    return ListTask.Output.create(list1.getOutputs().list());
  }
}
