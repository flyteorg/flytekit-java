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

import com.google.auto.service.AutoService;
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkNode;
import org.flyte.flytekit.SdkWorkflow;
import org.flyte.flytekit.SdkWorkflowBuilder;
import org.flyte.localengine.ImmutableMap;

import java.util.Map;

@AutoService(SdkWorkflow.class)
public class MapWorkflow extends SdkWorkflow {
    public MapWorkflow() {
        super(outputType);
    }

    @Override
  public void expand(SdkWorkflowBuilder builder) {
    SdkNode<TestUnaryIntegerOutput> sum1 = builder.apply("sum-1", new SumTask().withInput("a", 1).withInput("b", 2));
    SdkNode<TestUnaryIntegerOutput> sum2 = builder.apply("sum-2", new SumTask().withInput("a", 3).withInput("b", 4));

    SdkBindingData<Map<String, TestUnaryIntegerOutput>> map =
        SdkBindingData.ofBindingMap(
            ImmutableMap.of(
                "e", sum1.getOutput("c"),
                "f", sum2.getOutput("c")));

    SdkNode<MapTask.Output> map1 = builder.apply("map-1", new MapTask().withInput("map", map));

    builder.output("map", map1.getOutputs().map());
  }
}
