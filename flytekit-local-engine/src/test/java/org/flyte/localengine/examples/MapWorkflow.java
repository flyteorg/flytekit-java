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
import com.google.auto.value.AutoValue;
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkNode;
import org.flyte.flytekit.SdkWorkflow;
import org.flyte.flytekit.SdkWorkflowBuilder;
import org.flyte.flytekit.jackson.JacksonSdkType;

import java.util.Map;

@AutoService(SdkWorkflow.class)
public class MapWorkflow extends SdkWorkflow<MapWorkflow.Output> {
  public MapWorkflow() {
    super(JacksonSdkType.of(MapWorkflow.Output.class));
  }

  @AutoValue
  public abstract static class Output {

    public abstract SdkBindingData<Map<String, String>> map();
    public static MapWorkflow.Output create(Map<String, String> map) {
      return new AutoValue_MapWorkflow_Output(SdkBindingData.ofMap(map, SdkBindingData::ofString));
    }

  }

  @Override
  public void expand(SdkWorkflowBuilder builder) {
    SdkBindingData<Long> sum1 =
            builder.apply("sum-1", new SumTask().withInput("a", 1).withInput("b", 2))
            .getOutputs().o();

    SdkBindingData<Long> sum2 =
        builder.apply("sum-2", new SumTask().withInput("a", 3).withInput("b", 4))
                .getOutputs().o();

    SdkBindingData<Map<String, Long>> map =
        SdkBindingData.ofBindingMap(Map.of("e", sum1,"f", sum2));

    SdkNode<MapTask.Output> map1 = builder.apply("map-1", new MapTask().withInput("map", map));

    builder.output("map", map1.getOutputs().map());
  }
}
