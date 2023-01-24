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

import static org.flyte.flytekit.SdkBindingData.ofInteger;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import java.util.Map;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.SimpleType;
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkNode;
import org.flyte.flytekit.SdkTypes;
import org.flyte.flytekit.SdkWorkflow;
import org.flyte.flytekit.SdkWorkflowBuilder;
import org.flyte.flytekit.jackson.JacksonSdkType;

@AutoService(SdkWorkflow.class)
public class MapWorkflow extends SdkWorkflow<Void, MapWorkflow.Output> {
  public MapWorkflow() {
    super(SdkTypes.nulls(), JacksonSdkType.of(MapWorkflow.Output.class));
  }

  @AutoValue
  public abstract static class Output {

    public abstract SdkBindingData<Map<String, String>> map();

    public static MapWorkflow.Output create(Map<String, String> map) {
      return new AutoValue_MapWorkflow_Output(SdkBindingData.ofStringMap(map));
    }
  }

  @Override
  public void expand(SdkWorkflowBuilder builder) {
    SdkBindingData<Long> sum1 =
        builder
            .apply("sum-1", new SumTask(), SumTask.Input.create(ofInteger(1), ofInteger(2)))
            .getOutputs()
            .o();

    SdkBindingData<Long> sum2 =
        builder
            .apply("sum-2", new SumTask(), SumTask.Input.create(ofInteger(3), ofInteger(4)))
            .getOutputs()
            .o();

    SdkBindingData<Map<String, Long>> map =
        SdkBindingData.ofBindingMap(
            LiteralType.ofMapValueType(LiteralType.ofSimpleType(SimpleType.INTEGER)),
            Map.of("e", sum1, "f", sum2));

    SdkNode<MapTask.Output> map1 = builder.apply("map-1", new MapTask(), MapTask.Input.create(map));

    builder.output("map", map1.getOutputs().map());
  }
}
