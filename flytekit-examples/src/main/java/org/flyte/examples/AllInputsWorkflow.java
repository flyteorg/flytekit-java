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
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.flyte.examples.AllInputsTask.AutoAllInputsOutput;
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkNode;
import org.flyte.flytekit.SdkTypes;
import org.flyte.flytekit.SdkWorkflow;
import org.flyte.flytekit.SdkWorkflowBuilder;
import org.flyte.flytekit.jackson.JacksonSdkType;

@AutoService(SdkWorkflow.class)
public class AllInputsWorkflow
    extends SdkWorkflow<Void, AllInputsWorkflow.AllInputsWorkflowOutput> {

  public AllInputsWorkflow() {
    super(SdkTypes.nulls(), JacksonSdkType.of(AllInputsWorkflow.AllInputsWorkflowOutput.class));
  }

  @Override
  public void expand(SdkWorkflowBuilder builder) {

    Instant someInstant = Instant.parse("2023-01-16T00:00:00Z");

    SdkNode<AutoAllInputsOutput> apply =
        builder.apply(
            "all-inputs",
            new AllInputsTask(),
            AllInputsTask.AutoAllInputsInput.create(
                SdkBindingData.ofInteger(1L),
                SdkBindingData.ofFloat(2),
                SdkBindingData.ofString("test"),
                SdkBindingData.ofBoolean(true),
                SdkBindingData.ofDatetime(someInstant),
                SdkBindingData.ofDuration(Duration.ofDays(1L)),
                SdkBindingData.ofStringCollection(Arrays.asList("foo", "bar")),
                SdkBindingData.ofStringMap(Map.of("test", "test")),
                SdkBindingData.ofStringCollection(Collections.emptyList()),
                SdkBindingData.ofIntegerMap(Collections.emptyMap())));

    AllInputsTask.AutoAllInputsOutput outputs = apply.getOutputs();

    builder.output("i", outputs.i(), "Integer value");
    builder.output("f", outputs.f(), "Double value");
    builder.output("s", outputs.s(), "String value");
    builder.output("b", outputs.b(), "Boolean value");
    builder.output("t", outputs.t(), "Instant value");
    builder.output("d", outputs.d(), "Duration value");
    builder.output("l", outputs.l(), "List value");
    builder.output("m", outputs.m(), "Map value");
    builder.output("emptyList", outputs.emptyList(), "Empty list value");
    builder.output("emptyMap", outputs.emptyMap(), "Empty map value");
  }

  @AutoValue
  public abstract static class AllInputsWorkflowOutput {

    public abstract SdkBindingData<Long> i();

    public abstract SdkBindingData<Double> f();

    public abstract SdkBindingData<String> s();

    public abstract SdkBindingData<Boolean> b();

    public abstract SdkBindingData<Instant> t();

    public abstract SdkBindingData<Duration> d();

    // TODO add blobs to sdkbinding data
    // public abstract SdkBindingData<Blob> blob();

    public abstract SdkBindingData<List<String>> l();

    public abstract SdkBindingData<Map<String, String>> m();

    public abstract SdkBindingData<List<String>> emptyList();

    public abstract SdkBindingData<Map<String, Long>> emptyMap();

    public static AllInputsWorkflow.AllInputsWorkflowOutput create(
        SdkBindingData<Long> i,
        SdkBindingData<Double> f,
        SdkBindingData<String> s,
        SdkBindingData<Boolean> b,
        SdkBindingData<Instant> t,
        SdkBindingData<Duration> d,
        SdkBindingData<List<String>> l,
        SdkBindingData<Map<String, String>> m,
        SdkBindingData<List<String>> emptyList,
        SdkBindingData<Map<String, Long>> emptyMap) {
      return new AutoValue_AllInputsWorkflow_AllInputsWorkflowOutput(
          i, f, s, b, t, d, l, m, emptyList, emptyMap);
    }
  }
}
