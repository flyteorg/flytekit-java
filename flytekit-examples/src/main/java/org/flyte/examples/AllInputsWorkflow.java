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
import java.util.List;
import java.util.Map;
import org.flyte.examples.AllInputsTask.AutoAllInputsOutput;
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkNode;
import org.flyte.flytekit.SdkWorkflow;
import org.flyte.flytekit.SdkWorkflowBuilder;
import org.flyte.flytekit.jackson.JacksonSdkType;

@AutoService(SdkWorkflow.class)
public class AllInputsWorkflow extends SdkWorkflow<AllInputsWorkflow.AllInputsWorkflowOutput> {

  public AllInputsWorkflow() {
    super(JacksonSdkType.of(AllInputsWorkflow.AllInputsWorkflowOutput.class));
  }

  @Override
  public void expand(SdkWorkflowBuilder builder) {

    SdkNode<AutoAllInputsOutput> apply =
        builder.apply(
            "all-inputs",
            AllInputsTask.of(
                SdkBindingData.ofInteger(1L),
                SdkBindingData.ofFloat(2),
                SdkBindingData.ofString("test"),
                SdkBindingData.ofBoolean(true),
                SdkBindingData.ofDatetime(Instant.EPOCH),
                SdkBindingData.ofDuration(Duration.ofDays(1L)),
                SdkBindingData.ofCollection(Arrays.asList("foo", "bar"), SdkBindingData::ofString),
                SdkBindingData.ofMap(Map.of("test", "test"), SdkBindingData::ofString)));
    AllInputsTask.AutoAllInputsOutput outputs = apply.getOutputs();

    builder.output("i", outputs.i(), "Integer value");
    builder.output("f", outputs.f(), "Double value");
    builder.output("s", outputs.s(), "String value");
    builder.output("b", outputs.b(), "Boolean value");
    builder.output("t", outputs.t(), "Instant value");
    builder.output("d", outputs.d(), "Duration value");
    builder.output("l", outputs.l(), "List value");
    builder.output("m", outputs.m(), "Map value");
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

    public static AllInputsWorkflow.AllInputsWorkflowOutput create(
        long i,
        Double f,
        String s,
        boolean b,
        Instant t,
        Duration d,
        List<String> l,
        Map<String, String> m) {
      return new AutoValue_AllInputsWorkflow_AllInputsWorkflowOutput(
          SdkBindingData.ofInteger(i),
          SdkBindingData.ofFloat(f),
          SdkBindingData.ofString(s),
          SdkBindingData.ofBoolean(b),
          SdkBindingData.ofDatetime(t),
          SdkBindingData.ofDuration(d),
          SdkBindingData.ofCollection(l, SdkBindingData::ofString),
          SdkBindingData.ofMap(m, SdkBindingData::ofString));
    }
  }
}
