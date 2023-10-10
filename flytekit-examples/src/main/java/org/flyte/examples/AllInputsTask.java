/*
 * Copyright 2023 Flyte Authors.
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
import java.util.List;
import java.util.Map;
import org.flyte.api.v1.Blob;
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkRunnableTask;
import org.flyte.flytekit.jackson.JacksonSdkType;

@AutoService(SdkRunnableTask.class)
public class AllInputsTask
    extends SdkRunnableTask<AllInputsTask.AutoAllInputsInput, AllInputsTask.AutoAllInputsOutput> {
  public AllInputsTask() {
    super(
        JacksonSdkType.of(AutoAllInputsInput.class), JacksonSdkType.of(AutoAllInputsOutput.class));
  }

  @AutoValue
  public abstract static class AutoAllInputsInput {
    public abstract SdkBindingData<Long> i();

    public abstract SdkBindingData<Double> f();

    public abstract SdkBindingData<String> s();

    public abstract SdkBindingData<Boolean> b();

    public abstract SdkBindingData<Instant> t();

    public abstract SdkBindingData<Duration> d();

    public abstract SdkBindingData<Blob> blob();

    public abstract SdkBindingData<List<String>> l();

    public abstract SdkBindingData<Map<String, String>> m();

    public abstract SdkBindingData<List<String>> emptyList();

    public abstract SdkBindingData<Map<String, Long>> emptyMap();

    public static AutoAllInputsInput create(
        SdkBindingData<Long> i,
        SdkBindingData<Double> f,
        SdkBindingData<String> s,
        SdkBindingData<Boolean> b,
        SdkBindingData<Instant> t,
        SdkBindingData<Duration> d,
        SdkBindingData<Blob> blob,
        SdkBindingData<List<String>> l,
        SdkBindingData<Map<String, String>> m,
        SdkBindingData<List<String>> emptyList,
        SdkBindingData<Map<String, Long>> emptyMap) {
      return new AutoValue_AllInputsTask_AutoAllInputsInput(
          i, f, s, b, t, d, blob, l, m, emptyList, emptyMap);
    }
  }

  @AutoValue
  public abstract static class AutoAllInputsOutput {

    public abstract SdkBindingData<Long> i();

    public abstract SdkBindingData<Double> f();

    public abstract SdkBindingData<String> s();

    public abstract SdkBindingData<Boolean> b();

    public abstract SdkBindingData<Instant> t();

    public abstract SdkBindingData<Duration> d();

    public abstract SdkBindingData<Blob> blob();

    public abstract SdkBindingData<List<String>> l();

    public abstract SdkBindingData<Map<String, String>> m();

    public abstract SdkBindingData<List<String>> emptyList();

    public abstract SdkBindingData<Map<String, Long>> emptyMap();

    public static AutoAllInputsOutput create(
        SdkBindingData<Long> i,
        SdkBindingData<Double> f,
        SdkBindingData<String> s,
        SdkBindingData<Boolean> b,
        SdkBindingData<Instant> t,
        SdkBindingData<Duration> d,
        SdkBindingData<Blob> blob,
        SdkBindingData<List<String>> l,
        SdkBindingData<Map<String, String>> m,
        SdkBindingData<List<String>> emptyList,
        SdkBindingData<Map<String, Long>> emptyMap) {
      return new AutoValue_AllInputsTask_AutoAllInputsOutput(
          i, f, s, b, t, d, blob, l, m, emptyList, emptyMap);
    }
  }

  @Override
  public AutoAllInputsOutput run(AutoAllInputsInput input) {
    return AutoAllInputsOutput.create(
        input.i(),
        input.f(),
        input.s(),
        input.b(),
        input.t(),
        input.d(),
        input.blob(),
        input.l(),
        input.m(),
        input.emptyList(),
        input.emptyMap());
  }

  @Override
  public boolean isCached() {
    return true;
  }

  @Override
  public String getCacheVersion() {
    return "1";
  }

  @Override
  public boolean isCacheSerializable() {
    return true;
  }
}
