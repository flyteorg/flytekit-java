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
import java.util.List;
import java.util.Map;
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkRunnableTask;
import org.flyte.flytekit.SdkTransform;
import org.flyte.flytekit.jackson.JacksonSdkType;

@AutoService(SdkRunnableTask.class)
public class AllInputsTask
    extends SdkRunnableTask<AllInputsTask.AutoAllInputsInput, AllInputsTask.AutoAllInputsOutput> {
  public AllInputsTask() {
    super(
        JacksonSdkType.of(AutoAllInputsInput.class), JacksonSdkType.of(AutoAllInputsOutput.class));
  }

  public static SdkTransform<AllInputsTask.AutoAllInputsOutput> of(
      SdkBindingData<Long> i,
      SdkBindingData<Double> f,
      SdkBindingData<String> s,
      SdkBindingData<Boolean> b,
      SdkBindingData<Instant> t,
      SdkBindingData<Duration> d,
      SdkBindingData<List<String>> l,
      SdkBindingData<Map<String, String>> m) {
    return new AllInputsTask()
        .withInput("i", i) // this still sucks
        .withInput("f", f)
        .withInput("s", s)
        .withInput("b", b)
        .withInput("t", t)
        .withInput("d", d)
        .withInput("l", l)
        .withInput("m", m);
  }

  @AutoValue
  public abstract static class AutoAllInputsInput {
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

    public static AutoAllInputsInput create(
        SdkBindingData<Long> i,
        SdkBindingData<Double> f,
        SdkBindingData<String> s,
        SdkBindingData<Boolean> b,
        SdkBindingData<Instant> t,
        SdkBindingData<Duration> d,
        // Blob blob,
        SdkBindingData<List<String>> l,
        SdkBindingData<Map<String, String>> m) {
      return new AutoValue_AllInputsTask_AutoAllInputsInput(i, f, s, b, t, d, l, m);
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

    // TODO add blobs to sdkbinding data
    // public abstract SdkBindingData<Blob> blob();

    public abstract SdkBindingData<List<String>> l();

    public abstract SdkBindingData<Map<String, String>> m();

    public static AutoAllInputsOutput create(
        long i,
        Double f,
        String s,
        boolean b,
        Instant t,
        Duration d,
        List<String> l,
        Map<String, String> m) {
      return new AutoValue_AllInputsTask_AutoAllInputsOutput(
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

  @Override
  public AutoAllInputsOutput run(AutoAllInputsInput input) {
    return AutoAllInputsOutput.create(
        input.i().get(),
        input.f().get(),
        input.s().get(),
        input.b().get(),
        input.t().get(),
        input.d().get(),
        input.l().get(),
        input.m().get());
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
