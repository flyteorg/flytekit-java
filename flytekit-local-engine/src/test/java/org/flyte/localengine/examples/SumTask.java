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
import org.flyte.flytekit.SdkRunnableTask;
import org.flyte.flytekit.jackson.JacksonSdkType;

@AutoService(SdkRunnableTask.class)
public class SumTask extends SdkRunnableTask<SumTask.SumInput, TestUnaryIntegerOutput> {
  private static final long serialVersionUID = -7796919693971619417L;

  public SumTask() {
    super(JacksonSdkType.of(SumInput.class), new TestUnaryIntegerOutput.SdkType());
  }

  @AutoValue
  public abstract static class SumInput {
    public abstract SdkBindingData<Long> a();

    public abstract SdkBindingData<Long> b();

    public static SumInput create(long a, long b) {
      return new AutoValue_SumTask_SumInput(
          SdkBindingData.ofInteger(a), SdkBindingData.ofInteger(b));
    }
  }

  @Override
  public TestUnaryIntegerOutput run(SumInput input) {
    return TestUnaryIntegerOutput.create(
        SdkBindingData.ofInteger(input.a().get() + input.b().get()));
  }
}
