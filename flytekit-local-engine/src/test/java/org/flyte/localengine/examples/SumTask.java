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
import org.flyte.flytekit.SdkBindingDataFactory;
import org.flyte.flytekit.SdkRunnableTask;
import org.flyte.flytekit.jackson.JacksonSdkType;
import org.flyte.localengine.examples.SumTask.Input;

@AutoService(SdkRunnableTask.class)
public class SumTask extends SdkRunnableTask<Input, TestUnaryIntegerOutput> {
  private static final long serialVersionUID = -7796919693971619417L;

  public SumTask() {
    super(JacksonSdkType.of(Input.class), JacksonSdkType.of(TestUnaryIntegerOutput.class));
  }

  @AutoValue
  public abstract static class Input {
    public abstract SdkBindingData<Long> a();

    public abstract SdkBindingData<Long> b();

    public static Input create(SdkBindingData<Long> a, SdkBindingData<Long> b) {
      return new AutoValue_SumTask_Input(a, b);
    }
  }

  @Override
  public TestUnaryIntegerOutput run(Input input) {
    return TestUnaryIntegerOutput.create(
        SdkBindingDataFactory.of(input.a().get() + input.b().get()));
  }
}
