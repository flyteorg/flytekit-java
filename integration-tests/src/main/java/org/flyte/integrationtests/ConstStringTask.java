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
package org.flyte.integrationtests;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import org.flyte.flytekit.NopOutputTransformer;
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkRunnableTask;
import org.flyte.flytekit.SdkTransform;
import org.flyte.flytekit.jackson.JacksonSdkType;

@AutoService(SdkRunnableTask.class)
public class ConstStringTask
    extends SdkRunnableTask<ConstStringTask.Input, ConstStringTask.Output> {
  private static final long serialVersionUID = 42L;

  @AutoValue
  abstract static class Input {
    abstract SdkBindingData<String> value();
  }

  @AutoValue
  abstract static class Output {
    abstract SdkBindingData<String> value();

    public static Output create(String value) {
      return new AutoValue_ConstStringTask_Output(value);
    }
  }

  public ConstStringTask() {
    super(JacksonSdkType.of(Input.class), JacksonSdkType.of(Output.class));
  }

  public static SdkTransform of(String value) {
    return new ConstStringTask().withInput("value", SdkBindingData.ofString(value));
  }

  @Override
  public Output run(Input input) {
    return Output.create(input.value());
  }
}
