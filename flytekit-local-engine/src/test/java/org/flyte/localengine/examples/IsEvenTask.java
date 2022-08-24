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
import org.flyte.flytekit.SdkRunnableTask;
import org.flyte.flytekit.jackson.JacksonSdkType;

@AutoService(SdkRunnableTask.class)
public class IsEvenTask extends SdkRunnableTask<IsEvenTask.Input, IsEvenTask.Output> {

  public IsEvenTask() {
    super(JacksonSdkType.of(IsEvenTask.Input.class), JacksonSdkType.of(IsEvenTask.Output.class));
  }

  @Override
  public Output run(Input input) {
    return Output.create(input.x() % 2 == 0);
  }

  @AutoValue
  public abstract static class Input {

    public abstract Long x();

    public static Input create(Long x) {
      return new AutoValue_IsEvenTask_Input(x);
    }
  }

  @AutoValue
  public abstract static class Output {

    public abstract boolean res();

    public static Output create(boolean res) {
      return new AutoValue_IsEvenTask_Output(res);
    }
  }
}
