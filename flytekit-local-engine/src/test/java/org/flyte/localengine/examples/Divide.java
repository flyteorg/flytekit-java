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
public class Divide extends SdkRunnableTask<Divide.Input, Divide.Output> {

  public Divide() {
    super(JacksonSdkType.of(Divide.Input.class), JacksonSdkType.of(Divide.Output.class));
  }

  @Override
  public Output run(Input input) {
    return Output.create(input.num() / input.den());
  }

  @AutoValue
  public abstract static class Input {
    public abstract long num();

    public abstract long den();

    public static Input create(Long num, Long den) {
      return new AutoValue_Divide_Input(num, den);
    }
  }

  @AutoValue
  public abstract static class Output {

    public abstract long res();

    public static Output create(long res) {
      return new AutoValue_Divide_Output(res);
    }
  }
}
