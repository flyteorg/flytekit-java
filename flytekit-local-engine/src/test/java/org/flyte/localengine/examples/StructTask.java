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
public class StructTask extends SdkRunnableTask<StructTask.Input, StructTask.Output> {

  public StructTask() {
    super(JacksonSdkType.of(Input.class), JacksonSdkType.of(Output.class));
  }

  @Override
  public Output run(Input input) {
    return Output.create(
        CustomStruct.create(
            input.struct_data().some_key_1() + "-output", input.struct_data().some_key_2()));
  }

  @AutoValue
  public abstract static class CustomStruct {
    public abstract String some_key_1();

    public abstract Boolean some_key_2();

    public static CustomStruct create(String some_key_1, Boolean some_key_2) {
      return new AutoValue_StructTask_CustomStruct(some_key_1, some_key_2);
    }
  }

  @AutoValue
  public abstract static class Input {

    public abstract String some_string();

    public abstract CustomStruct struct_data();

    public static Input create(String some_string, CustomStruct struct) {
      return new AutoValue_StructTask_Input(some_string, struct);
    }
  }

  @AutoValue
  public abstract static class Output {

    public abstract CustomStruct output_struct_data();

    public static Output create(CustomStruct struct) {
      return new AutoValue_StructTask_Output(struct);
    }
  }
}
