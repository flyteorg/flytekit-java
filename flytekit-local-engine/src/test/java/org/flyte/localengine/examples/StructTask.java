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

import com.google.auto.value.AutoValue;
import org.flyte.flytekit.SdkRunnableTask;
import org.flyte.flytekit.jackson.JacksonSdkType;

// @AutoService(SdkRunnableTask.class)
public class StructTask extends SdkRunnableTask<StructTask.Input, StructTask.Output> {
  private static final long serialVersionUID = -3990613929313621336L;

  public StructTask() {
    super(JacksonSdkType.of(Input.class), JacksonSdkType.of(Output.class));
  }

  @Override
  public Output run(Input input) {
    return Output.create(
        CustomStruct.create(
            input.structData().get().someKey1() + "-output", input.structData().get().someKey2()));
  }

  @AutoValue
  public abstract static class CustomStruct {
    public abstract String someKey1();

    public abstract Boolean someKey2();

    public static CustomStruct create(String someKey1, Boolean someKey2) {
      return new AutoValue_StructTask_CustomStruct(someKey1, someKey2);
    }
  }

  @AutoValue
  public abstract static class Input {

    public abstract SdkBindingData<String> someString();

    public abstract SdkBindingData<CustomStruct> structData();

    public static Input create(String someString, CustomStruct struct) {
      return new AutoValue_StructTask_Input(SdkBindingData.ofString(someString), SdkBindingData.ofStruct(struct));
    }
  }

  @AutoValue
  public abstract static class Output {

    public abstract CustomStruct outputStructData();

    public static Output create(CustomStruct struct) {
      return new AutoValue_StructTask_Output(struct);
    }
  }
}
*/
