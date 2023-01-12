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

// TODO: Enable when Structs are supported
/** Example Flyte task that takes a name as the input and outputs a simple greeting message. */
/*
@AutoService(SdkRunnableTask.class)
public class SimpleStructTask
    extends SdkRunnableTask<SimpleStructTask.Input, SimpleStructTask.Output> {
  public SimpleStructTask() {
    super(JacksonSdkType.of(Input.class), JacksonSdkType.of(Output.class));
  }

  public static SdkTransform<?> of(SdkBindingData<?> struct) {
    return new SimpleStructTask().withInput("struct", struct);
  }

  @AutoValue
  public abstract static class Input {
    public abstract Struct in();
  }

  @AutoValue
  public abstract static class Output {
    public abstract Struct out();

    public static Output create(Struct struct) {
      return new AutoValue_SimpleStructTask_Output(struct);
    }
  }

  @Override
  public Output run(Input input) {
    return Output.create(input.in());
  }
}
*/
