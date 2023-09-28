/*
 * Copyright 2020-2023 Flyte Authors
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
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkBindingDataFactory;
import org.flyte.flytekit.SdkRunnableTask;
import org.flyte.flytekit.jackson.JacksonSdkType;

/** Example Flyte task that takes a name as the input and outputs a simple greeting message. */
@AutoService(SdkRunnableTask.class)
public class GreetTask extends SdkRunnableTask<GreetTask.Input, GreetTask.Output> {

  public GreetTask() {
    super(JacksonSdkType.of(Input.class), JacksonSdkType.of(Output.class));
  }

  /**
   * Generate an immutable value class that represents {@link GreetTask}'s input, which is a String.
   */
  @AutoValue
  public abstract static class Input {
    public abstract SdkBindingData<String> name();

    public static Input create(SdkBindingData<String> greeting) {
      return new AutoValue_GreetTask_Input(greeting);
    }
  }

  /**
   * Generate an immutable value class that represents {@link GreetTask}'s output, which is a
   * String.
   */
  @AutoValue
  public abstract static class Output {
    public abstract SdkBindingData<String> greeting();

    /**
     * Wraps the constructor of the generated output value class.
     *
     * @param greeting the String literal output of {@link GreetTask}
     * @return output of GreetTask
     */
    public static Output create(SdkBindingData<String> greeting) {
      return new AutoValue_GreetTask_Output(greeting);
    }
  }

  /**
   * Defines task behavior. This task takes a name as the input, wraps it in a welcome message, and
   * outputs the message.
   *
   * @param input the name of the person to be greeted
   * @return the welcome message
   */
  @Override
  public Output run(Input input) {
    return Output.create(
        SdkBindingDataFactory.of(String.format("Welcome, %s!", input.name().get())));
  }
}
