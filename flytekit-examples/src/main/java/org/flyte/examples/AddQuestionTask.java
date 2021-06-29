/*
 * Copyright 2020 Spotify AB.
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
import org.flyte.flytekit.SdkRunnableTask;
import org.flyte.flytekit.SdkTransform;
import org.flyte.flytekit.jackson.JacksonSdkType;

/**
 * Example Flyte task that takes a greeting message as input, appends "How are you?", and outputs
 * the result.
 */
@AutoService(SdkRunnableTask.class)
public class AddQuestionTask
    extends SdkRunnableTask<AddQuestionTask.Input, AddQuestionTask.Output> {
  public AddQuestionTask() {
    super(JacksonSdkType.of(Input.class), JacksonSdkType.of(Output.class));
  }

  /**
   * Binds input data to this task
   *
   * @param greeting the input greeting message
   * @return a transformed instance of this class with input data
   */
  public static SdkTransform of(SdkBindingData greeting) {
    return new AddQuestionTask().withInput("greeting", greeting);
  }

  /**
   * Generate an immutable value class that represents {@link AddQuestionTask}'s input, which is a
   * String.
   */
  @AutoValue
  public abstract static class Input {
    public abstract String greeting();
  }

  /**
   * Generate an immutable value class that represents {@link AddQuestionTask}'s output, which is a
   * String.
   */
  @AutoValue
  public abstract static class Output {
    public abstract String greeting();

    /**
     * Wraps the constructor of the generated output value class.
     *
     * @param greeting the String literal output of {@link AddQuestionTask}
     * @return output of AddQuestionTask
     */
    public static Output create(String greeting) {
      return new AutoValue_AddQuestionTask_Output(greeting);
    }
  }

  /**
   * Defines task behavior. This task takes a greeting message as the input, append it with " How
   * are you?", and outputs a new greeting message.
   *
   * @param input the greeting message
   * @return the updated greeting message
   */
  @Override
  public Output run(Input input) {
    return Output.create(String.format("%s How are you?", input.greeting()));
  }
}
