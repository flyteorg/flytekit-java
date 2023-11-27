/*
 * Copyright 2023 Flyte Authors.
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
import java.util.List;
import java.util.stream.Collectors;
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkBindingDataFactory;
import org.flyte.flytekit.SdkMapTask;
import org.flyte.flytekit.SdkType;
import org.flyte.flytekit.jackson.JacksonSdkType;

@AutoService(SdkMapTask.class)
public class MapTask extends SdkMapTask<MapTask.Input, MapTask.Output> {

  /** Called by subclasses passing the {@link SdkType}s for inputs and outputs. */
  public MapTask() {
    super(JacksonSdkType.of(MapTask.Input.class), JacksonSdkType.of(MapTask.Output.class));
  }

  @Override
  public Output run(Input input) {
    return MapTask.Output.create(
        SdkBindingDataFactory.of(
            input.names().get().stream()
                .map(name -> name + "!")
                .collect(Collectors.toList())
                .stream()
                .findFirst()
                .get()));
  }

  @AutoValue
  public abstract static class Input {
    public abstract SdkBindingData<List<String>> names();

    public static MapTask.Input create(SdkBindingData<List<String>> greeting) {
      return new AutoValue_MapTask_Input(greeting);
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
    public static MapTask.Output create(SdkBindingData<String> greeting) {
      return new AutoValue_MapTask_Output(greeting);
    }
  }
}
