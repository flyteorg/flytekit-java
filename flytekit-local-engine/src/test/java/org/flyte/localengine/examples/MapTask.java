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
import java.util.Map;
import java.util.function.Function;

import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkRunnableTask;
import org.flyte.flytekit.jackson.JacksonSdkType;

import static java.util.stream.Collectors.toMap;

@AutoService(SdkRunnableTask.class)
public class MapTask extends SdkRunnableTask<MapTask.Input, MapTask.Output> {
  private static final long serialVersionUID = 4810131589121130850L;

  public MapTask() {
    super(JacksonSdkType.of(Input.class), JacksonSdkType.of(Output.class));
  }

  @Override
  public Output run(Input input) {
    return Output.create(input.map().get());
  }

  @AutoValue
  public abstract static class Input {
    public abstract SdkBindingData<Map<String, Long>> map();

    public static Input create(Map<String, Long> map) {
      return new AutoValue_MapTask_Input(SdkBindingData.ofIntegerMap(map));
    }
  }

  @AutoValue
  public abstract static class Output {
    public abstract SdkBindingData<Map<String, Long>> map();

    public static Output create(Map<String, Long> map) {
      return new AutoValue_MapTask_Output(SdkBindingData.ofIntegerMap(map));
    }
  }
}
