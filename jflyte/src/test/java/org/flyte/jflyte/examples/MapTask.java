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
package org.flyte.jflyte.examples;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import java.util.Map;
import org.flyte.flytekit.SdkRunnableTask;
import org.flyte.flytekit.jackson.JacksonSdkType;

@AutoService(SdkRunnableTask.class)
public class MapTask extends SdkRunnableTask<MapTask.Input, MapTask.Output> {

  public MapTask() {
    super(JacksonSdkType.of(Input.class), JacksonSdkType.of(Output.class));
  }

  @Override
  public Output run(Input input) {
    return Output.create(input.map());
  }

  @AutoValue
  public abstract static class Input {
    @JsonProperty
    public abstract Map<String, Long> map();

    @JsonCreator
    public static Input create(Map<String, Long> map) {
      return new AutoValue_MapTask_Input(map);
    }
  }

  @AutoValue
  public abstract static class Output {
    @JsonProperty
    public abstract Map<String, Long> map();

    @JsonCreator
    public static Output create(Map<String, Long> map) {
      return new AutoValue_MapTask_Output(map);
    }
  }
}
