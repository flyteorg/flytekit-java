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
package org.flyte.localengine.examples;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import java.util.List;
import org.flyte.flytekit.SdkRunnableTask;
import org.flyte.flytekit.jackson.JacksonSdkType;

@AutoService(SdkRunnableTask.class)
public class ListTask extends SdkRunnableTask<ListTask.Input, ListTask.Output> {

  public ListTask() {
    super(JacksonSdkType.of(Input.class), JacksonSdkType.of(Output.class));
  }

  @Override
  public Output run(Input input) {
    return Output.create(input.list());
  }

  @AutoValue
  public abstract static class Input {
    @JsonProperty
    public abstract List<Long> list();

    public static Input create(List<Long> list) {
      return new AutoValue_ListTask_Input(list);
    }
  }

  @AutoValue
  public abstract static class Output {
    @JsonProperty
    public abstract List<Long> list();

    public static Output create(List<Long> list) {
      return new AutoValue_ListTask_Output(list);
    }
  }
}
