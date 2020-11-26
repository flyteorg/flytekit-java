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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.flyte.flytekit.SdkRunnableTask;
import org.flyte.flytekit.jackson.JacksonSdkType;

/**
 * Demo task to show case using {@code List<String>} and {@code Map<String,String>} as input and
 * outputs.
 */
@AutoService(SdkRunnableTask.class)
public class BatchLookUpTask
    extends SdkRunnableTask<BatchLookUpTask.Input, BatchLookUpTask.Output> {
  private static final long serialVersionUID = -5702649537830812613L;

  public BatchLookUpTask() {
    super(JacksonSdkType.of(Input.class), JacksonSdkType.of(Output.class));
  }

  @Override
  public Output run(Input input) {
    List<String> foundValues =
        input.getSearchKeys().stream()
            .filter(key -> input.getKeyValues().containsKey(key))
            .map(key -> input.getKeyValues().get(key))
            .collect(Collectors.toList());

    return Output.create(foundValues);
  }

  @AutoValue
  @JsonDeserialize(as = AutoValue_BatchLookUpTask_Input.class)
  public abstract static class Input {
    public abstract Map<String, String> getKeyValues();

    public abstract List<String> getSearchKeys();

    @JsonCreator
    public static Input create(Map<String, String> keyValues, List<String> searchKeys) {
      return new AutoValue_BatchLookUpTask_Input(keyValues, searchKeys);
    }
  }

  @AutoValue
  @JsonSerialize
  public abstract static class Output {
    public abstract List<String> getValues();

    public static Output create(List<String> values) {
      return new AutoValue_BatchLookUpTask_Output(values);
    }
  }
}
