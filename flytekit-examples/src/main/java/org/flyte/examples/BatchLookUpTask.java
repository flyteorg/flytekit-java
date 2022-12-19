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

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.flyte.flytekit.NopNamedOutput;
import org.flyte.flytekit.SdkRunnableTask;
import org.flyte.flytekit.jackson.JacksonSdkType;

/**
 * Demo task to show case using {@code List<String>} and {@code Map<String,String>} as input and
 * outputs.
 */
@AutoService(SdkRunnableTask.class)
public class BatchLookUpTask
    extends SdkRunnableTask<BatchLookUpTask.Input, BatchLookUpTask.Output, NopNamedOutput> {
  private static final long serialVersionUID = -5702649537830812613L;

  @Override
  public Class<NopNamedOutput> getNamedOutputClass() {
    return NopNamedOutput.class;
  }

  public BatchLookUpTask() {
    super(JacksonSdkType.of(Input.class), JacksonSdkType.of(Output.class));
  }

  @Override
  public Output run(Input input) {
    List<String> foundValues =
        input.searchKeys().stream()
            .filter(key -> input.keyValues().containsKey(key))
            .map(key -> input.keyValues().get(key))
            .collect(Collectors.toList());

    return Output.create(foundValues);
  }

  @AutoValue
  public abstract static class Input {
    public abstract Map<String, String> keyValues();

    public abstract List<String> searchKeys();
  }

  @AutoValue
  public abstract static class Output {
    public abstract List<String> values();

    public static Output create(List<String> values) {
      return new AutoValue_BatchLookUpTask_Output(values);
    }
  }
}
