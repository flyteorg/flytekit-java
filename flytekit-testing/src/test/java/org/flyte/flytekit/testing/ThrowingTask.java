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
package org.flyte.flytekit.testing;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkRunnableTask;
import org.flyte.flytekit.SdkTypes;
import org.flyte.flytekit.jackson.JacksonSdkType;

@AutoService(SdkRunnableTask.class)
public class ThrowingTask extends SdkRunnableTask<ThrowingTask.Input, Void> {
  private static final long serialVersionUID = 0L;

  public ThrowingTask() {
    super(JacksonSdkType.of(Input.class), SdkTypes.nulls());
  }

  public static class ThrowingTaskException extends RuntimeException {
    public ThrowingTaskException(String message) {
      super(message);
    }
  }

  @AutoValue
  public abstract static class Input {
    public abstract SdkBindingData<String> in();

    public static Input create(SdkBindingData<String> in) {
      return new AutoValue_ThrowingTask_Input(in);
    }
  }

  @Override
  public Void run(Input input) {
    throw new ThrowingTaskException("in: " + input.in().get());
  }
}
