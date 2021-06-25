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
import org.flyte.flytekit.SdkTypes;
import org.flyte.flytekit.jackson.JacksonSdkType;

/** Receives message as an input, and prints it. */
@AutoService(SdkRunnableTask.class)
public class PrintMessageTask extends SdkRunnableTask<PrintMessageTask.Input, Void> {

  public PrintMessageTask() {
    super(JacksonSdkType.of(Input.class), SdkTypes.nulls());
  }

  public static SdkTransform of(SdkBindingData message) {
    return new PrintMessageTask().withInput("message", message);
  }

  /** Input for {@link PrintMessageTask}. */
  @AutoValue
  public abstract static class Input {
    public abstract String message();
  }

  @Override
  public Void run(Input input) {
    System.out.println(input.message());

    return null;
  }
}
