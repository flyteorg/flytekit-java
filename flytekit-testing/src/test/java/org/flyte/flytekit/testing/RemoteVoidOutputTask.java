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
package org.flyte.flytekit.testing;

import com.google.auto.value.AutoValue;
import org.flyte.flytekit.SdkRemoteTask;
import org.flyte.flytekit.SdkTypes;
import org.flyte.flytekit.jackson.JacksonSdkType;

public class RemoteVoidOutputTask {

  public static SdkRemoteTask<Input, Void> create() {
    return SdkRemoteTask.create(
        /* domain= */ null,
        /* project= */ "project",
        /* name= */ "remote_void_output_task",
        JacksonSdkType.of(Input.class),
        SdkTypes.nulls());
  }

  @AutoValue
  public abstract static class Input {
    public abstract String ignore();

    public static Input create(String ignore) {
      return new AutoValue_RemoteVoidOutputTask_Input(/*ignore=*/ ignore);
    }
  }
}
