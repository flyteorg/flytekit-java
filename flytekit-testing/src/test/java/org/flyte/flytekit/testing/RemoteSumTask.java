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
import org.flyte.flytekit.jackson.JacksonSdkType;

public class RemoteSumTask {

  public static SdkRemoteTask<RemoteSumInput, RemoteSumOutput> create() {
    return SdkRemoteTask.create(
        /* domain= */ null,
        /* project= */ "project",
        /* name= */ "remote_sum_task",
        JacksonSdkType.of(RemoteSumInput.class),
        JacksonSdkType.of(RemoteSumOutput.class));
  }

  @AutoValue
  public abstract static class RemoteSumInput {
    public abstract long a();

    public abstract long b();

    public static RemoteSumInput create(long a, long b) {
      return new AutoValue_RemoteSumTask_RemoteSumInput(a, b);
    }
  }

  @AutoValue
  public abstract static class RemoteSumOutput {
    public abstract long c();

    public static RemoteSumOutput create(long c) {
      return new AutoValue_RemoteSumTask_RemoteSumOutput(c);
    }
  }
}
