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

@AutoService(SdkRunnableTask.class)
public class SumTask extends SdkRunnableTask<SumTask.SumInput, SumTask.SumOutput> {
  public SumTask() {
    super(SdkTypes.autoValue(SumInput.class), SdkTypes.autoValue(SumOutput.class));
  }

  public static SdkTransform of(SdkBindingData a, SdkBindingData b) {
    return new SumTask().withInput("a", a).withInput("b", b);
  }

  @AutoValue
  public abstract static class SumInput {
    public abstract long a();

    public abstract long b();
  }

  @AutoValue
  public abstract static class SumOutput {
    public abstract long c();

    public static SumOutput create(long c) {
      return new AutoValue_SumTask_SumOutput(c);
    }
  }

  @Override
  public SumOutput run(SumInput input) {
    return SumOutput.create(input.a() + input.b());
  }
}
