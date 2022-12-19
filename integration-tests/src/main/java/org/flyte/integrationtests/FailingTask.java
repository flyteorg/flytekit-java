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
package org.flyte.integrationtests;

import com.google.auto.service.AutoService;
import org.flyte.flytekit.NopNamedOutput;
import org.flyte.flytekit.SdkRunnableTask;
import org.flyte.flytekit.SdkTypes;

@AutoService(SdkRunnableTask.class)
public class FailingTask extends SdkRunnableTask<Void, Void, NopNamedOutput> {
  private static final long serialVersionUID = 42L;

  public static FailingTask of() {
    return new FailingTask();
  }

  public FailingTask() {
    super(SdkTypes.nulls(), SdkTypes.nulls());
  }

  @Override
  public Void run(Void input) {
    throw new RuntimeException("task failed");
  }
}
