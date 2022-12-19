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
package org.flyte.localengine.examples;

import com.google.auto.service.AutoService;
import java.util.concurrent.atomic.AtomicLong;
import org.flyte.flytekit.NopNamedOutput;
import org.flyte.flytekit.SdkRunnableTask;
import org.flyte.flytekit.SdkTypes;

/** Used to test retries. Shares global counter that only works in testing mode. */
@AutoService(SdkRunnableTask.class)
public class RetryableTask extends SdkRunnableTask<Void, Void, NopNamedOutput> {
  private static final long serialVersionUID = -4698187378116857395L;

  public static final AtomicLong ATTEMPTS_BEFORE_SUCCESS = new AtomicLong();
  public static final AtomicLong ATTEMPTS = new AtomicLong();

  public RetryableTask() {
    super(SdkTypes.nulls(), SdkTypes.nulls());
  }

  @Override
  public int getRetries() {
    return 5;
  }

  @Override
  public Void run(Void input) {
    long attemptsLeft = ATTEMPTS_BEFORE_SUCCESS.decrementAndGet();
    ATTEMPTS.incrementAndGet();

    if (attemptsLeft <= 0) {
      return null;
    } else {
      throw new RuntimeException("oops");
    }
  }
}
