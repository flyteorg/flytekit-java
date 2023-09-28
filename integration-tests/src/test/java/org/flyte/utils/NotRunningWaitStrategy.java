/*
 * Copyright 2020-2021 Flyte Authors
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
package org.flyte.utils;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.rnorth.ducttape.TimeoutException;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.wait.strategy.AbstractWaitStrategy;

class NotRunningWaitStrategy extends AbstractWaitStrategy {
  @Override
  protected void waitUntilReady() {
    try {
      Unreliables.retryUntilTrue(
          (int) startupTimeout.getSeconds(),
          TimeUnit.SECONDS,
          () -> {
            String status = waitStrategyTarget.getCurrentContainerInfo().getState().getStatus();

            return !Objects.equals(status, "running");
          });
    } catch (TimeoutException e) {
      throw new ContainerLaunchException("Timed out waiting for container to exit");
    }
  }
}
