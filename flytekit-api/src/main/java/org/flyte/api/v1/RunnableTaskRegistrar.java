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
package org.flyte.api.v1;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A registrar that creates {@link RunnableTask} instances. */
public abstract class RunnableTaskRegistrar {
  private static final Logger LOG = LoggerFactory.getLogger(RunnableTaskRegistrar.class);

  public abstract Map<TaskIdentifier, RunnableTask> load(ClassLoader classLoader);

  public static Map<TaskIdentifier, RunnableTask> loadAll(ClassLoader classLoader) {
    ServiceLoader<RunnableTaskRegistrar> loader =
        ServiceLoader.load(RunnableTaskRegistrar.class, classLoader);

    LOG.debug("Discovering RunnableTaskRegistrar");

    Map<TaskIdentifier, RunnableTask> tasks = new HashMap<>();

    for (RunnableTaskRegistrar registrar : loader) {
      LOG.debug("Discovered [{}]", registrar.getClass().getName());

      for (Map.Entry<TaskIdentifier, RunnableTask> entry : registrar.load(classLoader).entrySet()) {
        RunnableTask previous = tasks.put(entry.getKey(), entry.getValue());

        if (previous != null) {
          throw new IllegalArgumentException(
              String.format(
                  "Discovered a duplicate task [%s] [%s] [%s]",
                  entry.getKey(), entry.getValue(), previous));
        }
      }
    }

    return tasks;
  }
}
