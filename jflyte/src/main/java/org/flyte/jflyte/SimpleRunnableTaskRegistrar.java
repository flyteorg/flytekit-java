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
package org.flyte.jflyte;

import com.google.auto.service.AutoService;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import org.flyte.api.v1.RunnableTask;
import org.flyte.api.v1.RunnableTaskRegistrar;
import org.flyte.api.v1.TaskIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple implementation of {@link RunnableTaskRegistrar}.
 *
 * <p>FIXME mock implementation to be replaced with the code in SDK.
 */
@AutoService(RunnableTaskRegistrar.class)
public class SimpleRunnableTaskRegistrar extends RunnableTaskRegistrar {
  private static final Logger LOG = LoggerFactory.getLogger(SimpleRunnableTaskRegistrar.class);

  @Override
  public Map<TaskIdentifier, RunnableTask> load(ClassLoader classLoader) {
    String project = "flytetester";
    String domain = "development";
    String version = String.valueOf(System.currentTimeMillis());

    ServiceLoader<RunnableTask> loader = ServiceLoader.load(RunnableTask.class, classLoader);

    Map<TaskIdentifier, RunnableTask> tasks = new HashMap<>();

    LOG.debug("Discovering RunnableTask");

    for (RunnableTask task : loader) {
      LOG.debug("Discovered [{}]", task.getClass().getName());

      String name = task.getClass().getName();
      TaskIdentifier id = TaskIdentifier.create(domain, project, name, version);
      RunnableTask previous = tasks.put(id, task);

      if (previous != null) {
        throw new IllegalArgumentException(
            String.format("Discovered a duplicate task [%s] [%s] [%s]", id, task, previous));
      }
    }

    return tasks;
  }
}
