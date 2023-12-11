/*
 * Copyright 2023 Flyte Authors.
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
package org.flyte.flytekit;

import com.google.auto.service.AutoService;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.flyte.api.v1.PluginTask;
import org.flyte.api.v1.PluginTaskRegistrar;
import org.flyte.api.v1.RetryStrategy;
import org.flyte.api.v1.Struct;
import org.flyte.api.v1.TaskIdentifier;
import org.flyte.api.v1.TypedInterface;

/**
 * Default implementation of a {@link PluginTaskRegistrar} that discovers {@link SdkPluginTask}s
 * implementation via {@link ServiceLoader} mechanism. Plugin tasks implementations must use
 * {@code @AutoService(SdkPluginTask.class)} or manually add their fully qualifies name to the
 * corresponding file.
 *
 * @see ServiceLoader
 */
@AutoService(PluginTaskRegistrar.class)
public class SdkPluginTaskRegistrar extends PluginTaskRegistrar {
  private static final Logger LOG = Logger.getLogger(SdkPluginTaskRegistrar.class.getName());

  static {
    // enable all levels for the actual handler to pick up
    LOG.setLevel(Level.ALL);
  }

  private static class PluginTaskImpl<InputT, OutputT> implements PluginTask {
    private final SdkPluginTask<InputT, OutputT> sdkTask;

    private PluginTaskImpl(SdkPluginTask<InputT, OutputT> sdkTask) {
      this.sdkTask = sdkTask;
    }

    @Override
    public String getType() {
      return sdkTask.getType();
    }

    @Override
    public Struct getCustom() {
      return sdkTask.getCustom().struct();
    }

    @Override
    public TypedInterface getInterface() {
      return TypedInterface.builder()
          .inputs(sdkTask.getInputType().getVariableMap())
          .outputs(sdkTask.getOutputType().getVariableMap())
          .build();
    }

    @Override
    public RetryStrategy getRetries() {
      return RetryStrategy.builder().retries(sdkTask.getRetries()).build();
    }

    @Override
    public boolean isCached() {
      return sdkTask.isCached();
    }

    @Override
    public String getCacheVersion() {
      return sdkTask.getCacheVersion();
    }

    @Override
    public boolean isCacheSerializable() {
      return sdkTask.isCacheSerializable();
    }

    @Override
    public String getName() {
      return sdkTask.getName();
    }

    @Override
    public boolean isSyncPlugin() {
      return sdkTask.isSyncPlugin();
    }
  }

  /**
   * Load {@link SdkPluginTask}s using {@link ServiceLoader}.
   *
   * @param env env vars in a map that would be used to pick up the project, domain and version for
   *     the discovered tasks.
   * @param classLoader class loader to use when discovering the task using {@link
   *     ServiceLoader#load(Class, ClassLoader)}
   * @return a map of {@link SdkPluginTask}s by its task identifier.
   */
  @Override
  @SuppressWarnings("rawtypes")
  public Map<TaskIdentifier, PluginTask> load(Map<String, String> env, ClassLoader classLoader) {
    ServiceLoader<SdkPluginTask> loader = ServiceLoader.load(SdkPluginTask.class, classLoader);

    LOG.fine("Discovering SdkPluginTask");

    Map<TaskIdentifier, PluginTask> tasks = new HashMap<>();
    SdkConfig sdkConfig = SdkConfig.load(env);

    for (SdkPluginTask<?, ?> sdkTask : loader) {
      String name = sdkTask.getName();
      TaskIdentifier taskId =
          TaskIdentifier.builder()
              .domain(sdkConfig.domain())
              .project(sdkConfig.project())
              .name(name)
              .version(sdkConfig.version())
              .build();
      LOG.fine(String.format("Discovered [%s]", name));

      PluginTask task = new PluginTaskImpl<>(sdkTask);
      PluginTask previous = tasks.put(taskId, task);

      if (previous != null) {
        throw new IllegalArgumentException(
            String.format("Discovered a duplicate task [%s] [%s] [%s]", name, task, previous));
      }
    }

    return tasks;
  }
}
