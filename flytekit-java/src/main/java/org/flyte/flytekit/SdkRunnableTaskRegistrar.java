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
package org.flyte.flytekit;

import com.google.auto.service.AutoService;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.Resources;
import org.flyte.api.v1.RetryStrategy;
import org.flyte.api.v1.RunnableTask;
import org.flyte.api.v1.RunnableTaskRegistrar;
import org.flyte.api.v1.Struct;
import org.flyte.api.v1.TaskIdentifier;
import org.flyte.api.v1.TypedInterface;

/**
 * Default implementation of a {@link RunnableTaskRegistrar} that discovers {@link SdkRunnableTask}s
 * implementation via {@link ServiceLoader} mechanism. Container tasks implementations must use
 * {@code @AutoService(SdkContainerTask.class)} or manually add their fully qualifies name to the
 * corresponding file.
 *
 * @see ServiceLoader
 */
@AutoService(RunnableTaskRegistrar.class)
public class SdkRunnableTaskRegistrar extends RunnableTaskRegistrar {
  private static final Logger LOG = Logger.getLogger(SdkRunnableTaskRegistrar.class.getName());

  static {
    // enable all levels for the actual handler to pick up
    LOG.setLevel(Level.ALL);
  }

  private static class RunnableTaskImpl<InputT, OutputT> implements RunnableTask {
    private final SdkRunnableTask<InputT, OutputT> sdkTask;

    private RunnableTaskImpl(SdkRunnableTask<InputT, OutputT> sdkTask) {
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
    public Map<String, Literal> run(Map<String, Literal> inputs) {
      InputT value = sdkTask.getInputType().fromLiteralMap(inputs);
      OutputT output = sdkTask.run(value);

      return sdkTask.getOutputType().toLiteralMap(output);
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
    public Resources getResources() {
      return sdkTask.getResources().toIdl();
    }
  }

  /**
   * Load {@link SdkRunnableTask}s using {@link ServiceLoader}
   *
   * @param env env vars in a map that would be used to pickup the project, domain and version for
   *     the discovered tasks.
   * @param classLoader class loader to use when discovering the task using {@link
   *     ServiceLoader#load(Class, ClassLoader)}
   * @return a map of {@link SdkRunnableTask}s by its task identifier.
   */
  @Override
  @SuppressWarnings("rawtypes")
  public Map<TaskIdentifier, RunnableTask> load(Map<String, String> env, ClassLoader classLoader) {
    ServiceLoader<SdkRunnableTask> loader = ServiceLoader.load(SdkRunnableTask.class, classLoader);

    LOG.fine("Discovering SdkRunnableTask");

    Map<TaskIdentifier, RunnableTask> tasks = new HashMap<>();
    SdkConfig sdkConfig = SdkConfig.load(env);

    for (SdkRunnableTask<?, ?> sdkTask : loader) {
      String name = sdkTask.getName();
      TaskIdentifier taskId =
          TaskIdentifier.builder()
              .domain(sdkConfig.domain())
              .project(sdkConfig.project())
              .name(name)
              .version(sdkConfig.version())
              .build();
      LOG.fine(String.format("Discovered [%s]", name));

      RunnableTask task = new RunnableTaskImpl<>(sdkTask);
      RunnableTask previous = tasks.put(taskId, task);

      if (previous != null) {
        throw new IllegalArgumentException(
            String.format("Discovered a duplicate task [%s] [%s] [%s]", name, task, previous));
      }
    }

    return tasks;
  }
}
