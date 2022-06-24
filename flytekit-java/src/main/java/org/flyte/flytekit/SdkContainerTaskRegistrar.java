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
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.flyte.api.v1.ContainerTask;
import org.flyte.api.v1.ContainerTaskRegistrar;
import org.flyte.api.v1.KeyValuePair;
import org.flyte.api.v1.Resources;
import org.flyte.api.v1.RetryStrategy;
import org.flyte.api.v1.RunnableTask;
import org.flyte.api.v1.Struct;
import org.flyte.api.v1.TaskIdentifier;
import org.flyte.api.v1.TypedInterface;

/** A registrar that creates {@link RunnableTask} instances. */
@AutoService(ContainerTaskRegistrar.class)
public class SdkContainerTaskRegistrar extends ContainerTaskRegistrar {
  private static final Logger LOG = Logger.getLogger(SdkContainerTaskRegistrar.class.getName());

  static {
    // enable all levels for the actual handler to pick up
    LOG.setLevel(Level.ALL);
  }

  private static class ContainerTaskImpl<InputT, OutputT> implements ContainerTask {
    private final SdkContainerTask<InputT, OutputT> sdkTask;

    private ContainerTaskImpl(SdkContainerTask<InputT, OutputT> sdkTask) {
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
    public String getName() {
      return sdkTask.getName();
    }

    @Override
    public String getImage() {
      return sdkTask.getImage();
    }

    @Override
    public List<String> getArgs() {
      return sdkTask.getArgs();
    }

    @Override
    public List<String> getCommand() {
      return sdkTask.getCommand();
    }

    @Override
    public List<KeyValuePair> getEnv() {
      return sdkTask.getEnv().entrySet().stream()
          .map(entry -> KeyValuePair.of(entry.getKey(), entry.getValue()))
          .collect(Collectors.toList());
    }

    @Override
    public Resources getResources() {
      return sdkTask.getResources().toIdl();
    }
  }

  @Override
  @SuppressWarnings("rawtypes")
  public Map<TaskIdentifier, ContainerTask> load(Map<String, String> env, ClassLoader classLoader) {
    ServiceLoader<SdkContainerTask> loader =
        ServiceLoader.load(SdkContainerTask.class, classLoader);

    LOG.fine("Discovering SdkContainerTask");

    Map<TaskIdentifier, ContainerTask> tasks = new HashMap<>();
    SdkConfig sdkConfig = SdkConfig.load(env);

    for (SdkContainerTask<?, ?> sdkTask : loader) {
      String name = sdkTask.getName();
      TaskIdentifier taskId =
          TaskIdentifier.builder()
              .domain(sdkConfig.domain())
              .project(sdkConfig.project())
              .name(name)
              .version(sdkConfig.version())
              .build();
      LOG.fine(String.format("Discovered [%s]", name));

      ContainerTask task = new ContainerTaskImpl<>(sdkTask);
      ContainerTask previous = tasks.put(taskId, task);

      if (previous != null) {
        throw new IllegalArgumentException(
            String.format("Discovered a duplicate task [%s] [%s] [%s]", name, task, previous));
      }
    }

    return tasks;
  }
}
