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

import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toUnmodifiableList;

import com.google.auto.service.AutoService;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.flyte.api.v1.Binding;
import org.flyte.api.v1.DynamicJobSpec;
import org.flyte.api.v1.DynamicWorkflowTask;
import org.flyte.api.v1.DynamicWorkflowTaskRegistrar;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.Node;
import org.flyte.api.v1.RetryStrategy;
import org.flyte.api.v1.TaskIdentifier;
import org.flyte.api.v1.TypedInterface;

/**
 * Default implementation of a {@link DynamicWorkflowTaskRegistrar} that discovers {@link
 * SdkDynamicWorkflowTask}s implementation via {@link ServiceLoader} mechanism. Dynamic Workflow
 * Task implementations must use {@code @AutoService(SdkDynamicWorkflowTask.class)} or manually add
 * their fully qualifies name to the corresponding file.
 *
 * @see ServiceLoader
 */
@AutoService(DynamicWorkflowTaskRegistrar.class)
public class SdkDynamicWorkflowTaskRegistrar extends DynamicWorkflowTaskRegistrar {
  private static final Logger LOG =
      Logger.getLogger(SdkDynamicWorkflowTaskRegistrar.class.getName());

  static {
    // enable all levels for the actual handler to pick up
    LOG.setLevel(Level.ALL);
  }

  private static class DynamicWorkflowTaskImpl<InputT, OutputT> implements DynamicWorkflowTask {
    private final SdkDynamicWorkflowTask<InputT, OutputT> sdkDynamicWorkflow;

    private DynamicWorkflowTaskImpl(SdkDynamicWorkflowTask<InputT, OutputT> sdkDynamicWorkflow) {
      this.sdkDynamicWorkflow = sdkDynamicWorkflow;
    }

    /** {@inheritDoc} */
    @Override
    public String getName() {
      return sdkDynamicWorkflow.getName();
    }

    /** {@inheritDoc} */
    @Override
    public TypedInterface getInterface() {
      return TypedInterface.builder()
          .inputs(sdkDynamicWorkflow.getInputType().getVariableMap())
          .outputs(sdkDynamicWorkflow.getOutputType().getVariableMap())
          .build();
    }

    /** {@inheritDoc} */
    @Override
    public DynamicJobSpec run(Map<String, Literal> inputs) {
      SdkWorkflowBuilder builder = new SdkWorkflowBuilder();

      InputT value = sdkDynamicWorkflow.getInputType().fromLiteralMap(inputs);
      sdkDynamicWorkflow.run(builder, value);

      List<Node> nodes =
          builder.getNodes().values().stream().map(SdkNode::toIdl).collect(toUnmodifiableList());

      List<Binding> outputs = WorkflowTemplateIdl.getOutputBindings(builder);

      return DynamicJobSpec.builder()
          .nodes(nodes)
          .outputs(outputs)
          .subWorkflows(emptyMap())
          .tasks(emptyMap())
          .build();
    }

    /** {@inheritDoc} */
    @Override
    public RetryStrategy getRetries() {
      return RetryStrategy.builder().retries(sdkDynamicWorkflow.getRetries()).build();
    }
  }

  /**
   * Load {@link DynamicWorkflowTask}s using {@link ServiceLoader}.
   *
   * @param env env vars in a map that would be used to pickup the project, domain and version for
   *     the discovered tasks.
   * @param classLoader class loader to use when discovering the task using {@link
   *     ServiceLoader#load(Class, ClassLoader)}
   * @return a map of {@link DynamicWorkflowTask}s by its task identifier.
   */
  @Override
  @SuppressWarnings("rawtypes")
  public Map<TaskIdentifier, DynamicWorkflowTask> load(
      Map<String, String> env, ClassLoader classLoader) {
    ServiceLoader<SdkDynamicWorkflowTask> loader =
        ServiceLoader.load(SdkDynamicWorkflowTask.class, classLoader);

    LOG.fine("Discovering SdkDynamicWorkflowTask");

    Map<TaskIdentifier, DynamicWorkflowTask> tasks = new HashMap<>();
    SdkConfig sdkConfig = SdkConfig.load(env);

    for (SdkDynamicWorkflowTask<?, ?> sdkTask : loader) {
      String name = sdkTask.getName();

      TaskIdentifier taskId =
          TaskIdentifier.builder()
              .domain(sdkConfig.domain())
              .project(sdkConfig.project())
              .name(name)
              .version(sdkConfig.version())
              .build();

      LOG.fine(String.format("Discovered [%s]", name));

      DynamicWorkflowTask task = new DynamicWorkflowTaskImpl<>(sdkTask);
      DynamicWorkflowTask previous = tasks.put(taskId, task);

      if (previous != null) {
        throw new IllegalArgumentException(
            String.format(
                "Discovered a duplicate dynamic workflow task [%s] [%s] [%s]",
                name, task, previous));
      }
    }

    return tasks;
  }
}
