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
import org.flyte.api.v1.WorkflowIdentifier;
import org.flyte.api.v1.WorkflowTemplate;
import org.flyte.api.v1.WorkflowTemplateRegistrar;

/**
 * Default implementation of a {@link WorkflowTemplateRegistrar} that discovers {@link SdkWorkflow}s
 * implementation via {@link ServiceLoader} mechanism and then gets the {@link WorkflowTemplate}
 * from them. Workflows implementations must use {@code @AutoService(SdkWorkflow.class)} or manually
 * add their fully qualifies name to the corresponding file.
 *
 * @see ServiceLoader
 */
@AutoService(WorkflowTemplateRegistrar.class)
public class SdkWorkflowTemplateRegistrar extends WorkflowTemplateRegistrar {
  private static final Logger LOG = Logger.getLogger(SdkWorkflowTemplateRegistrar.class.getName());

  static {
    // enable all levels for the actual handler to pick up
    LOG.setLevel(Level.ALL);
  }

  /**
   * Load {@link SdkWorkflow}s using {@link ServiceLoader} and then gets the {@link
   * WorkflowTemplate} from them.
   *
   * @param env env vars in a map that would be used to pick up the project, domain and version for
   *     the discovered tasks.
   * @param classLoader class loader to use when discovering the task using {@link
   *     ServiceLoader#load(Class, ClassLoader)}
   * @return a map of {@link WorkflowTemplate}s by its workflow identifier.
   */
  @Override
  public Map<WorkflowIdentifier, WorkflowTemplate> load(
      Map<String, String> env, ClassLoader classLoader) {
    // FIXME need to refactor registrars in API: classLoader is redundant because
    // jflyte sets context class loader, and SDK code should safely assume that
    // this is going to be a breaking change

    return load(SdkConfig.load(env), SdkWorkflowRegistry.loadAll());
  }

  Map<WorkflowIdentifier, WorkflowTemplate> load(
      SdkConfig sdkConfig, List<SdkWorkflow<?, ?>> sdkWorkflows) {
    LOG.fine("Discovering SdkWorkflow");

    Map<WorkflowIdentifier, WorkflowTemplate> workflows = new HashMap<>();

    for (SdkWorkflow<?, ?> sdkWorkflow : sdkWorkflows) {
      String name = sdkWorkflow.getName();
      WorkflowIdentifier workflowId =
          WorkflowIdentifier.builder()
              .domain(sdkConfig.domain())
              .project(sdkConfig.project())
              .name(name)
              .version(sdkConfig.version())
              .build();

      LOG.fine(String.format("Discovered [%s]", name));

      SdkWorkflowBuilder builder = new SdkWorkflowBuilder();
      sdkWorkflow.expand(builder);

      WorkflowTemplate workflow = WorkflowTemplateIdl.ofBuilder(builder);
      WorkflowTemplate previous = workflows.put(workflowId, workflow);

      if (previous != null) {
        throw new IllegalArgumentException(
            String.format(
                "Discovered a duplicate workflow [%s] [%s] [%s]", name, workflow, previous));
      }
    }

    return workflows;
  }
}
