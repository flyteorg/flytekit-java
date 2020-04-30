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
package org.flyte.flytekit;

import com.google.auto.service.AutoService;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import org.flyte.api.v1.Node;
import org.flyte.api.v1.WorkflowIdentifier;
import org.flyte.api.v1.WorkflowMetadata;
import org.flyte.api.v1.WorkflowTemplate;
import org.flyte.api.v1.WorkflowTemplateRegistrar;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoService(WorkflowTemplateRegistrar.class)
public class SdkWorkflowTemplateRegistrar extends WorkflowTemplateRegistrar {
  private static final Logger LOG = LoggerFactory.getLogger(SdkRunnableTaskRegistrar.class);

  @Override
  public Map<WorkflowIdentifier, WorkflowTemplate> load(
      ClassLoader classLoader, Map<String, String> env) {
    ServiceLoader<SdkWorkflow> loader = ServiceLoader.load(SdkWorkflow.class, classLoader);

    LOG.debug("Discovering SdkRunnableTask");

    Map<WorkflowIdentifier, WorkflowTemplate> workflows = new HashMap<>();
    SdkConfig sdkConfig = SdkConfig.load(env);

    for (SdkWorkflow sdkWorkflow : loader) {
      String name = sdkWorkflow.getName();
      WorkflowIdentifier workflowId =
          WorkflowIdentifier.create(
              /* domain= */ sdkConfig.domain(),
              /* project= */ sdkConfig.project(),
              /* name= */ name,
              /* version= */ sdkConfig.version());

      LOG.debug("Discovered [{}]", name);

      SdkWorkflowBuilder builder = new SdkWorkflowBuilder();
      sdkWorkflow.expand(builder);

      WorkflowMetadata metadata = WorkflowMetadata.create();

      List<Node> nodes = builder.toIdl();
      WorkflowTemplate workflow = WorkflowTemplate.create(nodes, metadata);
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
