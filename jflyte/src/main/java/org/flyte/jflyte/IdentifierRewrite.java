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

import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import com.google.common.base.Verify;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.flyte.api.v1.LaunchPlan;
import org.flyte.api.v1.NamedEntityIdentifier;
import org.flyte.api.v1.Node;
import org.flyte.api.v1.PartialTaskIdentifier;
import org.flyte.api.v1.PartialWorkflowIdentifier;
import org.flyte.api.v1.TaskIdentifier;
import org.flyte.api.v1.TaskNode;
import org.flyte.api.v1.WorkflowIdentifier;
import org.flyte.api.v1.WorkflowTemplate;

/** Overrides project, domain and version for nodes in {@link WorkflowTemplate}. */
@AutoValue
abstract class IdentifierRewrite {

  abstract String domain();

  abstract String project();

  abstract String version();

  abstract FlyteAdminClient adminClient();

  WorkflowTemplate apply(WorkflowTemplate template) {
    List<Node> newNodes = template.nodes().stream().map(this::apply).collect(Collectors.toList());

    return template.toBuilder().nodes(newNodes).build();
  }

  private Node apply(Node node) {
    return node.toBuilder().taskNode(apply(node.taskNode())).build();
  }

  private TaskNode apply(TaskNode taskNode) {
    return TaskNode.builder().referenceId(apply(taskNode.referenceId())).build();
  }

  private PartialTaskIdentifier apply(PartialTaskIdentifier taskId) {
    String name = Preconditions.checkNotNull(taskId.name(), "name is null");

    // inherit domain and project if not set
    String domain = taskId.domain() == null ? domain() : taskId.domain();
    String project = taskId.project() == null ? project() : taskId.project();

    if (taskId.version() == null) {
      // workflows referencing tasks from the same project
      if (taskId.project() == null) {
        return PartialTaskIdentifier.builder()
            .name(name)
            .domain(domain)
            .project(project)
            .version(version())
            .build();
      }

      // otherwise we reference the latest task

      TaskIdentifier latestTaskId =
          adminClient()
              .fetchLatestTaskId(
                  NamedEntityIdentifier.builder()
                      .domain(domain)
                      .project(project)
                      .name(taskId.name())
                      .build());

      Verify.verifyNotNull(
          latestTaskId,
          "task not found domain=[%s], project=[%s], name=[%s]",
          domain,
          project,
          taskId.name());

      return PartialTaskIdentifier.builder()
          .name(latestTaskId.name())
          .domain(latestTaskId.domain())
          .project(latestTaskId.project())
          .version(latestTaskId.version())
          .build();
    }

    // nothing else is null if we got to this point

    return PartialTaskIdentifier.builder()
        .name(taskId.name())
        .domain(domain)
        .project(project)
        .version(version())
        .build();
  }

  LaunchPlan apply(LaunchPlan launchPlan) {
    return LaunchPlan.builder()
        .name(launchPlan.name())
        .fixedInputs(launchPlan.fixedInputs())
        .workflowId(apply(launchPlan.workflowId()))
        .build();
  }

  private PartialWorkflowIdentifier apply(PartialWorkflowIdentifier workflowId) {
    String name = Preconditions.checkNotNull(workflowId.name(), "name is null");

    if (workflowId.project() != null) {
      // External workflow reference
      String project = workflowId.project();
      String domain =
          Objects.requireNonNull(workflowId.domain(), "domain is null, but project is not");

      if (workflowId.version() != null) {
        return workflowId;
      }

      // we need to reference to latest version of workflow
      WorkflowIdentifier latestWorkflowId =
          adminClient()
              .fetchLatestWorkflowId(
                  NamedEntityIdentifier.builder()
                      .project(workflowId.project())
                      .domain(workflowId.domain())
                      .name(name)
                      .build());

      Verify.verifyNotNull(
          latestWorkflowId,
          "workflow not found domain=[%s], project=[%s], name=[%s]",
          domain,
          project,
          name);

      return PartialWorkflowIdentifier.builder()
          .project(workflowId.project())
          .domain(workflowId.domain())
          .name(name)
          .version(latestWorkflowId.version())
          .build();
    }

    // no project set, launch plan referencing workflow from the same project
    return PartialWorkflowIdentifier.builder()
        .project(project())
        .domain(domain())
        .name(name)
        .version(version())
        .build();
  }

  static Builder builder() {
    return new AutoValue_IdentifierRewrite.Builder();
  }

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder domain(String domain);

    abstract Builder project(String project);

    abstract Builder version(String version);

    abstract Builder adminClient(FlyteAdminClient adminClient);

    abstract IdentifierRewrite build();
  }
}
