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

import static com.google.common.base.Verify.verifyNotNull;

import com.google.common.annotations.VisibleForTesting;
import flyteidl.admin.Common;
import flyteidl.admin.ExecutionOuterClass;
import flyteidl.admin.LaunchPlanOuterClass;
import flyteidl.admin.TaskOuterClass;
import flyteidl.admin.WorkflowOuterClass;
import flyteidl.service.AdminServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.List;
import javax.annotation.Nullable;
import org.flyte.api.v1.LaunchPlanIdentifier;
import org.flyte.api.v1.NamedEntityIdentifier;
import org.flyte.api.v1.TaskIdentifier;
import org.flyte.api.v1.TaskTemplate;
import org.flyte.api.v1.WorkflowIdentifier;
import org.flyte.api.v1.WorkflowTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a thin synchronous wrapper around the auto-generated GRPC stubs for communicating with
 * the admin service.
 */
public class FlyteAdminClient implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(FlyteAdminClient.class);
  static final String TRIGGERING_PRINCIPAL = "sdk";
  static final int USER_TRIGGERED_EXECUTION_NESTING = 0;

  private final AdminServiceGrpc.AdminServiceBlockingStub stub;
  private final ManagedChannel channel;

  @VisibleForTesting
  FlyteAdminClient(AdminServiceGrpc.AdminServiceBlockingStub stub, ManagedChannel channel) {
    this.stub = stub;
    this.channel = channel;
  }

  public static FlyteAdminClient create(String target) {
    return create(target, /* insecure= */ false);
  }

  public static FlyteAdminClient create(String target, boolean insecure) {
    ManagedChannelBuilder<?> builder = ManagedChannelBuilder.forTarget(target);

    if (insecure) {
      builder.usePlaintext();
    }

    ManagedChannel channel = builder.build();

    return new FlyteAdminClient(AdminServiceGrpc.newBlockingStub(builder.build()), channel);
  }

  public void createTask(TaskIdentifier id, TaskTemplate template) {
    LOG.debug("createTask {}", id);

    TaskOuterClass.TaskCreateResponse response =
        stub.createTask(
            TaskOuterClass.TaskCreateRequest.newBuilder()
                .setId(ProtoUtil.serialize(id))
                .setSpec(
                    TaskOuterClass.TaskSpec.newBuilder()
                        .setTemplate(ProtoUtil.serialize(template))
                        .build())
                .build());

    verifyNotNull(response, "Unexpected null response when creating task: %s", id);
  }

  public void createWorkflow(WorkflowIdentifier id, WorkflowTemplate template) {
    LOG.debug("createWorkflow {}", id);

    WorkflowOuterClass.WorkflowCreateResponse response =
        stub.createWorkflow(
            WorkflowOuterClass.WorkflowCreateRequest.newBuilder()
                .setId(ProtoUtil.serialize(id))
                .setSpec(
                    WorkflowOuterClass.WorkflowSpec.newBuilder()
                        .setTemplate(ProtoUtil.serialize(template))
                        .build())
                .build());

    verifyNotNull(response, "Unexpected null response when creating workflow: %s", id);
  }

  public void createLaunchPlan(LaunchPlanIdentifier id, WorkflowIdentifier workflowId) {
    LOG.debug("createLaunchPlan {}", id);

    LaunchPlanOuterClass.LaunchPlanCreateResponse response =
        stub.createLaunchPlan(
            LaunchPlanOuterClass.LaunchPlanCreateRequest.newBuilder()
                .setId(ProtoUtil.serialize(id))
                .setSpec(
                    LaunchPlanOuterClass.LaunchPlanSpec.newBuilder()
                        .setWorkflowId(ProtoUtil.serialize(workflowId))
                        .build())
                .build());

    verifyNotNull(response, "Unexpected null response when creating launch plan: %s", id);
  }

  public void createExecution(String domain, String project, LaunchPlanIdentifier launchPlanId) {
    LOG.debug("createExecution {} {} {}", domain, project, launchPlanId);

    ExecutionOuterClass.ExecutionMetadata metadata =
        ExecutionOuterClass.ExecutionMetadata.newBuilder()
            .setMode(ExecutionOuterClass.ExecutionMetadata.ExecutionMode.MANUAL)
            .setPrincipal(TRIGGERING_PRINCIPAL)
            .setNesting(USER_TRIGGERED_EXECUTION_NESTING)
            .build();

    ExecutionOuterClass.ExecutionSpec spec =
        ExecutionOuterClass.ExecutionSpec.newBuilder()
            .setLaunchPlan(ProtoUtil.serialize(launchPlanId))
            .setMetadata(metadata)
            .build();

    ExecutionOuterClass.ExecutionCreateResponse response =
        stub.createExecution(
            ExecutionOuterClass.ExecutionCreateRequest.newBuilder()
                .setDomain(domain)
                .setProject(project)
                .setSpec(spec)
                .build());

    verifyNotNull(
        response,
        "Unexpected null response when creating execution %s on project %s domain %s",
        launchPlanId,
        project,
        domain);
  }

  @Nullable
  public TaskIdentifier fetchLatestTaskId(NamedEntityIdentifier taskId) {
    Common.ResourceListRequest request =
        Common.ResourceListRequest.newBuilder()
            .setLimit(1)
            .setId(ProtoUtil.serialize(taskId))
            .setSortBy(
                Common.Sort.newBuilder()
                    .setKey("created_at")
                    .setDirection(Common.Sort.Direction.DESCENDING)
                    .build())
            .build();

    List<TaskOuterClass.Task> list = stub.listTasks(request).getTasksList();

    if (list.isEmpty()) {
      return null;
    }

    return ProtoUtil.deserializeTaskId(list.get(0).getId());
  }

  @Override
  public void close() {
    if (channel != null) {
      channel.shutdown();
    }
  }
}
