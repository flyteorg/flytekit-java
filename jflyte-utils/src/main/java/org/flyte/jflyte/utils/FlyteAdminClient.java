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
package org.flyte.jflyte.utils;

import static com.google.common.base.Verify.verifyNotNull;

import com.google.common.annotations.VisibleForTesting;
import flyteidl.admin.Common;
import flyteidl.admin.Common.ResourceListRequest;
import flyteidl.admin.ExecutionOuterClass;
import flyteidl.admin.LaunchPlanOuterClass;
import flyteidl.admin.TaskOuterClass;
import flyteidl.admin.WorkflowOuterClass;
import flyteidl.core.IdentifierOuterClass;
import flyteidl.service.AdminServiceGrpc;
import io.grpc.Channel;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.flyte.api.v1.LaunchPlan;
import org.flyte.api.v1.LaunchPlanIdentifier;
import org.flyte.api.v1.NamedEntityIdentifier;
import org.flyte.api.v1.TaskIdentifier;
import org.flyte.api.v1.TaskTemplate;
import org.flyte.api.v1.WorkflowIdentifier;
import org.flyte.api.v1.WorkflowTemplate;
import org.flyte.jflyte.api.TokenSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a thin synchronous wrapper around the auto-generated gRPC stubs for communicating with
 * the admin service.
 */
public class FlyteAdminClient implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(FlyteAdminClient.class);
  static final String TRIGGERING_PRINCIPAL = "sdk";
  static final int USER_TRIGGERED_EXECUTION_NESTING = 0;

  private final AdminServiceGrpc.AdminServiceBlockingStub stub;
  private final ManagedChannel channel;
  private final GrpcRetries retries;

  @VisibleForTesting
  FlyteAdminClient(
      AdminServiceGrpc.AdminServiceBlockingStub stub, ManagedChannel channel, GrpcRetries retries) {
    this.stub = stub;
    this.channel = channel;
    this.retries = retries;
  }

  public static FlyteAdminClient create(
      String target, boolean insecure, @Nullable TokenSource tokenSource) {
    ManagedChannelBuilder<?> builder = ManagedChannelBuilder.forTarget(target);

    if (insecure) {
      builder.usePlaintext();
    }

    ManagedChannel originChannel = builder.build();
    GrpcRetries retries = GrpcRetries.create();
    if (tokenSource == null) {
      // In case of no tokenSource, no need to intercept the grpc call.
      return new FlyteAdminClient(
          AdminServiceGrpc.newBlockingStub(originChannel), originChannel, retries);
    } else {
      ClientInterceptor interceptor = new AuthorizationHeaderInterceptor(tokenSource);
      Channel channel = ClientInterceptors.intercept(originChannel, interceptor);
      return new FlyteAdminClient(
          AdminServiceGrpc.newBlockingStub(channel), originChannel, retries);
    }
  }

  public void createTask(TaskIdentifier id, TaskTemplate template) {
    LOG.debug("createTask {}", id);

    TaskOuterClass.TaskCreateRequest request =
        TaskOuterClass.TaskCreateRequest.newBuilder()
            .setId(ProtoUtil.serialize(id))
            .setSpec(
                TaskOuterClass.TaskSpec.newBuilder()
                    .setTemplate(ProtoUtil.serialize(template))
                    .build())
            .build();

    idempotentCreate("createTask", id, () -> stub.createTask(request));
  }

  public void createWorkflow(
      WorkflowIdentifier id,
      WorkflowTemplate template,
      Map<WorkflowIdentifier, WorkflowTemplate> subWorkflows) {
    LOG.debug("createWorkflow {}", id);

    WorkflowOuterClass.WorkflowSpec.Builder specBuilder =
        WorkflowOuterClass.WorkflowSpec.newBuilder().setTemplate(ProtoUtil.serialize(template));

    subWorkflows.forEach(
        (subWorkflowId, subWorkflow) ->
            specBuilder.addSubWorkflows(ProtoUtil.serialize(subWorkflowId, subWorkflow)));

    WorkflowOuterClass.WorkflowCreateRequest request =
        WorkflowOuterClass.WorkflowCreateRequest.newBuilder()
            .setId(ProtoUtil.serialize(id))
            .setSpec(specBuilder.build())
            .build();

    idempotentCreate("createWorkflow", id, () -> stub.createWorkflow(request));
  }

  public void createLaunchPlan(LaunchPlanIdentifier id, LaunchPlan launchPlan) {
    LOG.debug("createLaunchPlan {}", id);

    LaunchPlanOuterClass.LaunchPlanCreateRequest request =
        LaunchPlanOuterClass.LaunchPlanCreateRequest.newBuilder()
            .setId(ProtoUtil.serialize(id))
            .setSpec(ProtoUtil.serialize(launchPlan))
            .build();

    idempotentCreate("createLaunchPlan", id, () -> stub.createLaunchPlan(request));
  }

  void createExecution(String domain, String project, LaunchPlanIdentifier launchPlanId) {
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

    ExecutionOuterClass.ExecutionCreateRequest request =
        ExecutionOuterClass.ExecutionCreateRequest.newBuilder()
            .setDomain(domain)
            .setProject(project)
            .setSpec(spec)
            .build();

    // NB: createExecution doesn't compare payloads when throwing ALREADY_EXISTS, so only using
    // retries

    // create operation is idempotent, so it's fine to retry
    ExecutionOuterClass.ExecutionCreateResponse response =
        retries.retry(() -> stub.createExecution(request));

    verifyNotNull(
        response,
        "Unexpected null response when creating execution %s on project %s domain %s",
        launchPlanId,
        project,
        domain);
  }

  @Nullable
  public TaskIdentifier fetchLatestTaskId(NamedEntityIdentifier taskId) {
    return fetchLatestResource(
        taskId,
        request -> stub.listTasks(request).getTasksList(),
        TaskOuterClass.Task::getId,
        ProtoUtil::deserializeTaskId);
  }

  @Nullable
  public WorkflowIdentifier fetchLatestWorkflowId(NamedEntityIdentifier workflowId) {
    return fetchLatestResource(
        workflowId,
        request -> stub.listWorkflows(request).getWorkflowsList(),
        WorkflowOuterClass.Workflow::getId,
        ProtoUtil::deserializeWorkflowId);
  }

  @Nullable
  public LaunchPlanIdentifier fetchLatestLaunchPlanId(NamedEntityIdentifier launchPlanId) {
    return fetchLatestResource(
        launchPlanId,
        request -> stub.listLaunchPlans(request).getLaunchPlansList(),
        LaunchPlanOuterClass.LaunchPlan::getId,
        ProtoUtil::deserializeLaunchPlanId);
  }

  @Nullable
  private <T, RespT> T fetchLatestResource(
      NamedEntityIdentifier nameId,
      Function<ResourceListRequest, List<RespT>> performRequestFn,
      Function<RespT, IdentifierOuterClass.Identifier> extractIdFn,
      Function<IdentifierOuterClass.Identifier, T> deserializeFn) {
    ResourceListRequest request =
        ResourceListRequest.newBuilder()
            .setLimit(1)
            .setId(ProtoUtil.serialize(nameId))
            .setSortBy(
                Common.Sort.newBuilder()
                    .setKey("created_at")
                    .setDirection(Common.Sort.Direction.DESCENDING)
                    .build())
            .build();

    List<RespT> list = retries.retry(() -> performRequestFn.apply(request));

    if (list.isEmpty()) {
      return null;
    }

    IdentifierOuterClass.Identifier id = extractIdFn.apply(list.get(0));
    return deserializeFn.apply(id);
  }

  private <T> void idempotentCreate(String label, Object id, GrpcRetries.Retryable<T> retryable) {
    try {
      // create operation is idempotent, so it's fine to retry
      T response = retries.retry(retryable);

      verifyNotNull(response, "%s %s: Unexpected null response", label, id);
    } catch (StatusRuntimeException e) {
      // flyteadmin uses ALREADY_EXISTS only if payload is identical, except for executions
      if (e.getStatus().getCode() == Status.Code.ALREADY_EXISTS) {
        LOG.debug("{} {}: ALREADY_EXISTS with identical payload", label, id);
      } else {
        throw e;
      }
    }
  }

  @Override
  public void close() {
    if (channel != null) {
      channel.shutdown();
    }
  }
}
