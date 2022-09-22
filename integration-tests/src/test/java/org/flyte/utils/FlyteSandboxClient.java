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
package org.flyte.utils;

import flyteidl.admin.ExecutionOuterClass;
import flyteidl.core.Execution;
import flyteidl.core.IdentifierOuterClass;
import flyteidl.core.Literals;
import flyteidl.service.AdminServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.rnorth.ducttape.unreliables.Unreliables;

public class FlyteSandboxClient {
  private static final String DOMAIN = "development";
  private static final String PROJECT = "flytesnacks";
  private static final int EXECUTION_TIMEOUT_SECONDS = 300;

  private final String version;
  private final AdminServiceGrpc.AdminServiceBlockingStub stub;

  FlyteSandboxClient(String version, AdminServiceGrpc.AdminServiceBlockingStub stub) {
    this.version = version;
    this.stub = stub;
  }

  public static FlyteSandboxClient create() {
    String version = String.valueOf(System.currentTimeMillis());

    String address = FlyteSandboxContainer.INSTANCE.getContainerIpAddress();
    int port = FlyteSandboxContainer.INSTANCE.getMappedPort(30081);

    ManagedChannel channel =
        ManagedChannelBuilder.forTarget(address + ":" + port).usePlaintext().enableRetry().build();

    AdminServiceGrpc.AdminServiceBlockingStub stub = AdminServiceGrpc.newBlockingStub(channel);

    return new FlyteSandboxClient(version, stub);
  }

  public Literals.LiteralMap createTaskExecution(String name, Literals.LiteralMap inputs) {
    return createExecution(
        IdentifierOuterClass.Identifier.newBuilder()
            .setResourceType(IdentifierOuterClass.ResourceType.TASK)
            .setDomain(DOMAIN)
            .setProject(PROJECT)
            .setName(name)
            .setVersion(version)
            .build(),
        inputs);
  }

  public Literals.LiteralMap createExecution(String name, Literals.LiteralMap inputs) {
    return createExecution(
        IdentifierOuterClass.Identifier.newBuilder()
            .setResourceType(IdentifierOuterClass.ResourceType.LAUNCH_PLAN)
            .setDomain(DOMAIN)
            .setProject(PROJECT)
            .setName(name)
            .setVersion(version)
            .build(),
        inputs);
  }

  private Literals.LiteralMap createExecution(
      IdentifierOuterClass.Identifier id, Literals.LiteralMap inputs) {
    ExecutionOuterClass.ExecutionCreateResponse response =
        stub.createExecution(
            ExecutionOuterClass.ExecutionCreateRequest.newBuilder()
                .setDomain(DOMAIN)
                .setProject(PROJECT)
                .setInputs(inputs)
                .setSpec(ExecutionOuterClass.ExecutionSpec.newBuilder().setLaunchPlan(id).build())
                .build());

    IdentifierOuterClass.WorkflowExecutionIdentifier executionId = response.getId();

    Unreliables.retryUntilTrue(
        EXECUTION_TIMEOUT_SECONDS,
        TimeUnit.SECONDS,
        () -> {
          ExecutionOuterClass.Execution execution =
              stub.getExecution(
                  ExecutionOuterClass.WorkflowExecutionGetRequest.newBuilder()
                      .setId(executionId)
                      .build());

          return !isRunning(execution.getClosure().getPhase());
        });

    ExecutionOuterClass.Execution execution =
        stub.getExecution(
            ExecutionOuterClass.WorkflowExecutionGetRequest.newBuilder()
                .setId(executionId)
                .build());

    if (execution.getClosure().getPhase() != Execution.WorkflowExecution.Phase.SUCCEEDED) {
      throw new RuntimeException(
          "Workflow didn't succeed. [Phase="
              + execution.getClosure().getPhase()
              + "] [Error: "
              + execution.getClosure().getError().getMessage());
    }

    ExecutionOuterClass.WorkflowExecutionGetDataResponse executionData =
        stub.getExecutionData(
            ExecutionOuterClass.WorkflowExecutionGetDataRequest.newBuilder()
                .setId(executionId)
                .build());

    return executionData.getFullOutputs();
  }

  private boolean isRunning(Execution.WorkflowExecution.Phase phase) {
    switch (phase) {
      case SUCCEEDING:
      case QUEUED:
      case RUNNING:
      case UNDEFINED:
        return true;
      case TIMED_OUT:
      case SUCCEEDED:
      case ABORTED:
      case ABORTING:
      case FAILED:
      case FAILING:
      case UNRECOGNIZED:
        return false;
    }

    return false;
  }

  public void registerWorkflows(String classpath) {
    try {
      jflyte(
          "jflyte",
          "register",
          "workflows",
          "-p=" + PROJECT,
          "-d=" + DOMAIN,
          "-v=" + version,
          "-cp=" + classpath);
    } catch (Exception e) {
      throw new RuntimeException("Could not register workflows from: " + classpath, e);
    }
  }

  public void serializeWorkflows(String classpath, String folder) {
    jflyte("jflyte", "serialize", "workflows", "-cp=" + classpath, "-f=" + folder);
  }

  private void jflyte(String... cmd) {
    JFlyteContainer jflyte = new JFlyteContainer(cmd);
    jflyte.start();

    Long exitCode = jflyte.getCurrentContainerInfo().getState().getExitCodeLong();

    if (!Objects.equals(exitCode, 0L)) {
      throw new RuntimeException("Container terminated with non-zero exit code: " + exitCode);
    }
  }
}
