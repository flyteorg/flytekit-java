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

import static java.util.Collections.emptyList;
import static org.flyte.jflyte.ApiUtils.createVar;
import static org.flyte.jflyte.FlyteAdminClient.TRIGGERING_PRINCIPAL;
import static org.flyte.jflyte.FlyteAdminClient.USER_TRIGGERED_EXECUTION_NESTING;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import flyteidl.admin.ExecutionOuterClass;
import flyteidl.admin.LaunchPlanOuterClass;
import flyteidl.admin.TaskOuterClass;
import flyteidl.admin.WorkflowOuterClass;
import flyteidl.core.IdentifierOuterClass;
import flyteidl.core.IdentifierOuterClass.ResourceType;
import flyteidl.core.Interface;
import flyteidl.core.Literals;
import flyteidl.core.Tasks;
import flyteidl.core.Types;
import flyteidl.core.Workflow;
import flyteidl.service.AdminServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.Collections;
import org.flyte.api.v1.Binding;
import org.flyte.api.v1.BindingData;
import org.flyte.api.v1.Container;
import org.flyte.api.v1.KeyValuePair;
import org.flyte.api.v1.LaunchPlan;
import org.flyte.api.v1.LaunchPlanIdentifier;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.Node;
import org.flyte.api.v1.PartialTaskIdentifier;
import org.flyte.api.v1.PartialWorkflowIdentifier;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.RetryStrategy;
import org.flyte.api.v1.Scalar;
import org.flyte.api.v1.SimpleType;
import org.flyte.api.v1.TaskIdentifier;
import org.flyte.api.v1.TaskNode;
import org.flyte.api.v1.TaskTemplate;
import org.flyte.api.v1.TypedInterface;
import org.flyte.api.v1.WorkflowIdentifier;
import org.flyte.api.v1.WorkflowMetadata;
import org.flyte.api.v1.WorkflowTemplate;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class FlyteAdminClientTest {

  private static final String VAR_NAME = "x";
  private static final String SCALAR = "foo";
  private static final String LP_NAME = "launch-plan-1";
  private static final String LP_VERSION = "launch" + "-plan" + "-version";
  private static final String DOMAIN = "development";
  private static final String PROJECT = "flyte-test";
  private static final String TASK_NAME = "task-foo";
  private static final String TASK_VERSION = "version-task-foo";
  private static final String WF_NAME = "workflow-foo";
  private static final String WF_VERSION = "version-wf-foo";
  private static final String IMAGE_NAME = "alpine:latest";
  private static final String COMMAND = "date";

  private FlyteAdminClient client;
  private TestAdminService stubService;

  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
  private static final LaunchPlanIdentifier LP_IDENTIFIER =
      LaunchPlanIdentifier.builder()
          .domain(DOMAIN)
          .project(PROJECT)
          .name(LP_NAME)
          .version(LP_VERSION)
          .build();

  @Before
  public void setup() throws IOException {
    stubService = new TestAdminService();
    String serverName = InProcessServerBuilder.generateName();
    Server build = GrpcUtils.buildServer(serverName, stubService);
    ManagedChannel channel = GrpcUtils.buildChannel(serverName);
    client = new FlyteAdminClient(AdminServiceGrpc.newBlockingStub(channel), channel);
    grpcCleanup.register(build.start());
    grpcCleanup.register(channel);
  }

  @After
  public void tearDown() {
    client.close();
  }

  @Test
  public void shouldPropagateCreateTaskToStub() {
    TaskIdentifier identifier =
        TaskIdentifier.builder()
            .domain(DOMAIN)
            .project(PROJECT)
            .name(TASK_NAME)
            .version(TASK_VERSION)
            .build();

    TypedInterface interface_ =
        TypedInterface.builder()
            .inputs(ImmutableMap.of("x", createVar(SimpleType.STRING)))
            .outputs(ImmutableMap.of("y", createVar(SimpleType.INTEGER)))
            .build();

    Container container =
        Container.builder()
            .command(ImmutableList.of(COMMAND))
            .args(ImmutableList.of())
            .image(IMAGE_NAME)
            .env(ImmutableList.of(KeyValuePair.of("key", "value")))
            .build();

    RetryStrategy retries = RetryStrategy.builder().retries(4).build();
    TaskTemplate template =
        TaskTemplate.builder().container(container).interface_(interface_).retries(retries).build();

    client.createTask(identifier, template);

    assertThat(
        stubService.createTaskRequest,
        equalTo(
            TaskOuterClass.TaskCreateRequest.newBuilder()
                .setId(newIdentifier(ResourceType.TASK, TASK_NAME, TASK_VERSION))
                .setSpec(newTaskSpec())
                .build()));
  }

  @Test
  public void shouldPropagateCreateWorkflowToStub() {
    String nodeId = "node";
    WorkflowIdentifier identifier =
        WorkflowIdentifier.builder()
            .domain(DOMAIN)
            .project(PROJECT)
            .name(WF_NAME)
            .version(WF_VERSION)
            .build();
    TaskNode taskNode =
        TaskNode.builder()
            .referenceId(
                PartialTaskIdentifier.builder()
                    .domain(DOMAIN)
                    .project(PROJECT)
                    .name(TASK_NAME)
                    .version(TASK_VERSION)
                    .build())
            .build();

    Node node =
        Node.builder()
            .id(nodeId)
            .taskNode(taskNode)
            .inputs(
                ImmutableList.of(
                    Binding.builder()
                        .var_(VAR_NAME)
                        .binding(
                            BindingData.ofScalar(Scalar.ofPrimitive(Primitive.ofString(SCALAR))))
                        .build()))
            .upstreamNodeIds(emptyList())
            .build();

    TypedInterface interface_ =
        TypedInterface.builder().inputs(ImmutableMap.of()).outputs(ImmutableMap.of()).build();

    WorkflowTemplate template =
        WorkflowTemplate.builder()
            .nodes(ImmutableList.of(node))
            .metadata(WorkflowMetadata.builder().build())
            .interface_(interface_)
            .outputs(ImmutableList.of())
            .build();

    client.createWorkflow(identifier, template);

    assertThat(
        stubService.createWorkflowRequest,
        equalTo(
            WorkflowOuterClass.WorkflowCreateRequest.newBuilder()
                .setId(newIdentifier(ResourceType.WORKFLOW, WF_NAME, WF_VERSION))
                .setSpec(newWorkflowSpec(nodeId))
                .build()));
  }

  @Test
  public void shouldPropagateLaunchPlanToStub() {
    PartialWorkflowIdentifier wfIdentifier =
        PartialWorkflowIdentifier.builder()
            .project(PROJECT)
            .domain(DOMAIN)
            .name(WF_NAME)
            .version(WF_VERSION)
            .build();
    LaunchPlan launchPlan =
        LaunchPlan.builder()
            .workflowId(wfIdentifier)
            .name(LP_NAME)
            .fixedInputs(
                Collections.singletonMap(
                    "foo", Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofString("bar")))))
            .build();

    client.createLaunchPlan(LP_IDENTIFIER, launchPlan);

    assertThat(
        stubService.createLaunchPlanRequest,
        equalTo(
            LaunchPlanOuterClass.LaunchPlanCreateRequest.newBuilder()
                .setId(newIdentifier(ResourceType.LAUNCH_PLAN, LP_NAME, LP_VERSION))
                .setSpec(
                    LaunchPlanOuterClass.LaunchPlanSpec.newBuilder()
                        .setWorkflowId(newIdentifier(ResourceType.WORKFLOW, WF_NAME, WF_VERSION))
                        .setFixedInputs(
                            Literals.LiteralMap.newBuilder()
                                .putLiterals(
                                    "foo",
                                    Literals.Literal.newBuilder()
                                        .setScalar(
                                            Literals.Scalar.newBuilder()
                                                .setPrimitive(
                                                    Literals.Primitive.newBuilder()
                                                        .setStringValue("bar")
                                                        .build())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build()));
  }

  @Test
  public void shouldPropagateCreateExecutionToStub() {
    client.createExecution(DOMAIN, PROJECT, LP_IDENTIFIER);

    assertThat(
        stubService.createExecutionRequest,
        equalTo(
            ExecutionOuterClass.ExecutionCreateRequest.newBuilder()
                .setDomain(DOMAIN)
                .setProject(PROJECT)
                .setSpec(newExecutionSpec())
                .build()));
  }

  private IdentifierOuterClass.Identifier newIdentifier(
      ResourceType type, String name, String version) {
    return IdentifierOuterClass.Identifier.newBuilder()
        .setResourceType(type)
        .setDomain(DOMAIN)
        .setProject(PROJECT)
        .setName(name)
        .setVersion(version)
        .build();
  }

  private TaskOuterClass.TaskSpec newTaskSpec() {
    return TaskOuterClass.TaskSpec.newBuilder()
        .setTemplate(
            Tasks.TaskTemplate.newBuilder()
                .setContainer(
                    Tasks.Container.newBuilder()
                        .setImage(FlyteAdminClientTest.IMAGE_NAME)
                        .addCommand(COMMAND)
                        .addEnv(
                            Literals.KeyValuePair.newBuilder()
                                .setKey("key")
                                .setValue("value")
                                .build())
                        .build())
                .setMetadata(
                    Tasks.TaskMetadata.newBuilder()
                        .setRuntime(
                            Tasks.RuntimeMetadata.newBuilder()
                                .setType(Tasks.RuntimeMetadata.RuntimeType.FLYTE_SDK)
                                .setFlavor(ProtoUtil.RUNTIME_FLAVOR)
                                .setVersion(ProtoUtil.RUNTIME_VERSION)
                                .build())
                        .setRetries(Literals.RetryStrategy.newBuilder().setRetries(4).build())
                        .build())
                .setInterface(
                    Interface.TypedInterface.newBuilder()
                        .setInputs(
                            Interface.VariableMap.newBuilder()
                                .putVariables(
                                    "x",
                                    Interface.Variable.newBuilder()
                                        .setType(
                                            Types.LiteralType.newBuilder()
                                                .setSimple(Types.SimpleType.STRING)
                                                .build())
                                        .build())
                                .build())
                        .setOutputs(
                            Interface.VariableMap.newBuilder()
                                .putVariables(
                                    "y",
                                    Interface.Variable.newBuilder()
                                        .setType(
                                            Types.LiteralType.newBuilder()
                                                .setSimple(Types.SimpleType.INTEGER)
                                                .build())
                                        .build())
                                .build())
                        .build())
                .setType(ProtoUtil.TASK_TYPE)
                .build())
        .build();
  }

  private WorkflowOuterClass.WorkflowSpec newWorkflowSpec(String nodeId) {
    return WorkflowOuterClass.WorkflowSpec.newBuilder()
        .setTemplate(
            Workflow.WorkflowTemplate.newBuilder()
                .setMetadata(Workflow.WorkflowMetadata.newBuilder().build())
                .setInterface(
                    Interface.TypedInterface.newBuilder()
                        .setInputs(Interface.VariableMap.newBuilder().build())
                        .setOutputs(Interface.VariableMap.newBuilder().build())
                        .build())
                .addNodes(
                    Workflow.Node.newBuilder()
                        .setId(nodeId)
                        .setTaskNode(
                            Workflow.TaskNode.newBuilder()
                                .setReferenceId(
                                    newIdentifier(ResourceType.TASK, TASK_NAME, TASK_VERSION))
                                .build())
                        .addInputs(
                            Literals.Binding.newBuilder()
                                .setVar(VAR_NAME)
                                .setBinding(
                                    Literals.BindingData.newBuilder()
                                        .setScalar(
                                            Literals.Scalar.newBuilder()
                                                .setPrimitive(
                                                    Literals.Primitive.newBuilder()
                                                        .setStringValue(SCALAR)
                                                        .build())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build())
        .build();
  }

  private ExecutionOuterClass.ExecutionSpec newExecutionSpec() {
    return ExecutionOuterClass.ExecutionSpec.newBuilder()
        .setLaunchPlan(newIdentifier(ResourceType.LAUNCH_PLAN, LP_NAME, LP_VERSION))
        .setMetadata(
            ExecutionOuterClass.ExecutionMetadata.newBuilder()
                .setMode(ExecutionOuterClass.ExecutionMetadata.ExecutionMode.MANUAL)
                .setPrincipal(TRIGGERING_PRINCIPAL)
                .setNesting(USER_TRIGGERED_EXECUTION_NESTING)
                .build())
        .build();
  }
}
