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

import static flyteidl.core.IdentifierOuterClass.ResourceType.LAUNCH_PLAN;
import static flyteidl.core.IdentifierOuterClass.ResourceType.TASK;
import static flyteidl.core.IdentifierOuterClass.ResourceType.WORKFLOW;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import flyteidl.core.IdentifierOuterClass;
import flyteidl.core.Interface;
import flyteidl.core.Literals;
import flyteidl.core.Tasks;
import flyteidl.core.Types;
import flyteidl.core.Workflow;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.flyte.api.v1.Binding;
import org.flyte.api.v1.BindingData;
import org.flyte.api.v1.Container;
import org.flyte.api.v1.Duration;
import org.flyte.api.v1.Identifier;
import org.flyte.api.v1.LaunchPlanIdentifier;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.Node;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;
import org.flyte.api.v1.SimpleType;
import org.flyte.api.v1.TaskIdentifier;
import org.flyte.api.v1.TaskNode;
import org.flyte.api.v1.TaskTemplate;
import org.flyte.api.v1.Timestamp;
import org.flyte.api.v1.TypedInterface;
import org.flyte.api.v1.Variable;
import org.flyte.api.v1.WorkflowIdentifier;
import org.flyte.api.v1.WorkflowMetadata;
import org.flyte.api.v1.WorkflowTemplate;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ProtoUtilTest {
  private static final String DOMAIN = "development";
  private static final String PROJECT = "flyte-test";
  private static final String VERSION = "1";

  @ParameterizedTest
  @MethodSource("createDeserializePrimitivesArguments")
  void shouldDeserializePrimitives(Literals.Primitive input, Primitive expected) {
    assertThat(ProtoUtil.deserialize(input), equalTo(expected));
  }

  static Stream<Arguments> createDeserializePrimitivesArguments() {
    Instant now = Instant.now();
    long seconds = now.getEpochSecond();
    int nanos = now.getNano();

    return Stream.of(
        Arguments.of(Literals.Primitive.newBuilder().setInteger(123).build(), Primitive.of(123)),
        Arguments.of(
            Literals.Primitive.newBuilder().setFloatValue(123.0).build(), Primitive.of(123.0)),
        Arguments.of(
            Literals.Primitive.newBuilder().setStringValue("123").build(), Primitive.of("123")),
        Arguments.of(Literals.Primitive.newBuilder().setBoolean(true).build(), Primitive.of(true)),
        Arguments.of(
            Literals.Primitive.newBuilder()
                .setDatetime(
                    com.google.protobuf.Timestamp.newBuilder()
                        .setSeconds(seconds)
                        .setNanos(nanos)
                        .build())
                .build(),
            Primitive.of(Timestamp.create(seconds, nanos))),
        Arguments.of(
            Literals.Primitive.newBuilder()
                .setDuration(
                    com.google.protobuf.Duration.newBuilder()
                        .setSeconds(seconds)
                        .setNanos(nanos)
                        .build())
                .build(),
            Primitive.of(Duration.create(seconds, nanos))));
  }

  @Test
  void shouldSerializeLaunchPlanIdentifiers() {
    String name = "launch-plan-a";
    LaunchPlanIdentifier id = LaunchPlanIdentifier.create(DOMAIN, PROJECT, name, VERSION);

    IdentifierOuterClass.Identifier serializedId = ProtoUtil.serialize(id);

    assertThat(
        serializedId,
        equalTo(
            IdentifierOuterClass.Identifier.newBuilder()
                .setResourceType(LAUNCH_PLAN)
                .setDomain(DOMAIN)
                .setProject(PROJECT)
                .setName(name)
                .setVersion(VERSION)
                .build()));
  }

  @Test
  void shouldSerializeTaskIdentifiers() {
    String name = "task-a";
    TaskIdentifier id = TaskIdentifier.create(DOMAIN, PROJECT, name, VERSION);

    IdentifierOuterClass.Identifier serializedId = ProtoUtil.serialize(id);

    assertThat(
        serializedId,
        equalTo(
            IdentifierOuterClass.Identifier.newBuilder()
                .setResourceType(TASK)
                .setDomain(DOMAIN)
                .setProject(PROJECT)
                .setName(name)
                .setVersion(VERSION)
                .build()));
  }

  @Test
  void shouldSerializeWorkflowIdentifiers() {
    String name = "workflow-a";
    WorkflowIdentifier id = WorkflowIdentifier.create(DOMAIN, PROJECT, name, VERSION);

    IdentifierOuterClass.Identifier serializedId = ProtoUtil.serialize(id);

    assertThat(
        serializedId,
        equalTo(
            IdentifierOuterClass.Identifier.newBuilder()
                .setResourceType(WORKFLOW)
                .setDomain(DOMAIN)
                .setProject(PROJECT)
                .setName(name)
                .setVersion(VERSION)
                .build()));
  }

  @Test
  void shouldThrowIllegalArgumentExceptionForUnknownImplementationsOfIdentifiers() {
    Identifier id = new UnknownIdentifier();

    Assertions.assertThrows(IllegalArgumentException.class, () -> ProtoUtil.serialize(id));
  }

  @Test
  void shouldSerializeTaskTemplate() {
    String image = "alpine:3.7";
    List<String> commands = Collections.singletonList("echo");
    List<String> args = Collections.singletonList("hello world");
    Container container = Container.create(commands, args, image);
    String varName = "x";
    Variable varValue = Variable.create(LiteralType.create(SimpleType.STRING), null);

    Map<String, Variable> inputs = Collections.singletonMap(varName, varValue);
    TypedInterface interface_ = TypedInterface.create(inputs);
    TaskTemplate template = TaskTemplate.create(container, interface_);

    Tasks.TaskTemplate serializedTemplate = ProtoUtil.serialize(template);

    assertThat(
        serializedTemplate,
        equalTo(
            Tasks.TaskTemplate.newBuilder()
                .setContainer(
                    Tasks.Container.newBuilder()
                        .setImage(image)
                        .addAllCommand(commands)
                        .addAllArgs(args)
                        .build())
                .setMetadata(
                    Tasks.TaskMetadata.newBuilder()
                        .setRuntime(
                            Tasks.RuntimeMetadata.newBuilder()
                                .setType(Tasks.RuntimeMetadata.RuntimeType.FLYTE_SDK)
                                .setFlavor(ProtoUtil.RUNTIME_FLAVOR)
                                .setVersion(ProtoUtil.RUNTIME_VERSION)
                                .build())
                        .build())
                .setInterface(
                    Interface.TypedInterface.newBuilder()
                        .setInputs(
                            Interface.VariableMap.newBuilder()
                                .putVariables(
                                    varName,
                                    Interface.Variable.newBuilder()
                                        // .setDescription(null)
                                        .setType(
                                            Types.LiteralType.newBuilder()
                                                .setSimple(Types.SimpleType.STRING)
                                                .build())
                                        .build())
                                .build())
                        .build())
                .setType(ProtoUtil.TASK_TYPE)
                .build()));
  }

  @Test
  void shouldSerializeWorkflowTemplate() {
    Node nodeA = createNode("a");
    Node nodeB = createNode("b");
    WorkflowMetadata metadata = WorkflowMetadata.create();
    WorkflowTemplate template = WorkflowTemplate.create(Arrays.asList(nodeA, nodeB), metadata);

    Workflow.Node expectedNode1 =
        Workflow.Node.newBuilder()
            .setId("a")
            .setTaskNode(
                Workflow.TaskNode.newBuilder()
                    .setReferenceId(
                        IdentifierOuterClass.Identifier.newBuilder()
                            .setResourceType(TASK)
                            .setDomain(DOMAIN)
                            .setProject(PROJECT)
                            .setName("task-a")
                            .setVersion("version-a")
                            .build())
                    .build())
            .addInputs(
                Literals.Binding.newBuilder()
                    .setVar("input-name-a")
                    .setBinding(
                        Literals.BindingData.newBuilder()
                            .setScalar(
                                Literals.Scalar.newBuilder()
                                    .setPrimitive(
                                        Literals.Primitive.newBuilder()
                                            .setStringValue("input-scalar-a")
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();

    Workflow.Node expectedNode2 =
        Workflow.Node.newBuilder()
            .setId("b")
            .setTaskNode(
                Workflow.TaskNode.newBuilder()
                    .setReferenceId(
                        IdentifierOuterClass.Identifier.newBuilder()
                            .setResourceType(TASK)
                            .setDomain(DOMAIN)
                            .setProject(PROJECT)
                            .setName("task-b")
                            .setVersion("version-b")
                            .build())
                    .build())
            .addInputs(
                Literals.Binding.newBuilder()
                    .setVar("input-name-b")
                    .setBinding(
                        Literals.BindingData.newBuilder()
                            .setScalar(
                                Literals.Scalar.newBuilder()
                                    .setPrimitive(
                                        Literals.Primitive.newBuilder()
                                            .setStringValue("input-scalar-b")
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();

    Workflow.WorkflowTemplate serializedTemplate = ProtoUtil.serialize(template);

    assertThat(
        serializedTemplate,
        equalTo(
            Workflow.WorkflowTemplate.newBuilder()
                .setMetadata(Workflow.WorkflowMetadata.newBuilder().build())
                .addNodes(expectedNode1)
                .addNodes(expectedNode2)
                .build()));
  }

  @ParameterizedTest
  @MethodSource("createSerializePrimitivesArguments")
  void shouldSerializePrimitives(Primitive input, Literals.Primitive expected) {
    assertThat(ProtoUtil.serialize(input), equalTo(expected));
  }

  static Stream<Arguments> createSerializePrimitivesArguments() {
    Instant now = Instant.now();
    long seconds = now.getEpochSecond();
    int nanos = now.getNano();

    return Stream.of(
        Arguments.of(Primitive.of(123), Literals.Primitive.newBuilder().setInteger(123).build()),
        Arguments.of(
            Primitive.of(123.0), Literals.Primitive.newBuilder().setFloatValue(123.0).build()),
        Arguments.of(
            Primitive.of("123"), Literals.Primitive.newBuilder().setStringValue("123").build()),
        Arguments.of(Primitive.of(true), Literals.Primitive.newBuilder().setBoolean(true).build()),
        Arguments.of(
            Primitive.of(Timestamp.create(seconds, nanos)),
            Literals.Primitive.newBuilder()
                .setDatetime(
                    com.google.protobuf.Timestamp.newBuilder()
                        .setSeconds(seconds)
                        .setNanos(nanos)
                        .build())
                .build()),
        Arguments.of(
            Primitive.of(Duration.create(seconds, nanos)),
            Literals.Primitive.newBuilder()
                .setDuration(
                    com.google.protobuf.Duration.newBuilder()
                        .setSeconds(seconds)
                        .setNanos(nanos)
                        .build())
                .build()));
  }

  private Node createNode(String name) {
    String taskName = "task-" + name;
    String version = "version-" + name;
    String input_name = "input-name-" + name;
    String input_scalar = "input-scalar-" + name;

    TaskNode taskNode = TaskNode.create(TaskIdentifier.create(DOMAIN, PROJECT, taskName, version));
    List<Binding> inputs =
        Collections.singletonList(
            Binding.create(
                input_name, BindingData.create(Scalar.create(Primitive.of(input_scalar)))));
    return Node.create(name, taskNode, inputs);
  }

  private static class UnknownIdentifier implements Identifier {

    @Override
    public String domain() {
      return null;
    }

    @Override
    public String project() {
      return null;
    }

    @Override
    public String name() {
      return null;
    }

    @Override
    public String version() {
      return null;
    }
  }
}
