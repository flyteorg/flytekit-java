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
package org.flyte.jflyte;

import static flyteidl.core.IdentifierOuterClass.ResourceType.LAUNCH_PLAN;
import static flyteidl.core.IdentifierOuterClass.ResourceType.TASK;
import static flyteidl.core.IdentifierOuterClass.ResourceType.WORKFLOW;
import static java.time.format.DateTimeFormatter.ISO_DATE_TIME;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ListValue;
import com.google.protobuf.NullValue;
import com.google.protobuf.Value;
import flyteidl.admin.ScheduleOuterClass;
import flyteidl.core.Condition;
import flyteidl.core.DynamicJob;
import flyteidl.core.Errors;
import flyteidl.core.IdentifierOuterClass;
import flyteidl.core.Interface;
import flyteidl.core.Literals;
import flyteidl.core.Tasks;
import flyteidl.core.Types;
import flyteidl.core.Types.SchemaType.SchemaColumn.SchemaColumnType;
import flyteidl.core.Workflow;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.flyte.api.v1.Binding;
import org.flyte.api.v1.BindingData;
import org.flyte.api.v1.Blob;
import org.flyte.api.v1.BlobMetadata;
import org.flyte.api.v1.BlobType;
import org.flyte.api.v1.BooleanExpression;
import org.flyte.api.v1.BranchNode;
import org.flyte.api.v1.ComparisonExpression;
import org.flyte.api.v1.ConjunctionExpression;
import org.flyte.api.v1.Container;
import org.flyte.api.v1.ContainerError;
import org.flyte.api.v1.CronSchedule;
import org.flyte.api.v1.DynamicJobSpec;
import org.flyte.api.v1.Identifier;
import org.flyte.api.v1.IfBlock;
import org.flyte.api.v1.IfElseBlock;
import org.flyte.api.v1.KeyValuePair;
import org.flyte.api.v1.LaunchPlanIdentifier;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.Node;
import org.flyte.api.v1.NodeError;
import org.flyte.api.v1.NodeMetadata;
import org.flyte.api.v1.Operand;
import org.flyte.api.v1.OutputReference;
import org.flyte.api.v1.PartialLaunchPlanIdentifier;
import org.flyte.api.v1.PartialTaskIdentifier;
import org.flyte.api.v1.PartialWorkflowIdentifier;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Resources;
import org.flyte.api.v1.RetryStrategy;
import org.flyte.api.v1.Scalar;
import org.flyte.api.v1.SchemaType;
import org.flyte.api.v1.SimpleType;
import org.flyte.api.v1.Struct;
import org.flyte.api.v1.TaskIdentifier;
import org.flyte.api.v1.TaskNode;
import org.flyte.api.v1.TaskTemplate;
import org.flyte.api.v1.TypedInterface;
import org.flyte.api.v1.Variable;
import org.flyte.api.v1.WorkflowIdentifier;
import org.flyte.api.v1.WorkflowMetadata;
import org.flyte.api.v1.WorkflowNode;
import org.flyte.api.v1.WorkflowTemplate;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

@SuppressWarnings("PreferJavaTimeOverload")
class ProtoUtilTest {
  private static final String DOMAIN = "development";
  private static final String PROJECT = "flyte-test";
  private static final String TASK_NAME = "HadesPublish";
  private static final String WORKFLOW_NAME = "TopTracks";
  private static final String VERSION = "1";

  @ParameterizedTest
  @MethodSource("createPrimitivesArguments")
  void shouldDeserializePrimitives(Literals.Primitive input, Primitive expected) {
    assertThat(ProtoUtil.deserialize(input), equalTo(expected));
  }

  @ParameterizedTest
  @MethodSource("createPrimitivesArguments")
  void shouldSerializePrimitives(Literals.Primitive expected, Primitive input) {
    assertThat(ProtoUtil.serialize(input), equalTo(expected));
  }

  static Stream<Arguments> createPrimitivesArguments() {
    Instant now = Instant.now();
    long seconds = now.getEpochSecond();
    int nanos = now.getNano();

    return Stream.of(
        Arguments.of(
            Literals.Primitive.newBuilder().setInteger(123).build(), Primitive.ofIntegerValue(123)),
        Arguments.of(
            Literals.Primitive.newBuilder().setFloatValue(123.0).build(),
            Primitive.ofFloatValue(123.0)),
        Arguments.of(
            Literals.Primitive.newBuilder().setStringValue("123").build(),
            Primitive.ofStringValue("123")),
        Arguments.of(
            Literals.Primitive.newBuilder().setBoolean(true).build(),
            Primitive.ofBooleanValue(true)),
        Arguments.of(
            Literals.Primitive.newBuilder()
                .setDatetime(
                    com.google.protobuf.Timestamp.newBuilder()
                        .setSeconds(seconds)
                        .setNanos(nanos)
                        .build())
                .build(),
            Primitive.ofDatetime(Instant.ofEpochSecond(seconds, nanos))),
        Arguments.of(
            Literals.Primitive.newBuilder()
                .setDuration(
                    com.google.protobuf.Duration.newBuilder()
                        .setSeconds(seconds)
                        .setNanos(nanos)
                        .build())
                .build(),
            Primitive.ofDuration(Duration.ofSeconds(seconds, nanos))));
  }

  @ParameterizedTest
  @MethodSource("createLiteralsArguments")
  void shouldDeserializeLiterals(Literals.Literal input, Literal expected) {
    assertThat(ProtoUtil.deserialize(input), equalTo(expected));
  }

  @ParameterizedTest
  @MethodSource("createLiteralsArguments")
  void shouldSerializeLiterals(Literals.Literal expected, Literal input) {
    assertThat(ProtoUtil.serialize(input), equalTo(expected));
  }

  static Stream<Arguments> createLiteralsArguments() {
    Literal apiLiteral = Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(123)));
    Literals.Literal protoLiteral =
        Literals.Literal.newBuilder()
            .setScalar(
                Literals.Scalar.newBuilder()
                    .setPrimitive(Literals.Primitive.newBuilder().setInteger(123).build())
                    .build())
            .build();

    com.google.protobuf.Struct protoStruct =
        com.google.protobuf.Struct.newBuilder()
            .putFields("stringValue", Value.newBuilder().setStringValue("string").build())
            .putFields("boolValue", Value.newBuilder().setBoolValue(true).build())
            .putFields("numberValue", Value.newBuilder().setNumberValue(42.0).build())
            .putFields(
                "listValue",
                Value.newBuilder()
                    .setListValue(
                        ListValue.newBuilder()
                            .addValues(Value.newBuilder().setNumberValue(1.0).build())
                            .addValues(Value.newBuilder().setNumberValue(2.0).build())
                            .addValues(Value.newBuilder().setNumberValue(3.0).build())
                            .build())
                    .build())
            .putFields("nullValue", Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
            .putFields(
                "structValue",
                Value.newBuilder()
                    .setStructValue(
                        com.google.protobuf.Struct.newBuilder()
                            .putFields(
                                "stringValue", Value.newBuilder().setStringValue("string").build())
                            .build())
                    .build())
            .build();

    Struct struct =
        Struct.of(
            ImmutableMap.<String, Struct.Value>builder()
                .put("stringValue", Struct.Value.ofStringValue("string"))
                .put("boolValue", Struct.Value.ofBoolValue(true))
                .put("numberValue", Struct.Value.ofNumberValue(42.0))
                .put(
                    "listValue",
                    Struct.Value.ofListValue(
                        ImmutableList.of(
                            Struct.Value.ofNumberValue(1.0),
                            Struct.Value.ofNumberValue(2.0),
                            Struct.Value.ofNumberValue(3.0))))
                .put("nullValue", Struct.Value.ofNullValue())
                .put(
                    "structValue",
                    Struct.Value.ofStructValue(
                        Struct.of(
                            ImmutableMap.of("stringValue", Struct.Value.ofStringValue("string")))))
                .build());

    return Stream.of(
        Arguments.of(protoLiteral, apiLiteral),
        Arguments.of(
            Literals.Literal.newBuilder()
                .setCollection(
                    Literals.LiteralCollection.newBuilder().addLiterals(protoLiteral).build())
                .build(),
            Literal.ofCollection(singletonList(apiLiteral))),
        Arguments.of(
            Literals.Literal.newBuilder()
                .setMap(Literals.LiteralMap.newBuilder().putLiterals("name", protoLiteral).build())
                .build(),
            Literal.ofMap(Collections.singletonMap("name", apiLiteral))),
        Arguments.of(
            Literals.Literal.newBuilder()
                .setScalar(Literals.Scalar.newBuilder().setGeneric(protoStruct).build())
                .build(),
            Literal.ofScalar(Scalar.ofGeneric(struct))));
  }

  @Test
  void shouldSerializeLiteralMap() {
    Map<String, Literal> input =
        ImmutableMap.of("a", Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(1337L))));
    Literals.Primitive expectedPrimitive =
        Literals.Primitive.newBuilder().setInteger(1337L).build();
    Literals.Scalar expectedScalar =
        Literals.Scalar.newBuilder().setPrimitive(expectedPrimitive).build();
    Literals.LiteralMap expected =
        Literals.LiteralMap.newBuilder()
            .putLiterals("a", Literals.Literal.newBuilder().setScalar(expectedScalar).build())
            .build();

    Literals.LiteralMap output = ProtoUtil.serialize(input);

    assertEquals(expected, output);
  }

  @Test
  void shouldSerializeOutputReference() {
    OutputReference input = OutputReference.builder().nodeId("node-id").var("var").build();
    Types.OutputReference expected =
        Types.OutputReference.newBuilder().setNodeId("node-id").setVar("var").build();

    Types.OutputReference output = ProtoUtil.serialize(input);

    assertEquals(expected, output);
  }

  @ParameterizedTest
  @MethodSource("provideArgsForShouldSerializeBindingData")
  void shouldSerializeBindingData(BindingData input, Literals.BindingData expected) {
    Literals.BindingData output = ProtoUtil.serialize(input);

    assertEquals(expected, output);
  }

  static Stream<Arguments> provideArgsForShouldSerializeBindingData() {
    BindingData apiScalar =
        BindingData.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(1337L)));
    Literals.BindingData protoScalar =
        Literals.BindingData.newBuilder()
            .setScalar(
                Literals.Scalar.newBuilder()
                    .setPrimitive(Literals.Primitive.newBuilder().setInteger(1337L).build())
                    .build())
            .build();

    return Stream.of(
        Arguments.of(apiScalar, protoScalar),
        Arguments.of(
            BindingData.ofCollection(singletonList(apiScalar)),
            Literals.BindingData.newBuilder()
                .setCollection(
                    Literals.BindingDataCollection.newBuilder().addBindings(protoScalar).build())
                .build()),
        Arguments.of(
            BindingData.ofMap(singletonMap("foo", apiScalar)),
            Literals.BindingData.newBuilder()
                .setMap(
                    Literals.BindingDataMap.newBuilder().putBindings("foo", protoScalar).build())
                .build()),
        Arguments.of(
            BindingData.ofOutputReference(
                OutputReference.builder().nodeId("node-id").var("var").build()),
            Literals.BindingData.newBuilder()
                .setPromise(
                    Types.OutputReference.newBuilder().setNodeId("node-id").setVar("var").build())
                .build()));
  }

  @Test
  void shouldSerializeLaunchPlanIdentifiers() {
    String name = "launch-plan-a";
    LaunchPlanIdentifier id =
        LaunchPlanIdentifier.builder()
            .domain(DOMAIN)
            .project(PROJECT)
            .name(name)
            .version(VERSION)
            .build();

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
    TaskIdentifier id =
        TaskIdentifier.builder()
            .domain(DOMAIN)
            .project(PROJECT)
            .name(name)
            .version(VERSION)
            .build();

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
    WorkflowIdentifier id =
        WorkflowIdentifier.builder()
            .domain(DOMAIN)
            .project(PROJECT)
            .name(name)
            .version(VERSION)
            .build();

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
  void shouldSerDeTaskTemplate() {
    Container container =
        Container.builder()
            .command(ImmutableList.of("echo"))
            .args(ImmutableList.of("hello world"))
            .env(ImmutableList.of(KeyValuePair.of("key", "value")))
            .image("alpine:3.7")
            .build();

    Variable stringVar = ApiUtils.createVar(SimpleType.STRING);
    Variable integerVar = ApiUtils.createVar(SimpleType.INTEGER);

    TypedInterface interface_ =
        TypedInterface.builder()
            .inputs(ImmutableMap.of("x", stringVar))
            .outputs(ImmutableMap.of("y", integerVar))
            .build();

    RetryStrategy retries = RetryStrategy.builder().retries(4).build();

    TaskTemplate template =
        TaskTemplate.builder()
            .container(container)
            .type("custom-task")
            .interface_(interface_)
            .retries(retries)
            .custom(
                Struct.of(
                    ImmutableMap.of("custom-prop", Struct.Value.ofStringValue("custom-value"))))
            .cache(true)
            .cacheVersion("0.0.1")
            .cacheSerializable(true)
            .build();

    Tasks.TaskTemplate templateProto =
        Tasks.TaskTemplate.newBuilder()
            .setContainer(
                Tasks.Container.newBuilder()
                    .setImage("alpine:3.7")
                    .addCommand("echo")
                    .addArgs("hello world")
                    .addEnv(
                        Literals.KeyValuePair.newBuilder().setKey("key").setValue("value").build())
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
                    .setDiscoverable(true)
                    .setDiscoveryVersion("0.0.1")
                    .setCacheSerializable(true)
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
            .setType("custom-task")
            .setCustom(
                com.google.protobuf.Struct.newBuilder()
                    .putFields(
                        "custom-prop", Value.newBuilder().setStringValue("custom-value").build())
                    .build())
            .build();

    Tasks.TaskTemplate serializedTemplate = ProtoUtil.serialize(template);
    TaskTemplate deserializedTemplate = ProtoUtil.deserialize(templateProto);

    assertThat(serializedTemplate, equalTo(templateProto));
    assertThat(deserializedTemplate, equalTo(template));
  }

  @Test
  void shouldSerializeWorkflowTemplate() {
    Node nodeA = createNode("a").toBuilder().upstreamNodeIds(singletonList("b")).build();
    Node nodeB =
        createNode("b").toBuilder()
            .metadata(
                NodeMetadata.builder()
                    .name("fancy-b")
                    .timeout(Duration.ofMinutes(15))
                    .retries(RetryStrategy.builder().retries(3).build())
                    .build())
            .build();
    ;
    WorkflowMetadata metadata = WorkflowMetadata.builder().build();
    TypedInterface interface_ =
        TypedInterface.builder().inputs(emptyMap()).outputs(emptyMap()).build();
    WorkflowTemplate template =
        WorkflowTemplate.builder()
            .nodes(asList(nodeA, nodeB))
            .metadata(metadata)
            .interface_(interface_)
            .outputs(emptyList())
            .build();

    Workflow.Node expectedNode1 =
        Workflow.Node.newBuilder()
            .setId("a")
            .addUpstreamNodeIds("b")
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
            .setMetadata(
                Workflow.NodeMetadata.newBuilder()
                    .setName("fancy-b")
                    .setTimeout(
                        com.google.protobuf.Duration.newBuilder().setSeconds(15 * 60).build())
                    .setRetries(Literals.RetryStrategy.newBuilder().setRetries(3).build())
                    .build())
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
                .setInterface(
                    Interface.TypedInterface.newBuilder()
                        .setOutputs(Interface.VariableMap.newBuilder().build())
                        .setInputs(Interface.VariableMap.newBuilder().build())
                        .build())
                .addNodes(expectedNode1)
                .addNodes(expectedNode2)
                .build()));
  }

  @Test
  void shouldSerializeWorkflowNodeForSubWorkflowRef() {
    PartialWorkflowIdentifier subWorkflowRef =
        PartialWorkflowIdentifier.builder()
            .domain("domain")
            .project("project")
            .name("name")
            .version("version")
            .build();

    WorkflowNode workflowNode =
        WorkflowNode.builder()
            .reference(WorkflowNode.Reference.ofSubWorkflowRef(subWorkflowRef))
            .build();

    assertThat(
        ProtoUtil.serialize(workflowNode),
        equalTo(
            Workflow.WorkflowNode.newBuilder()
                .setSubWorkflowRef(
                    IdentifierOuterClass.Identifier.newBuilder()
                        .setResourceType(WORKFLOW)
                        .setDomain("domain")
                        .setProject("project")
                        .setName("name")
                        .setVersion("version")
                        .build())
                .build()));
  }

  @Test
  void shouldSerializeWorkflowNodeForLaunchPlanRef() {
    PartialLaunchPlanIdentifier launchPlanRef =
        PartialLaunchPlanIdentifier.builder()
            .domain("domain")
            .project("project")
            .name("name")
            .version("version")
            .build();

    WorkflowNode workflowNode =
        WorkflowNode.builder()
            .reference(WorkflowNode.Reference.ofLaunchPlanRef(launchPlanRef))
            .build();

    assertThat(
        ProtoUtil.serialize(workflowNode),
        equalTo(
            Workflow.WorkflowNode.newBuilder()
                .setLaunchplanRef(
                    IdentifierOuterClass.Identifier.newBuilder()
                        .setResourceType(LAUNCH_PLAN)
                        .setDomain("domain")
                        .setProject("project")
                        .setName("name")
                        .setVersion("version")
                        .build())
                .build()));
  }

  @Test
  void shouldSerializeBranchNode() {
    ComparisonExpression comparison =
        ComparisonExpression.builder()
            .operator(ComparisonExpression.Operator.EQ)
            .leftValue(Operand.ofVar("a"))
            .rightValue(Operand.ofVar("b"))
            .build();

    Condition.ComparisonExpression comparisonProto =
        Condition.ComparisonExpression.newBuilder()
            .setOperator(Condition.ComparisonExpression.Operator.EQ)
            .setLeftValue(Condition.Operand.newBuilder().setVar("a").build())
            .setRightValue(Condition.Operand.newBuilder().setVar("b").build())
            .build();

    IfBlock ifBlock =
        IfBlock.builder()
            .condition(BooleanExpression.ofComparison(comparison))
            .thenNode(
                Node.builder()
                    .id("node-1")
                    .upstreamNodeIds(ImmutableList.of())
                    .inputs(ImmutableList.of())
                    .build())
            .build();

    Workflow.IfBlock ifBlockProto =
        Workflow.IfBlock.newBuilder()
            .setThenNode(Workflow.Node.newBuilder().setId("node-1").build())
            .setCondition(
                Condition.BooleanExpression.newBuilder().setComparison(comparisonProto).build())
            .build();

    IfElseBlock ifElse =
        IfElseBlock.builder()
            .case_(ifBlock)
            .other(ImmutableList.of(ifBlock))
            .elseNode(ifBlock.thenNode())
            .build();

    Workflow.IfElseBlock ifElseProto =
        Workflow.IfElseBlock.newBuilder()
            .setCase(ifBlockProto)
            .addOther(ifBlockProto)
            .setElseNode(ifBlockProto.getThenNode())
            .build();

    BranchNode branchNode = BranchNode.builder().ifElse(ifElse).build();

    Workflow.BranchNode branchNodeProto =
        Workflow.BranchNode.newBuilder().setIfElse(ifElseProto).build();

    assertThat(ProtoUtil.serialize(branchNode), equalTo(branchNodeProto));
  }

  @Test
  void shouldSerializeBooleanExpressionForConjunction() {
    BooleanExpression left =
        BooleanExpression.ofComparison(
            ComparisonExpression.builder()
                .operator(ComparisonExpression.Operator.EQ)
                .leftValue(Operand.ofVar("a"))
                .rightValue(Operand.ofVar("b"))
                .build());

    BooleanExpression right =
        BooleanExpression.ofComparison(
            ComparisonExpression.builder()
                .operator(ComparisonExpression.Operator.EQ)
                .leftValue(Operand.ofVar("c"))
                .rightValue(Operand.ofVar("d"))
                .build());

    Condition.BooleanExpression leftProto =
        Condition.BooleanExpression.newBuilder()
            .setComparison(
                Condition.ComparisonExpression.newBuilder()
                    .setOperator(Condition.ComparisonExpression.Operator.EQ)
                    .setLeftValue(Condition.Operand.newBuilder().setVar("a").build())
                    .setRightValue(Condition.Operand.newBuilder().setVar("b").build())
                    .build())
            .build();

    Condition.BooleanExpression rightProto =
        Condition.BooleanExpression.newBuilder()
            .setComparison(
                Condition.ComparisonExpression.newBuilder()
                    .setOperator(Condition.ComparisonExpression.Operator.EQ)
                    .setLeftValue(Condition.Operand.newBuilder().setVar("c").build())
                    .setRightValue(Condition.Operand.newBuilder().setVar("d").build())
                    .build())
            .build();

    ConjunctionExpression conjunction =
        ConjunctionExpression.create(ConjunctionExpression.LogicalOperator.AND, left, right);
    Condition.ConjunctionExpression conjunctionProto =
        Condition.ConjunctionExpression.newBuilder()
            .setLeftExpression(leftProto)
            .setRightExpression(rightProto)
            .setOperator(Condition.ConjunctionExpression.LogicalOperator.AND)
            .build();

    assertThat(
        ProtoUtil.serialize(BooleanExpression.ofConjunction(conjunction)),
        equalTo(Condition.BooleanExpression.newBuilder().setConjunction(conjunctionProto).build()));
  }

  @ParameterizedTest
  @MethodSource("createSimpleSerializeLiteralArguments")
  void shouldSerializeSimpleLiteralTypes(LiteralType input, Types.LiteralType expected) {
    assertThat(ProtoUtil.serialize(input), equalTo(expected));
  }

  @ParameterizedTest
  @MethodSource("createSimpleSerializeLiteralArguments")
  void shouldDeserializeSimpleLiteralTypes(LiteralType expected, Types.LiteralType input) {
    assertThat(ProtoUtil.deserialize(input), equalTo(expected));
  }

  static Stream<Arguments> createSimpleSerializeLiteralArguments() {
    ImmutableMap<SimpleType, Types.SimpleType> types =
        ImmutableMap.<SimpleType, Types.SimpleType>builder()
            .put(SimpleType.INTEGER, Types.SimpleType.INTEGER)
            .put(SimpleType.FLOAT, Types.SimpleType.FLOAT)
            .put(SimpleType.STRING, Types.SimpleType.STRING)
            .put(SimpleType.BOOLEAN, Types.SimpleType.BOOLEAN)
            .put(SimpleType.DATETIME, Types.SimpleType.DATETIME)
            .put(SimpleType.DURATION, Types.SimpleType.DURATION)
            .put(SimpleType.STRUCT, Types.SimpleType.STRUCT)
            .build();

    return types.entrySet().stream()
        .map(
            entry ->
                Arguments.of(
                    LiteralType.ofSimpleType(entry.getKey()),
                    Types.LiteralType.newBuilder().setSimple(entry.getValue()).build()));
  }

  @ParameterizedTest
  @MethodSource("createSerializeComplexLiteralArguments")
  void shouldSerializeComplexLiteralTypes(LiteralType input, Types.LiteralType expected) {
    assertThat(ProtoUtil.serialize(input), equalTo(expected));
  }

  @ParameterizedTest
  @MethodSource("createSerializeComplexLiteralArguments")
  void shouldDeserializeComplexLiteralTypes(LiteralType expected, Types.LiteralType input) {
    assertThat(ProtoUtil.deserialize(input), equalTo(expected));
  }

  static Stream<Arguments> createSerializeComplexLiteralArguments() {
    return Stream.of(
        Arguments.of(
            LiteralType.ofBlobType(
                BlobType.builder()
                    .format("avro")
                    .dimensionality(BlobType.BlobDimensionality.SINGLE)
                    .build()),
            Types.LiteralType.newBuilder()
                .setBlob(
                    Types.BlobType.newBuilder()
                        .setFormat("avro")
                        .setDimensionality(Types.BlobType.BlobDimensionality.SINGLE)
                        .build())
                .build()),
        Arguments.of(
            LiteralType.ofCollectionType(LiteralType.ofSimpleType(SimpleType.INTEGER)),
            Types.LiteralType.newBuilder()
                .setCollectionType(
                    Types.LiteralType.newBuilder().setSimple(Types.SimpleType.INTEGER).build())
                .build()),
        Arguments.of(
            LiteralType.ofMapValueType(LiteralType.ofSimpleType(SimpleType.FLOAT)),
            Types.LiteralType.newBuilder()
                .setMapValueType(
                    Types.LiteralType.newBuilder().setSimple(Types.SimpleType.FLOAT).build())
                .build()),
        Arguments.of(
            LiteralType.ofSchemaType(
                SchemaType.builder()
                    .columns(
                        asList(
                            apiColumn("i", SchemaType.ColumnType.INTEGER),
                            apiColumn("f", SchemaType.ColumnType.FLOAT),
                            apiColumn("s", SchemaType.ColumnType.STRING),
                            apiColumn("b", SchemaType.ColumnType.BOOLEAN),
                            apiColumn("t", SchemaType.ColumnType.DATETIME),
                            apiColumn("d", SchemaType.ColumnType.DURATION)))
                    .build()),
            Types.LiteralType.newBuilder()
                .setSchema(
                    Types.SchemaType.newBuilder()
                        .addColumns(protoColumn("i", SchemaColumnType.INTEGER))
                        .addColumns(protoColumn("f", SchemaColumnType.FLOAT))
                        .addColumns(protoColumn("s", SchemaColumnType.STRING))
                        .addColumns(protoColumn("b", SchemaColumnType.BOOLEAN))
                        .addColumns(protoColumn("t", SchemaColumnType.DATETIME))
                        .addColumns(protoColumn("d", SchemaColumnType.DURATION))
                        .build())
                .build()),
        // Testing nesting complex literal types
        Arguments.of(
            LiteralType.ofMapValueType(
                LiteralType.ofCollectionType(LiteralType.ofSimpleType(SimpleType.INTEGER))),
            Types.LiteralType.newBuilder()
                .setMapValueType(
                    Types.LiteralType.newBuilder()
                        .setCollectionType(
                            Types.LiteralType.newBuilder()
                                .setSimple(Types.SimpleType.INTEGER)
                                .build())
                        .build())
                .build()));
  }

  @Test
  public void shouldSerializeCronSchedule() {
    CronSchedule cronSchedule =
        CronSchedule.builder()
            .schedule("* * */5 * *")
            .offset(Duration.ofHours(1).toString())
            .build();

    assertThat(
        ProtoUtil.serialize(cronSchedule),
        equalTo(
            ScheduleOuterClass.Schedule.newBuilder()
                .setCronSchedule(
                    ScheduleOuterClass.CronSchedule.newBuilder()
                        .setSchedule("* * */5 * *")
                        .setOffset("PT1H")
                        .build())
                .build()));
  }

  @Test
  public void shouldSerializeCronScheduleNoOffset() {
    CronSchedule cronSchedule = CronSchedule.builder().schedule("* * */5 * *").build();

    assertThat(
        ProtoUtil.serialize(cronSchedule),
        equalTo(
            ScheduleOuterClass.Schedule.newBuilder()
                .setCronSchedule(
                    ScheduleOuterClass.CronSchedule.newBuilder().setSchedule("* * */5 * *").build())
                .build()));
  }

  private static SchemaType.Column apiColumn(String name, SchemaType.ColumnType type) {
    return SchemaType.Column.builder().name(name).type(type).build();
  }

  private static Types.SchemaType.SchemaColumn protoColumn(String name, SchemaColumnType type) {
    return Types.SchemaType.SchemaColumn.newBuilder().setName(name).setType(type).build();
  }

  @Test
  void shouldSerializeThrowable() {
    // for now, we have very simple implementation
    // proto for ErrorDocument isn't well documented as well

    Throwable e = new RuntimeException("oops");

    Errors.ContainerError error = ProtoUtil.serializeThrowable(e);

    assertThat(error.getKind(), equalTo(Errors.ContainerError.Kind.NON_RECOVERABLE));
    assertThat(error.getMessage(), containsString(e.getMessage()));
    assertThat(error.getCode(), equalTo("SYSTEM:Unknown"));
  }

  @Test
  void shouldSerializeContainerError() {
    ContainerError error =
        ContainerError.create("SYSTEM:NOT_READY", "Not ready", ContainerError.Kind.RECOVERABLE);

    Errors.ContainerError proto = ProtoUtil.serializeContainerError(error);

    assertThat(
        proto,
        equalTo(
            Errors.ContainerError.newBuilder()
                .setMessage("Not ready")
                .setKind(Errors.ContainerError.Kind.RECOVERABLE)
                .setCode("SYSTEM:NOT_READY")
                .build()));
  }

  @Test
  void shouldSerializeBlob() {
    BlobType type =
        BlobType.builder()
            .dimensionality(BlobType.BlobDimensionality.MULTIPART)
            .format("csv")
            .build();

    BlobMetadata metadata = BlobMetadata.builder().type(type).build();

    Blob blob = Blob.builder().metadata(metadata).uri("file://uri").build();

    Literals.Blob proto = ProtoUtil.serialize(blob);

    assertThat(
        proto,
        equalTo(
            Literals.Blob.newBuilder()
                .setMetadata(
                    Literals.BlobMetadata.newBuilder()
                        .setType(
                            Types.BlobType.newBuilder()
                                .setDimensionality(Types.BlobType.BlobDimensionality.MULTIPART)
                                .setFormat("csv")
                                .build()))
                .setUri("file://uri")
                .build()));
  }

  @Test
  void shouldSerializeNodeError() {
    NodeError error = NodeError.builder().failedNodeId("node-1").message("Internal error").build();

    Types.Error proto = ProtoUtil.serialize(error);

    assertThat(
        proto,
        equalTo(
            Types.Error.newBuilder()
                .setFailedNodeId("node-1")
                .setMessage("Internal error")
                .build()));
  }

  @Test
  void shouldDeserializeTaskId() {
    TaskIdentifier taskId =
        ProtoUtil.deserializeTaskId(
            IdentifierOuterClass.Identifier.newBuilder()
                .setResourceType(TASK)
                .setProject(PROJECT)
                .setDomain(DOMAIN)
                .setName(TASK_NAME)
                .setVersion(VERSION)
                .build());

    assertThat(
        taskId,
        equalTo(
            TaskIdentifier.builder()
                .project(PROJECT)
                .domain(DOMAIN)
                .name(TASK_NAME)
                .version(VERSION)
                .build()));
  }

  @Test
  void shouldDeserializeWorkflowId() {
    WorkflowIdentifier workflowId =
        ProtoUtil.deserializeWorkflowId(
            IdentifierOuterClass.Identifier.newBuilder()
                .setResourceType(WORKFLOW)
                .setProject(PROJECT)
                .setDomain(DOMAIN)
                .setName(WORKFLOW_NAME)
                .setVersion(VERSION)
                .build());

    assertThat(
        workflowId,
        equalTo(
            WorkflowIdentifier.builder()
                .project(PROJECT)
                .domain(DOMAIN)
                .name(WORKFLOW_NAME)
                .version(VERSION)
                .build()));
  }

  @Test
  void shouldDeserializeBlob() {
    Types.BlobType type =
        Types.BlobType.newBuilder()
            .setFormat("csv")
            .setDimensionality(Types.BlobType.BlobDimensionality.MULTIPART)
            .build();

    Literals.BlobMetadata metadata = Literals.BlobMetadata.newBuilder().setType(type).build();

    Literals.Blob blob =
        Literals.Blob.newBuilder().setMetadata(metadata).setUri("file://csv").build();

    assertThat(
        ProtoUtil.deserialize(blob),
        equalTo(
            Blob.builder()
                .metadata(
                    BlobMetadata.builder()
                        .type(
                            BlobType.builder()
                                .format("csv")
                                .dimensionality(BlobType.BlobDimensionality.MULTIPART)
                                .build())
                        .build())
                .uri("file://csv")
                .build()));
  }

  @Test
  void shouldSerializeDynamicJobSpec() {
    DynamicJobSpec dynamicJobSpec =
        DynamicJobSpec.builder()
            .nodes(emptyList())
            .subWorkflows(emptyMap())
            .tasks(emptyMap())
            .outputs(emptyList())
            .build();

    DynamicJob.DynamicJobSpec proto = ProtoUtil.serialize(dynamicJobSpec);

    assertThat(proto, equalTo(DynamicJob.DynamicJobSpec.newBuilder().build()));
  }

  @ParameterizedTest
  @MethodSource("outOfRangeDatetimes")
  void shouldRejectDatetimeOutOfTimestampRange(Instant timestamp, String expectedErrMsg) {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> ProtoUtil.serialize(Primitive.ofDatetime(timestamp)));

    assertEquals(expectedErrMsg, ex.getMessage());
  }

  public static Stream<Arguments> outOfRangeDatetimes() {
    return Stream.of(
        Arguments.of(
            ISO_DATE_TIME.parse("0001-01-01T00:00:00Z", Instant::from).minusNanos(1),
            "Datetime out of range, minimum allowed value [0001-01-01T00:00:00Z] but was [0000-12-31T23:59:59"
                + ".999999999Z]"),
        Arguments.of(
            ISO_DATE_TIME.parse("9999-12-31T23:59:59.999999999Z", Instant::from).plusNanos(1),
            "Datetime out of range, maximum allowed value [9999-12-31T23:59:59.999999999Z] but was "
                + "[+10000-01-01T00:00:00Z]"));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "id with spaces is invalid",
        "CamelCaseIsInvalid",
      })
  void shouldRejectNonDNS1123NodesIds(String nodeId) {
    Node node = Node.builder().id(nodeId).upstreamNodeIds(emptyList()).inputs(emptyList()).build();
    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> ProtoUtil.serialize(node));

    assertEquals(
        "Node id [" + nodeId + "] must conform to DNS 1123 naming format", ex.getMessage());
  }

  @Test
  void shouldSerializeContainerWithResources() {
    Container container =
        createContainer(
            Resources.builder()
                .requests(
                    ImmutableMap.of(
                        Resources.ResourceName.CPU, "200m", Resources.ResourceName.MEMORY, "1Gi"))
                .limits(
                    ImmutableMap.of(
                        Resources.ResourceName.CPU, "8", Resources.ResourceName.MEMORY, "32G"))
                .build());

    Tasks.Container actual = ProtoUtil.serialize(container);

    assertThat(
        actual,
        equalTo(
            Tasks.Container.newBuilder()
                .setImage("busybox")
                .addCommand("bash")
                .addAllArgs(asList("-c", "echo", "hello"))
                .setResources(
                    Tasks.Resources.newBuilder()
                        .addRequests(
                            Tasks.Resources.ResourceEntry.newBuilder()
                                .setName(Tasks.Resources.ResourceName.CPU)
                                .setValue("200m")
                                .build())
                        .addRequests(
                            Tasks.Resources.ResourceEntry.newBuilder()
                                .setName(Tasks.Resources.ResourceName.MEMORY)
                                .setValue("1Gi")
                                .build())
                        .addLimits(
                            Tasks.Resources.ResourceEntry.newBuilder()
                                .setName(Tasks.Resources.ResourceName.CPU)
                                .setValue("8")
                                .build())
                        .addLimits(
                            Tasks.Resources.ResourceEntry.newBuilder()
                                .setName(Tasks.Resources.ResourceName.MEMORY)
                                .setValue("32G")
                                .build())
                        .build())
                .build()));
  }

  @ParameterizedTest
  @ValueSource(strings = {"4", "2.3", "+1", "2Ki", "4m", "5e-3", "3E6"})
  void shouldAcceptResourcesWithValidQuantities(String quantity) {
    Container container =
        createContainer(
            Resources.builder()
                .limits(ImmutableMap.of(Resources.ResourceName.CPU, quantity))
                .build());

    Tasks.Container actual = ProtoUtil.serialize(container);

    assertThat(
        actual,
        equalTo(
            Tasks.Container.newBuilder()
                .setImage("busybox")
                .addCommand("bash")
                .addAllArgs(asList("-c", "echo", "hello"))
                .setResources(
                    Tasks.Resources.newBuilder()
                        .addLimits(
                            Tasks.Resources.ResourceEntry.newBuilder()
                                .setName(Tasks.Resources.ResourceName.CPU)
                                .setValue(quantity)
                                .build())
                        .build())
                .build()));
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "1.1.", "+-1", "2+2", "Ki", "1Qi", "-1", "-5G"})
  void shouldRejectResourcesWithInvalidQuantities(String quantity) {
    Container container =
        createContainer(
            Resources.builder()
                .requests(ImmutableMap.of(Resources.ResourceName.CPU, quantity))
                .build());

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> ProtoUtil.serialize(container));

    assertEquals(
        "Resource requests [CPU] has invalid quantity: " + quantity, exception.getMessage());
  }

  private Container createContainer(Resources resources) {
    return Container.builder()
        .image("busybox")
        .command(singletonList("bash"))
        .args(asList("-c", "echo", "hello"))
        .env(emptyList())
        .resources(resources)
        .build();
  }

  private Node createNode(String id) {
    String taskName = "task-" + id;
    String version = "version-" + id;
    String input_name = "input-name-" + id;
    String input_scalar = "input-scalar-" + id;

    TaskNode taskNode =
        TaskNode.builder()
            .referenceId(
                PartialTaskIdentifier.builder()
                    .domain(DOMAIN)
                    .project(PROJECT)
                    .name(taskName)
                    .version(version)
                    .build())
            .build();
    List<Binding> inputs =
        singletonList(
            Binding.builder()
                .var_(input_name)
                .binding(
                    BindingData.ofScalar(Scalar.ofPrimitive(Primitive.ofStringValue(input_scalar))))
                .build());

    return Node.builder()
        .id(id)
        .taskNode(taskNode)
        .inputs(inputs)
        .upstreamNodeIds(emptyList())
        .build();
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
