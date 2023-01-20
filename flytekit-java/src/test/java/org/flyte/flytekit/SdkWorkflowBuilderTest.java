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

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.flyte.flytekit.SdkWorkflowBuilder.literalOfInteger;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;
import org.flyte.api.v1.Binding;
import org.flyte.api.v1.BindingData;
import org.flyte.api.v1.BooleanExpression;
import org.flyte.api.v1.BranchNode;
import org.flyte.api.v1.ComparisonExpression;
import org.flyte.api.v1.IfBlock;
import org.flyte.api.v1.IfElseBlock;
import org.flyte.api.v1.Node;
import org.flyte.api.v1.NodeError;
import org.flyte.api.v1.NodeMetadata;
import org.flyte.api.v1.Operand;
import org.flyte.api.v1.OutputReference;
import org.flyte.api.v1.PartialTaskIdentifier;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;
import org.flyte.api.v1.TaskNode;
import org.flyte.api.v1.TypedInterface;
import org.flyte.api.v1.Variable;
import org.flyte.api.v1.WorkflowMetadata;
import org.flyte.api.v1.WorkflowTemplate;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SdkWorkflowBuilderTest {

  @Mock SdkNodeNamePolicy sdkNodeNamePolicy;

  @Test
  void testTimes4WorkflowIdl() {
    when(sdkNodeNamePolicy.nextNodeId()).thenReturn("foo-n0", "foo-n1");
    when(sdkNodeNamePolicy.toNodeName(any())).thenReturn("multiplication-task");

    SdkWorkflowBuilder builder = new SdkWorkflowBuilder(sdkNodeNamePolicy);

    new Times4Workflow().expand(builder);

    Node node0 =
        Node.builder()
            .id("foo-n0")
            .metadata(NodeMetadata.builder().name("multiplication-task").build())
            .taskNode(
                TaskNode.builder()
                    .referenceId(
                        PartialTaskIdentifier.builder()
                            .name("org.flyte.flytekit.SdkWorkflowBuilderTest$MultiplicationTask")
                            .build())
                    .build())
            .inputs(
                asList(
                    Binding.builder()
                        .var_("a")
                        .binding(
                            BindingData.ofOutputReference(
                                OutputReference.builder()
                                    .var("in")
                                    .nodeId(Node.START_NODE_ID)
                                    .build()))
                        .build(),
                    Binding.builder()
                        .var_("b")
                        .binding(
                            BindingData.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(2L))))
                        .build()))
            .upstreamNodeIds(emptyList())
            .build();
    Node node1 =
        Node.builder()
            .id("foo-n1")
            .metadata(NodeMetadata.builder().name("multiplication-task").build())
            .taskNode(
                TaskNode.builder()
                    .referenceId(
                        PartialTaskIdentifier.builder()
                            .name("org.flyte.flytekit.SdkWorkflowBuilderTest$MultiplicationTask")
                            .build())
                    .build())
            .inputs(
                asList(
                    Binding.builder()
                        .var_("a")
                        .binding(
                            BindingData.ofOutputReference(
                                OutputReference.builder().var("o").nodeId("foo-n0").build()))
                        .build(),
                    Binding.builder()
                        .var_("b")
                        .binding(
                            BindingData.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(2L))))
                        .build()))
            .upstreamNodeIds(emptyList())
            .build();

    WorkflowTemplate expected =
        WorkflowTemplate.builder()
            .metadata(WorkflowMetadata.builder().build())
            .interface_(expectedInterface())
            .outputs(expectedOutputs("foo-n1"))
            .nodes(List.of(node0, node1))
            .build();

    assertEquals(expected, builder.toIdlTemplate());
  }

  @Test
  void testConditionalWorkflowIdl() {
    SdkWorkflowBuilder builder = new SdkWorkflowBuilder();

    new ConditionalWorkflow().expand(builder);

    Node caseNode =
        Node.builder()
            .id("neq")
            .taskNode(
                TaskNode.builder()
                    .referenceId(
                        PartialTaskIdentifier.builder()
                            .name("org.flyte.flytekit.SdkWorkflowBuilderTest$MultiplicationTask")
                            .build())
                    .build())
            .inputs(
                asList(
                    Binding.builder()
                        .var_("a")
                        .binding(
                            BindingData.ofOutputReference(
                                OutputReference.builder()
                                    .var("in")
                                    .nodeId(Node.START_NODE_ID)
                                    .build()))
                        .build(),
                    Binding.builder()
                        .var_("b")
                        .binding(
                            BindingData.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(2L))))
                        .build()))
            .upstreamNodeIds(emptyList())
            .build();

    IfElseBlock ifElse =
        IfElseBlock.builder()
            .case_(
                IfBlock.builder()
                    .condition(
                        BooleanExpression.ofComparison(
                            ComparisonExpression.builder()
                                .leftValue(Operand.ofVar("$0"))
                                .rightValue(Operand.ofPrimitive(Primitive.ofIntegerValue(2L)))
                                .operator(ComparisonExpression.Operator.NEQ)
                                .build()))
                    .thenNode(caseNode)
                    .build())
            .error(NodeError.builder().message("No cases matched").failedNodeId("square").build())
            .other(emptyList())
            .build();

    Node expectedNode =
        Node.builder()
            .id("square")
            .branchNode(BranchNode.builder().ifElse(ifElse).build())
            .inputs(
                singletonList(
                    Binding.builder()
                        .var_("$0")
                        .binding(
                            BindingData.ofOutputReference(
                                OutputReference.builder()
                                    .var("in")
                                    .nodeId(Node.START_NODE_ID)
                                    .build()))
                        .build()))
            .upstreamNodeIds(emptyList())
            .build();

    WorkflowTemplate expected =
        WorkflowTemplate.builder()
            .metadata(WorkflowMetadata.builder().build())
            .interface_(expectedInterface())
            .outputs(expectedOutputs("square"))
            .nodes(singletonList(expectedNode))
            .build();

    assertEquals(expected, builder.toIdlTemplate());
  }

  @Test
  void testDuplicateNodeId() {
    SdkWorkflowBuilder builder = new SdkWorkflowBuilder();

    SdkBindingData<Long> a = builder.inputOfInteger("a");
    SdkBindingData<Long> b = builder.inputOfInteger("b");

    builder.apply("node-1", new MultiplicationTask().withInput("a", a).withInput("b", b));

    CompilerException e =
        assertThrows(
            CompilerException.class,
            () ->
                builder.apply(
                    "node-1", new MultiplicationTask().withInput("a", a).withInput("b", b)));

    assertEquals(
        "Failed to build workflow with errors:\n"
            + "Error 0: Code: DUPLICATE_NODE_ID, Node Id: node-1, Description: Trying to insert two nodes with the same id.",
        e.getMessage());
  }

  @ParameterizedTest
  @MethodSource("createTransform")
  void testVariableNameNotFound_output(SdkTransform<TestUnaryIntegerOutput> transform) {
    SdkWorkflowBuilder builder = new SdkWorkflowBuilder();

    SdkBindingData<Long> a = builder.inputOfInteger("a");
    SdkBindingData<Long> b = builder.inputOfInteger("b");

    SdkNode<TestUnaryIntegerOutput> node1 =
        builder.apply("node-1", transform.withInput("a", a).withInput("b", b));

    CompilerException e = assertThrows(CompilerException.class, () -> node1.getOutput("foo"));

    assertEquals(
        "Failed to build workflow with errors:\n"
            + "Error 0: Code: VARIABLE_NAME_NOT_FOUND, Node Id: node-1, Description: Variable [foo] not found on node [node-1].",
        e.getMessage());
  }

  @ParameterizedTest
  @MethodSource("createTransform")
  void testVariableNameNotFound_input(SdkTransform<TestUnaryIntegerOutput> transform) {
    SdkWorkflowBuilder builder = new SdkWorkflowBuilder();

    SdkBindingData<Long> a = builder.inputOfInteger("a");
    SdkBindingData<Long> b = builder.inputOfInteger("b");
    SdkBindingData<Long> foo = builder.inputOfInteger("foo");

    CompilerException e =
        assertThrows(
            CompilerException.class,
            () ->
                builder.apply(
                    "node-1", transform.withInput("a", a).withInput("b", b).withInput("foo", foo)));

    assertEquals(
        "Failed to build workflow with errors:\n"
            + "Error 0: Code: VARIABLE_NAME_NOT_FOUND, Node Id: node-1, Description: Variable [foo] not found on node [node-1].",
        e.getMessage());
  }

  @ParameterizedTest
  @MethodSource("createTransform")
  void testParameterNotBound(SdkTransform<TestUnaryIntegerOutput> transform) {
    SdkWorkflowBuilder builder = new SdkWorkflowBuilder();

    SdkBindingData<Long> a = builder.inputOfInteger("a");

    CompilerException e =
        assertThrows(
            CompilerException.class, () -> builder.apply("node-1", transform.withInput("a", a)));

    assertEquals(
        "Failed to build workflow with errors:\n"
            + "Error 0: Code: PARAMETER_NOT_BOUND, Node Id: node-1, Description: Parameter not bound [b].",
        e.getMessage());
  }

  @ParameterizedTest
  @MethodSource("createTransform")
  void tesMismatchingTypes(SdkTransform<TestUnaryIntegerOutput> transform) {
    SdkWorkflowBuilder builder = new SdkWorkflowBuilder();

    SdkBindingData<String> a = builder.inputOfString("a");
    SdkBindingData<String> b = builder.inputOfString("b");

    CompilerException e =
        assertThrows(
            CompilerException.class,
            () -> builder.apply("node-1", transform.withInput("a", a).withInput("b", b)));

    // TODO need to implement pretty-printer for types, not it isn't super readable

    assertEquals(
        "Failed to build workflow with errors:\n"
            + "Error 0: Code: MISMATCHING_TYPES, Node Id: node-1, Description: Variable [a] (type [LiteralType{simpleType=STRING}]) doesn't match expected type [LiteralType{simpleType=INTEGER}].\n"
            + "Error 1: Code: MISMATCHING_TYPES, Node Id: node-1, Description: Variable [b] (type [LiteralType{simpleType=STRING}]) doesn't match expected type [LiteralType{simpleType=INTEGER}].",
        e.getMessage());
  }

  @ParameterizedTest
  @MethodSource("createTransform")
  void testUpstreamNode_withUpstreamNode(SdkTransform<TestUnaryIntegerOutput> transform) {
    SdkWorkflowBuilder builder = new SdkWorkflowBuilder();

    SdkBindingData<Long> el0 = builder.inputOfInteger("el0");
    SdkBindingData<Long> el1 = builder.inputOfInteger("el1");

    SdkNode<TestUnaryIntegerOutput> el2 =
        builder.apply("el2", transform.withInput("a", el0).withInput("b", el1));

    SdkNode<TestUnaryIntegerOutput> el3 =
        builder.apply(
            "el3", transform.withUpstreamNode(el2).withInput("a", el0).withInput("b", el1));

    assertEquals(singletonList("el2"), el3.toIdl().upstreamNodeIds());
  }

  @Test
  void testUpstreamNode_apply() {
    SdkWorkflowBuilder builder = new SdkWorkflowBuilder();

    SdkNode<Void> node1 = builder.apply("node1", new PrintHello());
    SdkNode<Void> node2 = node1.apply("node2", new PrintHello());

    assertEquals(singletonList("node1"), node2.toIdl().upstreamNodeIds());
  }

  @Test
  void testUpstreamNode_duplicate() {
    SdkWorkflowBuilder builder = new SdkWorkflowBuilder();

    SdkNode<Void> node1 = builder.apply("node1", new PrintHello());

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> node1.apply("node2", new PrintHello().withUpstreamNode(node1)));

    assertEquals("Duplicate upstream node ids [node1]", e.getMessage());
  }

  @Test
  void testUpstreamNode_duplicateWithNode() {
    SdkWorkflowBuilder builder = new SdkWorkflowBuilder();

    SdkNode<Void> node1 = builder.apply("node1", new PrintHello());

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                builder.apply(
                    "node2", new PrintHello().withUpstreamNode(node1).withUpstreamNode(node1)));

    assertEquals("Duplicate upstream node id [node1]", e.getMessage());
  }

  @ParameterizedTest
  @MethodSource("createTransform")
  void testNodeMetadataOverrides(SdkTransform<TestUnaryIntegerOutput> transform) {
    SdkWorkflowBuilder builder = new SdkWorkflowBuilder();

    SdkBindingData<Long> el0 = builder.inputOfInteger("el0");
    SdkBindingData<Long> el1 = builder.inputOfInteger("el1");

    SdkNode<TestUnaryIntegerOutput> el2 =
        builder.apply("el2", transform.withInput("a", el0).withInput("b", el1));

    SdkNode<TestUnaryIntegerOutput> el3 =
        builder.apply(
            "el3",
            transform
                .withUpstreamNode(el2)
                .withInput("a", el0)
                .withInput("b", el1)
                .withNameOverride("fancy-el3")
                .withTimeoutOverride(Duration.ofMinutes(15)));

    assertThat(
        el3.toIdl().metadata(),
        equalTo(NodeMetadata.builder().name("fancy-el3").timeout(Duration.ofMinutes(15)).build()));
  }

  @ParameterizedTest
  @MethodSource("createTransform")
  void testNodeMetadataOverrides_duplicate(SdkTransform<TestUnaryIntegerOutput> transform) {
    SdkWorkflowBuilder builder = new SdkWorkflowBuilder();

    SdkBindingData<Long> el0 = builder.inputOfInteger("el0");
    SdkBindingData<Long> el1 = builder.inputOfInteger("el1");

    SdkNode<TestUnaryIntegerOutput> el2 =
        builder.apply("el2", transform.withInput("a", el0).withInput("b", el1));

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                builder.apply(
                    "el3",
                    transform
                        .withUpstreamNode(el2)
                        .withInput("a", el0)
                        .withInput("b", el1)
                        .withNameOverride("fancy-el3")
                        .withNameOverride("another-name")));

    assertThat(ex.getMessage(), equalTo("Duplicate values for metadata: name"));
  }

  @Test
  void testInputOf() {
    SdkWorkflowBuilder builder = new SdkWorkflowBuilder();

    assertEquals(
        SdkBindingData.ofOutputReference("start-node", "input1", LiteralTypes.STRING),
        builder.inputOfString("input1"));

    assertEquals(
        SdkBindingData.ofOutputReference("start-node", "input2", LiteralTypes.BOOLEAN),
        builder.inputOfBoolean("input2"));

    assertEquals(
        SdkBindingData.ofOutputReference("start-node", "input3", LiteralTypes.DATETIME),
        builder.inputOfDatetime("input3"));

    assertEquals(
        SdkBindingData.ofOutputReference("start-node", "input4", LiteralTypes.DURATION),
        builder.inputOfDuration("input4"));

    assertEquals(
        SdkBindingData.ofOutputReference("start-node", "input5", LiteralTypes.FLOAT),
        builder.inputOfFloat("input5"));

    assertEquals(
        SdkBindingData.ofOutputReference("start-node", "input6", LiteralTypes.INTEGER),
        builder.inputOfInteger("input6"));
  }

  static List<SdkTransform<TestUnaryIntegerOutput>> createTransform() {
    return asList(new MultiplicationTask(), new MultiplicationWorkflow());
  }

  private TypedInterface expectedInterface() {
    return TypedInterface.builder()
        .inputs(
            singletonMap(
                "in",
                Variable.builder()
                    .literalType(LiteralTypes.INTEGER)
                    .description("Enter value to square")
                    .build()))
        .outputs(
            singletonMap(
                "o", Variable.builder().literalType(LiteralTypes.INTEGER).description("").build()))
        .build();
  }

  private List<Binding> expectedOutputs(String nodeId) {
    return singletonList(
        Binding.builder()
            .var_("o")
            .binding(
                BindingData.ofOutputReference(
                    OutputReference.builder().var("o").nodeId(nodeId).build()))
            .build());
  }

  private static class Times4Workflow extends SdkWorkflow<TestUnaryIntegerOutput> {

    protected Times4Workflow() {
      super(new TestUnaryIntegerOutput.SdkType());
    }

    @Override
    public void expand(SdkWorkflowBuilder builder) {
      SdkBindingData<Long> in = builder.inputOfInteger("in", "Enter value to square");
      SdkBindingData<Long> two = literalOfInteger(2L);
      SdkBindingData<Long> out1 =
          builder
              .apply(new MultiplicationTask().withInput("a", in).withInput("b", two))
              .getOutputs()
              .o();
      SdkBindingData<Long> out2 =
          builder
              .apply(new MultiplicationTask().withInput("a", out1).withInput("b", two))
              .getOutputs()
              .o();

      builder.output("o", out2);
    }
  }

  private static class ConditionalWorkflow extends SdkWorkflow<TestUnaryIntegerOutput> {

    private ConditionalWorkflow() {
      super(new TestUnaryIntegerOutput.SdkType());
    }

    @Override
    public void expand(SdkWorkflowBuilder builder) {
      SdkBindingData<Long> in = builder.inputOfInteger("in", "Enter value to square");
      SdkBindingData<Long> two = literalOfInteger(2L);

      SdkNode<TestUnaryIntegerOutput> out =
          builder.apply(
              "square",
              SdkConditions.when(
                  "neq",
                  SdkConditions.neq(in, two),
                  new MultiplicationTask().withInput("a", in).withInput("b", two)));

      builder.output("o", out.getOutputs().o());
    }
  }

  static class MultiplicationTask
      extends SdkRunnableTask<TestPairIntegerInput, TestUnaryIntegerOutput> {
    private static final long serialVersionUID = -1971936360636181781L;

    MultiplicationTask() {
      super(new TestPairIntegerInput.SdkType(), new TestUnaryIntegerOutput.SdkType());
    }

    @Override
    public TestUnaryIntegerOutput run(TestPairIntegerInput input) {
      throw new UnsupportedOperationException();
    }
  }

  static class MultiplicationWorkflow extends SdkWorkflow<TestUnaryIntegerOutput> {

    MultiplicationWorkflow() {
      super(new TestUnaryIntegerOutput.SdkType());
    }

    @Override
    public void expand(SdkWorkflowBuilder builder) {
      SdkBindingData<Long> a = builder.inputOfInteger("a");
      SdkBindingData<Long> b = builder.inputOfInteger("b");

      SdkNode<TestUnaryIntegerOutput> multiply =
          builder.apply("multiply", new MultiplicationTask().withInput("a", a).withInput("b", b));

      builder.output("c", multiply.getOutputs().o());
    }
  }

  static class PrintHello extends SdkRunnableTask<Void, Void> {
    private static final long serialVersionUID = -1971936360636181781L;

    PrintHello() {
      super(SdkTypes.nulls(), SdkTypes.nulls());
    }

    @Override
    public Void run(Void input) {
      throw new UnsupportedOperationException();
    }
  }
}
