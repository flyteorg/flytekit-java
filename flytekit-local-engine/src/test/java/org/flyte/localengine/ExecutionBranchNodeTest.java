/*
 * Copyright 2022 Flyte Authors
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
package org.flyte.localengine;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.*;

import com.google.errorprone.annotations.Var;
import java.util.stream.Stream;
import org.flyte.api.v1.BooleanExpression;
import org.flyte.api.v1.ComparisonExpression;
import org.flyte.api.v1.ComparisonExpression.Operator;
import org.flyte.api.v1.NodeError;
import org.flyte.api.v1.Operand;
import org.flyte.api.v1.Primitive;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ExecutionBranchNodeTest {

  private static final ExecutionNode THEN_NODE = buildNode("then");

  public static final BooleanExpression CONDITION =
      BooleanExpression.ofComparison(
          ComparisonExpression.builder()
              .leftValue(Operand.ofPrimitive(Primitive.ofBooleanValue(true)))
              .rightValue(Operand.ofPrimitive(Primitive.ofBooleanValue(true)))
              .operator(Operator.EQ)
              .build());

  private static final ExecutionIfBlock IF_THEN = ExecutionIfBlock.create(CONDITION, THEN_NODE);
  private static final ExecutionNode ELSE_NODE = buildNode("else");
  private static final NodeError ERROR =
      NodeError.builder().failedNodeId("error-node").message("nothing matched").build();

  @Test
  void shouldNotBuildWhenEmptyIfBlocks() {
    ExecutionBranchNode.Builder builder =
        ExecutionBranchNode.builder().elseNode(ELSE_NODE).ifNodes(emptyList());

    IllegalStateException ex = assertThrows(IllegalStateException.class, builder::build);

    assertEquals("There must be at least one if-then node", ex.getMessage());
  }

  @ParameterizedTest
  @MethodSource("validElseErrorCombinations")
  void shouldBuildForValidElseErrorCombinations(ExecutionNode elseNode, NodeError error) {
    @Var
    ExecutionBranchNode.Builder builder =
        ExecutionBranchNode.builder().ifNodes(singletonList(IF_THEN));

    if (elseNode != null) {
      builder = builder.elseNode(elseNode);
    }
    if (error != null) {
      builder = builder.error(error);
    }

    assertNotNull(builder.build());
  }

  @ParameterizedTest
  @MethodSource("invalidElseErrorCombinations")
  void shouldThrowExceptionForInvalidElseErrorCombinations(
      ExecutionNode elseNode, NodeError error) {
    @Var
    ExecutionBranchNode.Builder builder =
        ExecutionBranchNode.builder().ifNodes(singletonList(IF_THEN));

    if (elseNode != null) {
      builder = builder.elseNode(elseNode);
    }
    if (error != null) {
      builder = builder.error(error);
    }

    IllegalStateException ex = assertThrows(IllegalStateException.class, builder::build);

    assertEquals(
        "Must specify either elseNode or errorNode, both cannot be null nor but cannot be specified",
        ex.getMessage());
  }

  private static ExecutionNode buildNode(String nodeId) {
    return ExecutionNode.builder().nodeId(nodeId).upstreamNodeIds(emptyList()).attempts(0).build();
  }

  public static Stream<Arguments> validElseErrorCombinations() {
    return Stream.of(Arguments.of(ELSE_NODE, null), Arguments.of(null, ERROR));
  }

  public static Stream<Arguments> invalidElseErrorCombinations() {
    return Stream.of(Arguments.of(null, null), Arguments.of(ELSE_NODE, ERROR));
  }
}
