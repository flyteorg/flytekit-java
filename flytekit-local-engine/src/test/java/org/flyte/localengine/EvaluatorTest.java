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
package org.flyte.localengine;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.stream.Stream;
import org.flyte.api.v1.BooleanExpression;
import org.flyte.api.v1.ComparisonExpression;
import org.flyte.api.v1.ComparisonExpression.Operator;
import org.flyte.api.v1.ConjunctionExpression;
import org.flyte.api.v1.ConjunctionExpression.LogicalOperator;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.Operand;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;
import org.flyte.api.v1.Struct;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class EvaluatorTest {
  private static final Instant PRESENT = Instant.now();
  private static final Instant PAST = PRESENT.minus(100, ChronoUnit.MINUTES);
  private static final Instant FUTURE = PRESENT.plus(100, ChronoUnit.MINUTES);

  private static final Duration SMALL = Duration.ofMillis(1);
  private static final Duration MEDIUM = Duration.ofSeconds(1);
  private static final Duration LARGE = Duration.ofMinutes(1);
  private Evaluator evaluator;

  @BeforeEach
  void setUp() {
    evaluator = new Evaluator();
  }

  @ParameterizedTest
  @MethodSource("evaluateComparisonProvider")
  void testEvaluateComparisons(Operator op, Primitive left, Primitive right, boolean expected) {
    Evaluator evaluator = new Evaluator();

    boolean result =
        evaluator.evaluate(
            BooleanExpression.ofComparison(
                ComparisonExpression.builder()
                    .leftValue(Operand.ofPrimitive(left))
                    .rightValue(Operand.ofPrimitive(right))
                    .operator(op)
                    .build()),
            emptyMap());

    assertEquals(expected, result);
  }

  @ParameterizedTest
  @MethodSource("testEvaluateComparisonsWithIncompatiblePrimitivesProvider")
  void testEvaluateComparisonsWithIncompatiblePrimitives(
      Operator op, Primitive left, Primitive right) {
    String expectedErrMsg =
        String.format("Operands are not comparable: [%s] <-> [%s]", left, right);

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                evaluator.evaluate(
                    BooleanExpression.ofComparison(
                        ComparisonExpression.builder()
                            .leftValue(Operand.ofPrimitive(left))
                            .rightValue(Operand.ofPrimitive(right))
                            .operator(op)
                            .build()),
                    emptyMap()));

    assertEquals(expectedErrMsg, ex.getMessage());
  }

  @Test
  void testEvaluateComparisonsWithVar() {
    boolean result =
        evaluator.evaluate(
            BooleanExpression.ofComparison(
                ComparisonExpression.builder()
                    .leftValue(Operand.ofPrimitive(ip(42)))
                    .rightValue(Operand.ofVar("x"))
                    .operator(Operator.GT)
                    .build()),
            singletonMap("x", Literal.ofScalar(Scalar.ofPrimitive(ip(5)))));

    assertTrue(result);
  }

  @Test
  void testEvaluateThrowsExceptionWhenVarNotInInputs() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                evaluator.evaluate(
                    BooleanExpression.ofComparison(
                        ComparisonExpression.builder()
                            .leftValue(Operand.ofPrimitive(ip(42)))
                            .rightValue(Operand.ofVar("x"))
                            .operator(Operator.GT)
                            .build()),
                    emptyMap()));

    assertEquals("Variable [x] not in inputs: {}", ex.getMessage());
  }

  @Test
  void testEvaluateThrowsExceptionWhenVarIsNotAPrimitive() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                evaluator.evaluate(
                    BooleanExpression.ofComparison(
                        ComparisonExpression.builder()
                            .leftValue(Operand.ofPrimitive(ip(42)))
                            .rightValue(Operand.ofVar("x"))
                            .operator(Operator.GT)
                            .build()),
                    singletonMap("x", Literal.ofScalar(Scalar.ofGeneric(Struct.of(emptyMap()))))));

    assertEquals(
        "Variable [x] not a primitive: Literal{scalar=Scalar{generic=Struct{fields={}}}}",
        ex.getMessage());
  }

  @ParameterizedTest
  @MethodSource("testEvaluateConjunctionsProvider")
  void testEvaluateConjunctions(LogicalOperator op, boolean left, boolean right, boolean expected) {
    boolean result =
        evaluator.evaluate(
            BooleanExpression.ofConjunction(
                ConjunctionExpression.create(
                    op,
                    BooleanExpression.ofComparison(
                        ComparisonExpression.builder()
                            .leftValue(Operand.ofPrimitive(bp(true)))
                            .rightValue(Operand.ofPrimitive(bp(left)))
                            .operator(Operator.EQ)
                            .build()),
                    BooleanExpression.ofComparison(
                        ComparisonExpression.builder()
                            .leftValue(Operand.ofPrimitive(bp(true)))
                            .rightValue(Operand.ofPrimitive(bp(right)))
                            .operator(Operator.EQ)
                            .build()))),
            emptyMap());

    assertEquals(expected, result);
  }

  public static Stream<Arguments> evaluateComparisonProvider() {
    return Stream.of(
        // Integers
        Arguments.of(Operator.EQ, ip(1), ip(1), true),
        Arguments.of(Operator.EQ, ip(1), ip(3), false),
        Arguments.of(Operator.NEQ, ip(1), ip(3), true),
        Arguments.of(Operator.NEQ, ip(1), ip(1), false),
        Arguments.of(Operator.GT, ip(2), ip(1), true),
        Arguments.of(Operator.GT, ip(1), ip(2), false),
        Arguments.of(Operator.GT, ip(1), ip(1), false),
        Arguments.of(Operator.GTE, ip(2), ip(1), true),
        Arguments.of(Operator.GTE, ip(1), ip(2), false),
        Arguments.of(Operator.GTE, ip(1), ip(1), true),
        Arguments.of(Operator.LT, ip(2), ip(1), false),
        Arguments.of(Operator.LT, ip(1), ip(2), true),
        Arguments.of(Operator.LT, ip(1), ip(1), false),
        Arguments.of(Operator.LTE, ip(2), ip(1), false),
        Arguments.of(Operator.LTE, ip(1), ip(2), true),
        Arguments.of(Operator.LTE, ip(1), ip(1), true),
        // Floats
        Arguments.of(Operator.EQ, fp(1), fp(1), true),
        Arguments.of(Operator.EQ, fp(1), fp(3), false),
        Arguments.of(Operator.NEQ, fp(1), fp(3), true),
        Arguments.of(Operator.NEQ, fp(1), fp(1), false),
        Arguments.of(Operator.GT, fp(2), fp(1), true),
        Arguments.of(Operator.GT, fp(1), fp(2), false),
        Arguments.of(Operator.GT, fp(1), fp(1), false),
        Arguments.of(Operator.GTE, fp(2), fp(1), true),
        Arguments.of(Operator.GTE, fp(1), fp(2), false),
        Arguments.of(Operator.GTE, fp(1), fp(1), true),
        Arguments.of(Operator.LT, fp(2), fp(1), false),
        Arguments.of(Operator.LT, fp(1), fp(2), true),
        Arguments.of(Operator.LT, fp(1), fp(1), false),
        Arguments.of(Operator.LTE, fp(2), fp(1), false),
        Arguments.of(Operator.LTE, fp(1), fp(2), true),
        Arguments.of(Operator.LTE, fp(1), fp(1), true),
        // Type coercion
        Arguments.of(Operator.EQ, ip(1), fp(1), true),
        Arguments.of(Operator.EQ, fp(1), ip(1), true),
        Arguments.of(Operator.GT, fp(2), ip(1), true),
        Arguments.of(Operator.GT, ip(2), fp(1), true),
        Arguments.of(Operator.GTE, fp(2), ip(1), true),
        Arguments.of(Operator.GTE, ip(2), fp(1), true),
        // String Type (lexicography order)
        Arguments.of(Operator.EQ, sp("a"), sp("a"), true),
        Arguments.of(Operator.EQ, sp("a"), sp("c"), false),
        Arguments.of(Operator.NEQ, sp("a"), sp("c"), true),
        Arguments.of(Operator.NEQ, sp("a"), sp("a"), false),
        Arguments.of(Operator.GT, sp("b"), sp("a"), true),
        Arguments.of(Operator.GT, sp("a"), sp("b"), false),
        Arguments.of(Operator.GT, sp("a"), sp("a"), false),
        Arguments.of(Operator.GTE, sp("b"), sp("a"), true),
        Arguments.of(Operator.GTE, sp("a"), sp("b"), false),
        Arguments.of(Operator.GTE, sp("a"), sp("a"), true),
        Arguments.of(Operator.LT, sp("b"), sp("a"), false),
        Arguments.of(Operator.LT, sp("a"), sp("b"), true),
        Arguments.of(Operator.LT, sp("a"), sp("a"), false),
        Arguments.of(Operator.LTE, sp("b"), sp("a"), false),
        Arguments.of(Operator.LTE, sp("a"), sp("b"), true),
        Arguments.of(Operator.LTE, sp("a"), sp("a"), true),
        // Boolean Type (false are lesser than true)
        Arguments.of(Operator.EQ, bp(false), bp(false), true),
        Arguments.of(Operator.EQ, bp(false), bp(true), false),
        Arguments.of(Operator.NEQ, bp(false), bp(true), true),
        Arguments.of(Operator.NEQ, bp(false), bp(false), false),
        Arguments.of(Operator.GT, bp(true), bp(false), true),
        Arguments.of(Operator.GT, bp(false), bp(false), false),
        Arguments.of(Operator.GT, bp(true), bp(true), false),
        Arguments.of(Operator.GTE, bp(true), bp(false), true),
        Arguments.of(Operator.GTE, bp(false), bp(true), false),
        Arguments.of(Operator.GTE, bp(false), bp(false), true),
        Arguments.of(Operator.LT, bp(true), bp(false), false),
        Arguments.of(Operator.LT, bp(false), bp(true), true),
        Arguments.of(Operator.LT, bp(false), bp(false), false),
        Arguments.of(Operator.LTE, bp(true), bp(false), false),
        Arguments.of(Operator.LTE, bp(false), bp(true), true),
        Arguments.of(Operator.LTE, bp(false), bp(false), true),
        // Datetime type
        Arguments.of(Operator.EQ, dtp(PAST), dtp(PAST), true),
        Arguments.of(Operator.EQ, dtp(PAST), dtp(FUTURE), false),
        Arguments.of(Operator.NEQ, dtp(PAST), dtp(FUTURE), true),
        Arguments.of(Operator.NEQ, dtp(PAST), dtp(PAST), false),
        Arguments.of(Operator.GT, dtp(PRESENT), dtp(PAST), true),
        Arguments.of(Operator.GT, dtp(PAST), dtp(PRESENT), false),
        Arguments.of(Operator.GT, dtp(PAST), dtp(PAST), false),
        Arguments.of(Operator.GTE, dtp(PRESENT), dtp(PAST), true),
        Arguments.of(Operator.GTE, dtp(PAST), dtp(PRESENT), false),
        Arguments.of(Operator.GTE, dtp(PAST), dtp(PAST), true),
        Arguments.of(Operator.LT, dtp(PRESENT), dtp(PAST), false),
        Arguments.of(Operator.LT, dtp(PAST), dtp(PRESENT), true),
        Arguments.of(Operator.LT, dtp(PAST), dtp(PAST), false),
        Arguments.of(Operator.LTE, dtp(PRESENT), dtp(PAST), false),
        Arguments.of(Operator.LTE, dtp(PAST), dtp(PRESENT), true),
        Arguments.of(Operator.LTE, dtp(PAST), dtp(PAST), true),
        // Duration type
        Arguments.of(Operator.EQ, dup(SMALL), dup(SMALL), true),
        Arguments.of(Operator.EQ, dup(SMALL), dup(LARGE), false),
        Arguments.of(Operator.NEQ, dup(SMALL), dup(LARGE), true),
        Arguments.of(Operator.NEQ, dup(SMALL), dup(SMALL), false),
        Arguments.of(Operator.GT, dup(MEDIUM), dup(SMALL), true),
        Arguments.of(Operator.GT, dup(SMALL), dup(MEDIUM), false),
        Arguments.of(Operator.GT, dup(SMALL), dup(SMALL), false),
        Arguments.of(Operator.GTE, dup(MEDIUM), dup(SMALL), true),
        Arguments.of(Operator.GTE, dup(SMALL), dup(MEDIUM), false),
        Arguments.of(Operator.GTE, dup(SMALL), dup(SMALL), true),
        Arguments.of(Operator.LT, dup(MEDIUM), dup(SMALL), false),
        Arguments.of(Operator.LT, dup(SMALL), dup(MEDIUM), true),
        Arguments.of(Operator.LT, dup(SMALL), dup(SMALL), false),
        Arguments.of(Operator.LTE, dup(MEDIUM), dup(SMALL), false),
        Arguments.of(Operator.LTE, dup(SMALL), dup(MEDIUM), true),
        Arguments.of(Operator.LTE, dup(SMALL), dup(SMALL), true));
  }

  public static Stream<Arguments> testEvaluateComparisonsWithIncompatiblePrimitivesProvider() {
    return Stream.of(
        Arguments.of(Operator.GT, ip(1), sp("a")),
        Arguments.of(Operator.GTE, ip(1), bp(false)),
        Arguments.of(Operator.LT, ip(1), dtp(PAST)),
        Arguments.of(Operator.LTE, ip(1), dup(SMALL)),
        Arguments.of(Operator.GT, fp(1), sp("a")),
        Arguments.of(Operator.GTE, fp(1), bp(false)),
        Arguments.of(Operator.LT, fp(1), dtp(PAST)),
        Arguments.of(Operator.LTE, fp(1), dup(SMALL)),
        Arguments.of(Operator.GT, sp("a"), ip(1)),
        Arguments.of(Operator.GTE, sp("a"), bp(false)),
        Arguments.of(Operator.LT, sp("a"), dtp(PAST)),
        Arguments.of(Operator.LTE, sp("a"), dup(SMALL)),
        Arguments.of(Operator.GTE, bp(false), ip(1)),
        Arguments.of(Operator.GT, bp(false), sp("a")),
        Arguments.of(Operator.LT, bp(false), dtp(PAST)),
        Arguments.of(Operator.LTE, bp(false), dup(SMALL)),
        Arguments.of(Operator.LT, dtp(PAST), ip(1)),
        Arguments.of(Operator.GT, dtp(PAST), sp("a")),
        Arguments.of(Operator.GTE, dtp(PAST), bp(false)),
        Arguments.of(Operator.LTE, dtp(PAST), dup(SMALL)),
        Arguments.of(Operator.LTE, dup(SMALL), ip(1)),
        Arguments.of(Operator.GT, dup(SMALL), sp("a")),
        Arguments.of(Operator.GTE, dup(SMALL), bp(false)),
        Arguments.of(Operator.LT, dup(SMALL), dtp(PAST)));
  }

  public static Stream<Arguments> testEvaluateConjunctionsProvider() {
    return Stream.of(
        Arguments.of(LogicalOperator.OR, false, false, false),
        Arguments.of(LogicalOperator.OR, true, false, true),
        Arguments.of(LogicalOperator.OR, false, true, true),
        Arguments.of(LogicalOperator.OR, true, true, true),
        Arguments.of(LogicalOperator.AND, false, false, false),
        Arguments.of(LogicalOperator.AND, true, false, false),
        Arguments.of(LogicalOperator.AND, false, true, false),
        Arguments.of(LogicalOperator.AND, true, true, true));
  }

  private static Primitive ip(long v) {
    return Primitive.ofIntegerValue(v);
  }

  private static Primitive fp(double v) {
    return Primitive.ofFloatValue(v);
  }

  private static Primitive sp(String v) {
    return Primitive.ofStringValue(v);
  }

  private static Primitive bp(boolean v) {
    return Primitive.ofBooleanValue(v);
  }

  private static Primitive dtp(Instant v) {
    return Primitive.ofDatetime(v);
  }

  private static Primitive dup(Duration v) {
    return Primitive.ofDuration(v);
  }
}
