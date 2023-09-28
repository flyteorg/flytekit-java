/*
 * Copyright 2022 Flyte Authors.
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

import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import org.flyte.api.v1.BooleanExpression;
import org.flyte.api.v1.ComparisonExpression;
import org.flyte.api.v1.ConjunctionExpression;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.Operand;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;

class BooleanExpressionEvaluator {

  private BooleanExpressionEvaluator() {
    // Prevent instantiation
  }

  static boolean evaluate(BooleanExpression condition, Map<String, Literal> inputs) {
    switch (condition.kind()) {
      case CONJUNCTION:
        return evaluate(condition.conjunction(), inputs);
      case COMPARISON:
        return evaluate(condition.comparison(), inputs);
    }
    throw new AssertionError("Unexpected BooleanExpression.Kind: " + condition.kind());
  }

  private static boolean evaluate(ConjunctionExpression conjunction, Map<String, Literal> inputs) {
    boolean leftValue = evaluate(conjunction.leftExpression(), inputs);
    boolean rightValue = evaluate(conjunction.rightExpression(), inputs);

    switch (conjunction.operator()) {
      case AND:
        return leftValue && rightValue;
      case OR:
        return leftValue || rightValue;
    }

    throw new AssertionError(
        "Unexpected ConjunctionExpression.LogicalOperator: " + conjunction.operator());
  }

  private static boolean evaluate(ComparisonExpression comparison, Map<String, Literal> inputs) {
    Primitive left = resolve(comparison.leftValue(), inputs);
    Primitive right = resolve(comparison.rightValue(), inputs);
    switch (comparison.operator()) {
      case EQ:
        return eq(left, right);
      case NEQ:
        return !eq(left, right);
      case GT:
        return compare(left, right) > 0;
      case GTE:
        return compare(left, right) >= 0;
      case LT:
        return compare(left, right) < 0;
      case LTE:
        return compare(left, right) <= 0;
    }

    throw new AssertionError("Unexpected ComparisonExpression.Operator: " + comparison.operator());
  }

  /**
   * Compares two primitives for equality but taking type coercion of integers to float into
   * account. Type coercion means that comparing 1 == 1.0 would return true as the left hand side 1
   * will be implicitly converted to 1.0 before being compared with the right hand side 1.0.
   *
   * @param left left hand side of the equality comparison.
   * @param right right hand side of the equality comparison.
   * @return true if {@code left} and {@code right} represent the same primitive when type coercion
   *     of integers into float is taken into account, false otherwise.
   */
  private static boolean eq(Primitive left, Primitive right) {
    switch (left.kind()) {
      case INTEGER_VALUE:
        return integerEq(left, right);
      case FLOAT_VALUE:
        return floatEq(left, right);
      default:
        return Objects.equals(left, right);
    }
  }

  private static boolean integerEq(Primitive left, Primitive right) {
    if (right.kind() == Primitive.Kind.FLOAT_VALUE) {
      long integerLeft = left.integerValue();
      double floatRight = right.floatValue();
      return ((double) integerLeft) == floatRight;
    }
    return Objects.equals(left, right);
  }

  private static boolean floatEq(Primitive left, Primitive right) {
    if (right.kind() == Primitive.Kind.INTEGER_VALUE) {
      double floatLeft = left.floatValue();
      long integerRight = right.integerValue();
      return floatLeft == (double) integerRight;
    }
    return Objects.equals(left, right);
  }

  /**
   * Compares its two comparable arguments for order. Returns a negative integer, zero, or a
   * positive integer as the first argument is less than, equal to, or greater than the second. The
   * primitives are compared as follows:
   *
   * <dl>
   *   <dt>integers, floats
   *   <dd>numerically
   *   <dt>string
   *   <dd>lexicographically
   *   <dt>boolean
   *   <dd>false are considered less than true
   *   <dt>datetime
   *   <dd>based on the time-line position of the date-times
   *   <dt>duration
   *   <dd>based on the total length of the duration
   * </dl>
   *
   * @throws IllegalArgumentException if the two arguments are not comparable with each other. In
   *     general both must have the same type but integers can be type coerced into floats.
   */
  private static int compare(Primitive left, Primitive right) {
    switch (left.kind()) {
      case INTEGER_VALUE:
        return compareIntegers(left, right);
      case FLOAT_VALUE:
        return compareFloats(left, right);
      case STRING_VALUE:
        return compare(left, right, Primitive.Kind.STRING_VALUE, Primitive::stringValue);
      case BOOLEAN_VALUE:
        return compare(left, right, Primitive.Kind.BOOLEAN_VALUE, Primitive::booleanValue);
      case DATETIME:
        return compare(left, right, Primitive.Kind.DATETIME, Primitive::datetime);
      case DURATION:
        return compare(left, right, Primitive.Kind.DURATION, Primitive::duration);
      default:
        throw new AssertionError("Unexpected Primitive.Kind:" + left.kind());
    }
  }

  private static int compareIntegers(Primitive left, Primitive right) {
    long integerLeft = left.integerValue();
    switch (right.kind()) {
      case INTEGER_VALUE:
        long integerRight = right.integerValue();
        return Long.compare(integerLeft, integerRight);
      case FLOAT_VALUE:
        // type coercion
        double floatRight = right.floatValue();
        return Double.compare((double) integerLeft, floatRight);
      default: // fall out
    }
    throwPrimitivesNotCompatible(left, right);
    return 0; // unreachable
  }

  private static int compareFloats(Primitive left, Primitive right) {
    double floatLeft = left.floatValue();
    switch (right.kind()) {
      case INTEGER_VALUE:
        // type coercion
        long integerRight = right.integerValue();
        return Double.compare(floatLeft, (double) integerRight);
      case FLOAT_VALUE:
        double floatRight = right.floatValue();
        return Double.compare(floatLeft, floatRight);
      default: // fall out
    }
    throwPrimitivesNotCompatible(left, right);
    return 0; // unreachable
  }

  private static <T extends Comparable<T>> int compare(
      Primitive left,
      Primitive right,
      Primitive.Kind expectedKind,
      Function<Primitive, T> converter) {
    if (!(left.kind() == right.kind() && left.kind() == expectedKind)) {
      throwPrimitivesNotCompatible(left, right);
    }
    T valueLeft = converter.apply(left);
    T valueRight = converter.apply(right);

    return valueLeft.compareTo(valueRight);
  }

  private static void throwPrimitivesNotCompatible(Primitive left, Primitive right) {
    throw new IllegalArgumentException(
        String.format("Operands are not comparable: [%s] <-> [%s]", left, right));
  }

  private static Primitive resolve(Operand operand, Map<String, Literal> inputs) {
    switch (operand.kind()) {
      case PRIMITIVE:
        return operand.primitive();
      case VAR:
        Literal literal = inputs.get(operand.var());
        if (literal == null) {
          throw new IllegalArgumentException(
              String.format("Variable [%s] not in inputs: %s", operand.var(), inputs));
        } else if (literal.kind() != Literal.Kind.SCALAR
            || literal.scalar().kind() != Scalar.Kind.PRIMITIVE) {
          throw new IllegalArgumentException(
              String.format("Variable [%s] not a primitive: %s", operand.var(), literal));
        }
        return literal.scalar().primitive();
    }

    throw new AssertionError("Unexpected Operand.Kind: " + operand.kind());
  }
}
