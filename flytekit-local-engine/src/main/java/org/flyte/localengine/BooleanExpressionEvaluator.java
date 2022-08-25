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

import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;
import org.flyte.api.v1.*;
import org.flyte.api.v1.Scalar.Kind;

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
        return neq(left, right);
      case GT:
        return gt(left, right);
      case GTE:
        return gte(left, right);
      case LT:
        return lt(left, right);
      case LTE:
        return lte(left, right);
    }

    throw new AssertionError("Unexpected ComparisonExpression.Operator: " + comparison.operator());
  }

  private static boolean gt(Primitive left, Primitive right) {
    return compare(left, right, cmp -> cmp > 0);
  }

  private static boolean gte(Primitive left, Primitive right) {
    return compare(left, right, cmp -> cmp >= 0);
  }

  private static boolean lt(Primitive left, Primitive right) {
    return compare(left, right, cmp -> cmp < 0);
  }

  private static boolean lte(Primitive left, Primitive right) {
    return compare(left, right, cmp -> cmp <= 0);
  }

  private static boolean compare(Primitive left, Primitive right, Predicate<Integer> cmp) {
    return cmp.test(compare(left, right));
  }

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
    assert left.kind() == Primitive.Kind.INTEGER_VALUE;
    if (right.kind() == Primitive.Kind.FLOAT_VALUE) {
      long integerLeft = left.integerValue();
      double floatRight = right.floatValue();
      return ((double) integerLeft) == floatRight;
    }
    return Objects.equals(left, right);
  }

  private static boolean floatEq(Primitive left, Primitive right) {
    assert left.kind() == Primitive.Kind.FLOAT_VALUE;
    if (right.kind() == Primitive.Kind.INTEGER_VALUE) {
      double floatLeft = left.floatValue();
      long integerRight = right.integerValue();
      return floatLeft == (double) integerRight;
    }
    return Objects.equals(left, right);
  }

  private static boolean neq(Primitive left, Primitive right) {
    return !eq(left, right);
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
            || literal.scalar().kind() != Kind.PRIMITIVE) {
          throw new IllegalArgumentException(
              String.format("Variable [%s] not a primitive: %s", operand.var(), literal));
        }
        return literal.scalar().primitive();
    }

    throw new AssertionError("Unexpected Operand.Kind: " + operand.kind());
  }
}
