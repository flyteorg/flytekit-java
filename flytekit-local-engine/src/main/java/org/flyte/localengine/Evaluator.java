package org.flyte.localengine;

import org.flyte.api.v1.*;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;

class Evaluator {
     boolean evaluate(BooleanExpression condition, Map<String, Literal> inputs) {
    switch (condition.kind()) {
      case CONJUNCTION:
        return evaluate(condition.conjunction(), inputs);
      case COMPARISON:
        return evaluate(condition.comparison(), inputs);
    }
    throw new AssertionError("Unexpected BooleanExpression.Kind: " + condition.kind());
  }

    private  boolean evaluate(ConjunctionExpression conjunction, Map<String, Literal> inputs) {
    boolean leftValue = evaluate(conjunction.leftExpression(), inputs);
    boolean rightValue = evaluate(conjunction.rightExpression(), inputs);

    switch (conjunction.operator()) {
      case AND:
        return leftValue && rightValue;
      case OR:
        return leftValue || rightValue;
    }

    throw new AssertionError("Unexpected ConjunctionExpression.LogicalOperator: " + conjunction.operator());
  }

    private  boolean evaluate(ComparisonExpression comparison, Map<String, Literal> inputs) {
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

    private  boolean gt(Primitive left, Primitive right) {
    return compare(left, right, cmp -> cmp > 0);

  }

    private  boolean gte(Primitive left, Primitive right) {
    return compare(left, right, cmp -> cmp >= 0);
  }

    private  boolean lt(Primitive left, Primitive right) {
    return compare(left, right, cmp -> cmp < 0);

  }

    private  boolean lte(Primitive left, Primitive right) {
    return compare(left, right, cmp -> cmp <= 0);
  }

    private  boolean compare(Primitive left, Primitive right, Predicate<Integer> cmp) {
    return cmp.test(compare(left, right));
  }

    private  int compare(Primitive left, Primitive right) {
    switch (left.kind()) {
      case INTEGER_VALUE:
        return compareIntegers(left, right);
      case FLOAT_VALUE:
        return compareFloats(left, right);
      case STRING_VALUE:
        return compare(left, right, Primitive.Kind.STRING_VALUE, this::asString);
      case BOOLEAN_VALUE:
        return compare(left, right, Primitive.Kind.BOOLEAN_VALUE, this::asBoolean);
      case DATETIME:
        return compare(left, right, Primitive.Kind.DATETIME, this::asDateTime);
      case DURATION:
        return compare(left, right, Primitive.Kind.DURATION, this::asDuration);
      default:
        throw new AssertionError("Unexpected Primitive.Kind:" + left.kind());
    }
  }

    private  int compareIntegers(Primitive left,
                                       Primitive right) {
    long integerLeft = asInteger(left);
    switch (right.kind()) {
      case INTEGER_VALUE:
        long integerRight = asInteger(right);
        return Long.compare(integerLeft, integerRight);
      case FLOAT_VALUE:
        // type coercion
        double floatRight = asFloat(right);
        return Double.compare((double) integerLeft, floatRight);
      default: // fall out
    }
    throwPrimitivesNotCompatible(left, right);
    return 0; // unreachable
  }

    private  int compareFloats(Primitive left,
                                     Primitive right) {
    double floatLeft = asFloat(left);
    switch (right.kind()) {
      case INTEGER_VALUE:
        // type coercion
        long integerRight = asInteger(right);
        return Double.compare(floatLeft, (double) integerRight);
      case FLOAT_VALUE:
        double floatRight = asFloat(right);
        return Double.compare(floatLeft, floatRight);
      default: // fall out
    }
    throwPrimitivesNotCompatible(left, right);
    return 0; // unreachable
  }

    private  <T extends Comparable<T>> int compare(Primitive left, Primitive right,
                                                         Primitive.Kind expectedKind, Function<Primitive, T> converter) {
    if (!(left.kind() == right.kind() && left.kind() == expectedKind)) {
      throwPrimitivesNotCompatible(left, right);
    }
    T valueLeft = converter.apply(left);
    T valueRight = converter.apply(right);

    return valueLeft.compareTo(valueRight);
  }

    private  void throwPrimitivesNotCompatible(Primitive left, Primitive right) {
    throw new IllegalArgumentException(String.format("Operands are not comparable: [%s] <-> [%s]", left, right));
  }

    private  long asInteger(Primitive primitive) {
    if (primitive.kind() != Primitive.Kind.INTEGER_VALUE) {
      throw new IllegalArgumentException(String.format("Primitive [%s] is not an integer", primitive));
    }
    return primitive.integerValue();
  }

    private  double asFloat(Primitive primitive) {
    if (primitive.kind() != Primitive.Kind.FLOAT_VALUE) {
      throw new IllegalArgumentException(String.format("Primitive [%s] is not a float", primitive));
    }
    return primitive.floatValue();
  }

    private  String asString(Primitive primitive) {
    if (primitive.kind() != Primitive.Kind.STRING_VALUE) {
      throw new IllegalArgumentException(String.format("Primitive [%s] is not a string", primitive));
    }
    return primitive.stringValue();
  }

    private boolean asBoolean(Primitive primitive) {
    if (primitive.kind() != Primitive.Kind.BOOLEAN_VALUE) {
      throw new IllegalArgumentException(String.format("Primitive [%s] is not a boolean", primitive));
    }
    return primitive.booleanValue();
  }

    private  Instant asDateTime(Primitive primitive) {
    if (primitive.kind() != Primitive.Kind.DATETIME) {
      throw new IllegalArgumentException(String.format("Primitive [%s] is not a datetime", primitive));
    }
    return primitive.datetime();
  }

    private  Duration asDuration(Primitive primitive) {
    if (primitive.kind() != Primitive.Kind.DURATION) {
      throw new IllegalArgumentException(String.format("Primitive [%s] is not a duration", primitive));
    }
    return primitive.duration();
  }

    private  boolean eq(Primitive left, Primitive right) {
       switch (left.kind()){
         case INTEGER_VALUE:
           if (right.kind() == Primitive.Kind.FLOAT_VALUE) {
             long integerLeft = asInteger(left);
             double floatRight = asFloat(right);
             return Objects.equals((double) integerLeft, floatRight);
           }
           return Objects.equals(left, right);
         case FLOAT_VALUE:
           if (right.kind() ==  Primitive.Kind.INTEGER_VALUE){
             double floatLeft = asFloat(left);
             long integerRight = asInteger(right);
             return Objects.equals(floatLeft, (double) integerRight);
           }
         default:
           return Objects.equals(left, right);
       }
  }

    private  boolean neq(Primitive left, Primitive right) {
    return !eq(left, right);
  }

    private  Primitive resolve(Operand operand, Map<String, Literal> inputs) {
    switch (operand.kind()) {
      case PRIMITIVE:
        return operand.primitive();
      case VAR:
        Literal literal = inputs.get(operand.var());
        if (literal == null) {
          throw new IllegalArgumentException("asassass"); //XXX
        } else if (literal.scalar() == null) {
          throw new IllegalArgumentException("asassass"); //XXX
        } else if (literal.scalar().primitive() == null) {
          throw new IllegalArgumentException("asassass"); //XXX
        }
        return literal.scalar().primitive();
    }
    return null;
  }
}
