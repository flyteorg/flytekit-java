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

import static org.flyte.flytekit.SdkBooleanExpression.ofComparison;

import java.util.List;
import org.flyte.api.v1.ComparisonExpression;

public class SdkConditions {
  private SdkConditions() {}

  /**
   * Creates a new {@link SdkCondition} with a case clause that would execute {@code then}
   * transformation if the {@code condition} expression evaluates to true.
   *
   * @param name name for the case, it must be unique among all the case clauses in the {@link
   *     SdkCondition}.
   * @param condition expression of the case clause. The transformation must have no inputs.
   * @param then the transformation to apply if {@code condition} is true
   * @param <OutputT> the output type of the {@code then} transformation and therefore for the
   *     condition.
   * @return a condition
   */
  public static <OutputT> SdkCondition<OutputT> when(
      String name, SdkBooleanExpression condition, SdkTransform<Void, OutputT> then) {
    SdkConditionCase<OutputT> case_ = SdkConditionCase.create(name, condition, then);

    return new SdkCondition<>(List.of(case_), null, null);
  }

  /**
   * Creates a new {@link SdkCondition} with a case clause that would execute {@code then}
   * transformation if the {@code condition} expression evaluates to true.
   *
   * @param name name for the case, it must be unique among all the case clauses in the {@link
   *     SdkCondition}.
   * @param condition expression of the case clause. The transformation must have no inputs.
   * @param then the transformation to apply if {@code condition} is true.
   * @param inputs the inputs to apply to the {@code then} transform when the {@code condition}
   *     evaluates to true.
   * @param <InputT> the input type of the {@code then} transformation.
   * @param <OutputT> the output type of the {@code then} transformation and therefore for the
   *     condition.
   * @return a condition
   */
  public static <InputT, OutputT> SdkCondition<OutputT> when(
      String name,
      SdkBooleanExpression condition,
      SdkTransform<InputT, OutputT> then,
      InputT inputs) {
    return when(name, condition, new SdkAppliedTransform<>(then, inputs));
  }

  /**
   * Return a {@link SdkBooleanExpression} for {@code left == right}.
   * @param left first operand
   * @param right second operand
   * @return the boolean expression
   * @param <T> type for {@code left } and {@code right}
   */
  public static <T> SdkBooleanExpression eq(SdkBindingData<T> left, SdkBindingData<T> right) {
    return ofComparison(
        SdkComparisonExpression.create(left, right, ComparisonExpression.Operator.EQ));
  }

  /**
   * Return a {@link SdkBooleanExpression} for {@code left != right}.
   * @param left first operand
   * @param right second operand
   * @return the boolean expression
   * @param <T> type for {@code left } and {@code right}
   */
  public static <T> SdkBooleanExpression neq(SdkBindingData<T> left, SdkBindingData<T> right) {
    return ofComparison(
        SdkComparisonExpression.create(left, right, ComparisonExpression.Operator.NEQ));
  }

  /**
   * Return a {@link SdkBooleanExpression} for {@code left > right}.
   * @param left first operand
   * @param right second operand
   * @return the boolean expression
   * @param <T> type for {@code left } and {@code right}
   */
  public static <T> SdkBooleanExpression gt(SdkBindingData<T> left, SdkBindingData<T> right) {
    return ofComparison(
        SdkComparisonExpression.create(left, right, ComparisonExpression.Operator.GT));
  }

  /**
   * Return a {@link SdkBooleanExpression} for {@code left >= right}.
   * @param left first operand
   * @param right second operand
   * @return the boolean expression
   * @param <T> type for {@code left } and {@code right}
   */
  public static <T> SdkBooleanExpression gte(SdkBindingData<T> left, SdkBindingData<T> right) {
    return ofComparison(
        SdkComparisonExpression.create(left, right, ComparisonExpression.Operator.GTE));
  }

  /**
   * Return a {@link SdkBooleanExpression} for {@code left < right}.
   * @param left first operand
   * @param right second operand
   * @return the boolean expression
   * @param <T> type for {@code left } and {@code right}
   */
  public static <T> SdkBooleanExpression lt(SdkBindingData<T> left, SdkBindingData<T> right) {
    return ofComparison(
        SdkComparisonExpression.create(left, right, ComparisonExpression.Operator.LT));
  }

  /**
   * Return a {@link SdkBooleanExpression} for {@code left <= right}.
   * @param left first operand
   * @param right second operand
   * @return the boolean expression
   * @param <T> type for {@code left } and {@code right}
   */
  public static <T> SdkBooleanExpression lte(SdkBindingData<T> left, SdkBindingData<T> right) {
    return ofComparison(
        SdkComparisonExpression.create(left, right, ComparisonExpression.Operator.LTE));
  }

  /**
   * Return a {@link SdkBooleanExpression} for {@code data == true}.
   * @param data data to compare
   * @return the boolean expression
   */
  public static SdkBooleanExpression isTrue(SdkBindingData<Boolean> data) {
    return ofComparison(
        SdkComparisonExpression.create(
            data, SdkBindingData.ofBoolean(true), ComparisonExpression.Operator.EQ));
  }

  /**
   * Return a {@link SdkBooleanExpression} for {@code data == false}.
   * @param data data to compare
   * @return the boolean expression
   */
  public static SdkBooleanExpression isFalse(SdkBindingData<Boolean> data) {
    return ofComparison(
        SdkComparisonExpression.create(
            data, SdkBindingData.ofBoolean(false), ComparisonExpression.Operator.EQ));
  }
}
