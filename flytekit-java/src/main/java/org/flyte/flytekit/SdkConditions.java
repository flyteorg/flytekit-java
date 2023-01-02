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

import static java.util.Collections.singletonList;
import static org.flyte.flytekit.SdkBooleanExpression.ofComparison;

import org.flyte.api.v1.ComparisonExpression;

public class SdkConditions {
  private SdkConditions() {}

  public static <OutputT>
      SdkCondition<OutputT> when(
          String name, SdkBooleanExpression condition, SdkTransform<OutputT> then) {
    SdkConditionCase<OutputT> case_ = SdkConditionCase.create(name, condition, then);

    return new SdkCondition<>(singletonList(case_), null, null);
  }

  public static SdkBooleanExpression eq(SdkBindingData<?> left, SdkBindingData<?> right) {
    return ofComparison(
        SdkComparisonExpression.create(left, right, ComparisonExpression.Operator.EQ));
  }

  public static SdkBooleanExpression neq(SdkBindingData<?> left, SdkBindingData<?> right) {
    return ofComparison(
        SdkComparisonExpression.create(left, right, ComparisonExpression.Operator.NEQ));
  }

  public static SdkBooleanExpression gt(SdkBindingData<?> left, SdkBindingData<?> right) {
    return ofComparison(
        SdkComparisonExpression.create(left, right, ComparisonExpression.Operator.GT));
  }

  public static SdkBooleanExpression gte(SdkBindingData<?> left, SdkBindingData<?> right) {
    return ofComparison(
        SdkComparisonExpression.create(left, right, ComparisonExpression.Operator.GTE));
  }

  public static SdkBooleanExpression lt(SdkBindingData<?> left, SdkBindingData<?> right) {
    return ofComparison(
        SdkComparisonExpression.create(left, right, ComparisonExpression.Operator.LT));
  }

  public static SdkBooleanExpression lte(SdkBindingData<?> left, SdkBindingData<?> right) {
    return ofComparison(
        SdkComparisonExpression.create(left, right, ComparisonExpression.Operator.LTE));
  }

  public static SdkBooleanExpression isTrue(SdkBindingData<?> data) {
    return ofComparison(
        SdkComparisonExpression.create(
            data, SdkBindingData.ofBoolean(true), ComparisonExpression.Operator.EQ));
  }

  public static SdkBooleanExpression isFalse(SdkBindingData<?> data) {
    return ofComparison(
        SdkComparisonExpression.create(
            data, SdkBindingData.ofBoolean(false), ComparisonExpression.Operator.EQ));
  }
}
