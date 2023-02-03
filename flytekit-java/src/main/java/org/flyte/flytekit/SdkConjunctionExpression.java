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

import com.google.auto.value.AutoValue;
import org.flyte.api.v1.ConjunctionExpression;

/** Represent a conjunction expression: OR/AND. */
@AutoValue
abstract class SdkConjunctionExpression {
  abstract ConjunctionExpression.LogicalOperator operator();

  abstract SdkBooleanExpression leftExpression();

  abstract SdkBooleanExpression rightExpression();

  static SdkConjunctionExpression create(
      ConjunctionExpression.LogicalOperator operator,
      SdkBooleanExpression leftExpression,
      SdkBooleanExpression rightExpression) {
    return new AutoValue_SdkConjunctionExpression(operator, leftExpression, rightExpression);
  }
}
