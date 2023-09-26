/*
 * Copyright 2020-2023 Flyte Authors.
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
package org.flyte.api.v1;

import com.google.auto.value.AutoOneOf;

/**
 * Defines a boolean expression tree. It can be a simple or a conjunction expression. Multiple
 * expressions can be combined using a conjunction or a disjunction to result in a final boolean
 * result.
 */
@AutoOneOf(BooleanExpression.Kind.class)
public abstract class BooleanExpression {
  public enum Kind {
    CONJUNCTION,
    COMPARISON
  }

  public abstract Kind kind();

  public abstract ComparisonExpression comparison();

  public abstract ConjunctionExpression conjunction();

  public static BooleanExpression ofComparison(ComparisonExpression comparison) {
    return AutoOneOf_BooleanExpression.comparison(comparison);
  }

  public static BooleanExpression ofConjunction(ConjunctionExpression conjunction) {
    return AutoOneOf_BooleanExpression.conjunction(conjunction);
  }
}
