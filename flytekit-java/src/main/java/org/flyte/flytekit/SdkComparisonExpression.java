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
import org.flyte.api.v1.ComparisonExpression;

/**
 * Denotes a comparison of two {@link SdkBindingData}.
 *
 * @param <T> type of the comparison.
 */
@AutoValue
abstract class SdkComparisonExpression<T> {
  abstract SdkBindingData<T> left();

  abstract SdkBindingData<T> right();

  abstract ComparisonExpression.Operator operator();

  static <T> SdkComparisonExpression<T> create(
      SdkBindingData<T> left, SdkBindingData<T> right, ComparisonExpression.Operator operator) {
    return new AutoValue_SdkComparisonExpression<>(left, right, operator);
  }
}
