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

@AutoValue
abstract class SdkConditionCase<OutputT> {
  abstract String name();

  abstract SdkBooleanExpression condition();

  abstract SdkTransform<Void, OutputT> then();

  static <OutputT> SdkConditionCase<OutputT> create(
      String name, SdkBooleanExpression condition, SdkTransform<Void, OutputT> then) {
    return new AutoValue_SdkConditionCase<>(name, condition, then);
  }
}
