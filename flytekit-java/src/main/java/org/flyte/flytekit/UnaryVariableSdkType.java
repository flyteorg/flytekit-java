/*
 * Copyright 2023 Flyte Authors.
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

import static java.util.Objects.requireNonNull;

import java.util.Map;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.Variable;

/**
 * Implementation of {@link SdkType} for types that only contain a single variable.
 *
 * @param <T> Native type for the type of the single variable.
 */
class UnaryVariableSdkType<T> extends SdkType<SdkBindingData<T>> {

  private final SdkLiteralType<T> sdkLiteralType;
  private final String varName;
  private final String varDescription;

  UnaryVariableSdkType(SdkLiteralType<T> sdkLiteralType, String varName, String varDescription) {
    this.sdkLiteralType = requireNonNull(sdkLiteralType, "the sdkLiteralType should not be null");
    this.varName = requireNonNull(varName, "the variable name cannot be null");
    this.varDescription = requireNonNull(varDescription, "the description cannot be null");
  }

  @Override
  public Map<String, Literal> toLiteralMap(SdkBindingData<T> value) {
    return Map.of(varName, sdkLiteralType.toLiteral(value.get()));
  }

  @Override
  public SdkBindingData<T> fromLiteralMap(Map<String, Literal> literalMap) {
    if (!literalMap.containsKey(varName)) {
      throw new IllegalArgumentException(
          String.format("variable %s not found in literal map", varName));
    }
    return SdkBindingData.literal(
        sdkLiteralType, sdkLiteralType.fromLiteral(literalMap.get(varName)));
  }

  @Override
  public SdkBindingData<T> promiseFor(String nodeId) {
    return SdkBindingData.promise(sdkLiteralType, nodeId, varName);
  }

  @Override
  public Map<String, Variable> getVariableMap() {
    return Map.of(
        varName,
        Variable.builder()
            .literalType(sdkLiteralType.getLiteralType())
            .description(varDescription)
            .build());
  }

  @Override
  public Map<String, SdkLiteralType<?>> toLiteralTypes() {
    return Map.of(varName, sdkLiteralType);
  }

  @Override
  public Map<String, SdkBindingData<?>> toSdkBindingMap(SdkBindingData<T> value) {
    return Map.of(varName, value);
  }
}
