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

import static java.util.stream.Collectors.toMap;

import java.util.HashMap;
import java.util.Map;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.Variable;

public class TestSdkType extends SdkType<Map<String, Literal>> {

  private final Map<String, Variable> variableMap;

  private TestSdkType(Map<String, Variable> variableMap) {
    this.variableMap = variableMap;
  }

  public static TestSdkType of(String k1, LiteralType v1) {
    Map<String, LiteralType> types = new HashMap<>();
    types.put(k1, v1);

    return of(types);
  }

  public static TestSdkType of(String k1, LiteralType v1, String k2, LiteralType v2) {
    Map<String, LiteralType> types = new HashMap<>();
    types.put(k1, v1);
    types.put(k2, v2);

    return of(types);
  }

  public static TestSdkType of(Map<String, LiteralType> literalTypes) {
    Map<String, Variable> variableMap =
        literalTypes.entrySet().stream()
            .collect(
                toMap(
                    Map.Entry::getKey,
                    entry ->
                        Variable.builder().literalType(entry.getValue()).description("").build()));

    return new TestSdkType(variableMap);
  }

  @Override
  public Map<String, Literal> toLiteralMap(Map<String, Literal> value) {
    return value;
  }

  @Override
  public Map<String, Literal> fromLiteralMap(Map<String, Literal> value) {
    return value;
  }

  @Override
  public Map<String, Variable> getVariableMap() {
    return variableMap;
  }
}
