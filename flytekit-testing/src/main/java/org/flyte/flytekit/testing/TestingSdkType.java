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
package org.flyte.flytekit.testing;

import java.util.Map;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.Variable;
import org.flyte.flytekit.SdkType;

public class TestingSdkType extends SdkType<Map<String, Literal>> {

  private final Map<String, Variable> variableMap;

  private TestingSdkType(Map<String, Variable> variableMap) {
    this.variableMap = variableMap;
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

  public static SdkType<Map<String, Literal>> of(Map<String, Variable> intf) {
    return new TestingSdkType(intf);
  }
}
