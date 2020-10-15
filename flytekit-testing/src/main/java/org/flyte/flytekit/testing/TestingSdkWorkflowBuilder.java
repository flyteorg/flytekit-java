/*
 * Copyright 2020 Spotify AB.
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

import static org.flyte.flytekit.testing.Preconditions.checkArgument;

import java.util.Map;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.LiteralType;
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkWorkflowBuilder;

class TestingSdkWorkflowBuilder extends SdkWorkflowBuilder {

  private final Map<String, Literal> fixedInputMap;
  private final Map<String, LiteralType> fixedInputTypeMap;

  TestingSdkWorkflowBuilder(
      Map<String, Literal> fixedInputMap, Map<String, LiteralType> fixedInputTypeMap) {
    this.fixedInputMap = fixedInputMap;
    this.fixedInputTypeMap = fixedInputTypeMap;
  }

  @Override
  public SdkBindingData inputOf(String name, LiteralType literalType, String help) {
    LiteralType fixedInputType = fixedInputTypeMap.get(name);
    Literal fixedInput = fixedInputMap.get(name);

    checkArgument(
        fixedInputType != null,
        "Fixed input [%s] (of type %s) isn't defined, use SdkTestingExecutor#withFixedInput",
        name,
        LiteralTypes.toPrettyString(literalType));

    checkArgument(
        fixedInputType.equals(literalType),
        "Fixed input [%s] (of type %s) doesn't match expected type %s",
        name,
        LiteralTypes.toPrettyString(fixedInputType),
        LiteralTypes.toPrettyString(literalType));

    return SdkBindingData.create(Literals.toBindingData(fixedInput), fixedInputType);
  }
}
