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
import java.util.Map;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.Variable;

@AutoValue
abstract class TestPairIntegerInput {
  abstract SdkBindingData<Long> a();

  abstract SdkBindingData<Long> b();

  public static TestPairIntegerInput create(SdkBindingData<Long> a, SdkBindingData<Long> b) {
    return new AutoValue_TestPairIntegerInput(a, b);
  }

  public static class SdkType extends org.flyte.flytekit.SdkType<TestPairIntegerInput> {

    @Override
    public Map<String, Literal> toLiteralMap(TestPairIntegerInput value) {
      return Map.of(
          "a", Literals.ofInteger(value.a().get()),
          "b", Literals.ofInteger(value.b().get()));
    }

    @Override
    public TestPairIntegerInput fromLiteralMap(Map<String, Literal> value) {
      return create(
          SdkBindingData.ofInteger(value.get("a").scalar().primitive().integerValue()),
          SdkBindingData.ofInteger(value.get("b").scalar().primitive().integerValue()));
    }

    @Override
    public TestPairIntegerInput promiseFor(String nodeId) {
      return create(
          SdkBindingData.ofOutputReference(nodeId, "a", LiteralTypes.INTEGER),
          SdkBindingData.ofOutputReference(nodeId, "b", LiteralTypes.INTEGER));
    }

    @Override
    public Map<String, Variable> getVariableMap() {
      return Map.of(
          "a", Variable.builder().literalType(LiteralTypes.INTEGER).build(),
          "b", Variable.builder().literalType(LiteralTypes.INTEGER).build());
    }
  }
}
