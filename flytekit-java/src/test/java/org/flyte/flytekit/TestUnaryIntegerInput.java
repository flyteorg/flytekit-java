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
package org.flyte.flytekit;

import com.google.auto.value.AutoValue;
import java.util.Map;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.Variable;

@AutoValue
abstract class TestUnaryIntegerInput {

  abstract SdkBindingData<Long> in();

  public static TestUnaryIntegerInput create(SdkBindingData<Long> in) {
    return new AutoValue_TestUnaryIntegerInput(in);
  }

  public static class SdkType extends org.flyte.flytekit.SdkType<TestUnaryIntegerInput> {

    private static final String VAR = "in";
    private static final SdkLiteralType<Long> INTEGERS = SdkLiteralTypes.integers();

    @Override
    public Map<String, Literal> toLiteralMap(TestUnaryIntegerInput value) {
      return Map.of(VAR, Literals.ofInteger(value.in().get()));
    }

    @Override
    public TestUnaryIntegerInput fromLiteralMap(Map<String, Literal> value) {
      return create(SdkBindingDataFactory.of(value.get(VAR).scalar().primitive().integerValue()));
    }

    @Override
    public TestUnaryIntegerInput promiseFor(String nodeId) {
      return create(SdkBindingData.promise(INTEGERS, nodeId, VAR));
    }

    @Override
    public Map<String, Variable> getVariableMap() {
      return Map.of(
          VAR, Variable.builder().literalType(INTEGERS.getLiteralType()).description("").build());
    }

    @Override
    public Map<String, SdkLiteralType<?>> toLiteralTypes() {
      return Map.of(VAR, INTEGERS);
    }

    @Override
    public Map<String, SdkBindingData<?>> toSdkBindingMap(TestUnaryIntegerInput value) {
      return Map.of(VAR, value.in());
    }
  }
}
