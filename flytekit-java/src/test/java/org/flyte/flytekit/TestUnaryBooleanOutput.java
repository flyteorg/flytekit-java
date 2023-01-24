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
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.Variable;

@AutoValue
abstract class TestUnaryBooleanOutput {
  abstract SdkBindingData<Boolean> o();

  public static TestUnaryBooleanOutput create(SdkBindingData<Boolean> o) {
    return new AutoValue_TestUnaryBooleanOutput(o);
  }

  public static class SdkType extends org.flyte.flytekit.SdkType<TestUnaryBooleanOutput> {

    private static final String VAR = "o";
    private static final LiteralType LITERAL_TYPE = LiteralTypes.BOOLEAN;

    @Override
    public Map<String, Literal> toLiteralMap(TestUnaryBooleanOutput value) {
      return Map.of(VAR, Literals.ofBoolean(value.o().get()));
    }

    @Override
    public TestUnaryBooleanOutput fromLiteralMap(Map<String, Literal> value) {
      return create(SdkBindingData.ofBoolean(value.get(VAR).scalar().primitive().booleanValue()));
    }

    @Override
    public TestUnaryBooleanOutput promiseFor(String nodeId) {
      return create(SdkBindingData.ofOutputReference(nodeId, VAR, LITERAL_TYPE));
    }

    @Override
    public Map<String, Variable> getVariableMap() {
      return Map.of(VAR, Variable.builder().literalType(LITERAL_TYPE).build());
    }

    @Override
    public Map<String, SdkBindingData<?>> toSdkBindingMap(TestUnaryBooleanOutput value) {
      return Map.of(VAR, value.o());
    }
  }
}
