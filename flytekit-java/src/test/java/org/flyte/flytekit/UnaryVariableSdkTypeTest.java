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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.Map;
import java.util.Set;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;
import org.flyte.api.v1.Variable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class UnaryVariableSdkTypeTest {
  private static final String VAR_NAME = "x";
  private SdkType<SdkBindingData<Long>> sdkType;

  @BeforeEach
  void setUp() {
    sdkType = SdkLiteralTypes.integers().asSdkType(VAR_NAME, "some description");
  }

  @Test
  void testVariableNames() {
    assertThat(sdkType.variableNames(), equalTo(Set.of(VAR_NAME)));
  }

  @Test
  void testGetVariableMap() {
    assertThat(
        sdkType.getVariableMap(),
        equalTo(
            Map.of(
                VAR_NAME,
                Variable.builder()
                    .literalType(LiteralTypes.INTEGER)
                    .description("some description")
                    .build())));
  }

  @Test
  void testToLiteralTypes() {
    assertThat(sdkType.toLiteralTypes(), equalTo(Map.of(VAR_NAME, SdkLiteralTypes.integers())));
  }

  @Test
  void testToLiteralMap() {
    assertThat(
        sdkType.toLiteralMap(SdkBindingDataFactory.of(3L)),
        equalTo(Map.of(VAR_NAME, integerLiteral(3L))));
  }

  @Test
  void testToSdkBindingMap() {
    assertThat(
        sdkType.toSdkBindingMap(SdkBindingDataFactory.of(3L)),
        equalTo(Map.of(VAR_NAME, SdkBindingDataFactory.of(3L))));
  }

  @Test
  void testFromLiteralMap() {
    var literalMap = Map.of(VAR_NAME, integerLiteral(5L));

    assertThat(sdkType.fromLiteralMap(literalMap), equalTo(SdkBindingDataFactory.of(5L)));
  }

  @Test
  void testPromiseFor() {
    assertThat(
        sdkType.promiseFor("fib0"),
        equalTo(SdkBindingData.promise(SdkLiteralTypes.integers(), "fib0", VAR_NAME)));
  }

  @Test
  void testPromiseMapFor() {
    assertThat(
        sdkType.promiseMapFor("fib0"),
        equalTo(
            Map.of(
                VAR_NAME, SdkBindingData.promise(SdkLiteralTypes.integers(), "fib0", VAR_NAME))));
  }

  private static Literal integerLiteral(long integerValue) {
    return Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(integerValue)));
  }
}
