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
package org.flyte.flytekit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.time.Duration;
import java.time.Instant;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;
import org.junit.jupiter.api.Test;

class LiteralsTest {

  @Test
  void shouldCreateLiteralsForScalars() {
    Instant now = Instant.now();
    Duration duration = Duration.ofSeconds(123);
    assertAll(
        () ->
            assertThat(
                Literals.ofInteger(123L),
                is(Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(123L))))),
        () ->
            assertThat(
                Literals.ofFloat(12.3),
                is(Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofFloatValue(12.3))))),
        () ->
            assertThat(
                Literals.ofString("123"),
                is(Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofStringValue("123"))))),
        () ->
            assertThat(
                Literals.ofDatetime(now),
                is(Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofDatetime(now))))),
        () ->
            assertThat(
                Literals.ofDuration(duration),
                is(Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofDuration(duration))))));
  }
}
