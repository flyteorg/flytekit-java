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
package org.flyte.jflyte;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import flyteidl.core.Literals;
import java.time.Instant;
import java.util.stream.Stream;
import org.flyte.api.v1.Duration;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Timestamp;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ProtoUtilTest {

  @ParameterizedTest
  @MethodSource("createDeserializePrimitivesArguments")
  void shouldDeserializePrimitives(Literals.Primitive input, Primitive expected) {
    assertThat(ProtoUtil.deserialize(input), equalTo(expected));
  }

  static Stream<Arguments> createDeserializePrimitivesArguments() {
    Instant now = Instant.now();
    long seconds = now.getEpochSecond();
    int nanos = now.getNano();

    return Stream.of(
        Arguments.of(Literals.Primitive.newBuilder().setInteger(123).build(), Primitive.of(123)),
        Arguments.of(
            Literals.Primitive.newBuilder().setFloatValue(123.0).build(), Primitive.of(123.0)),
        Arguments.of(
            Literals.Primitive.newBuilder().setStringValue("123").build(), Primitive.of("123")),
        Arguments.of(Literals.Primitive.newBuilder().setBoolean(true).build(), Primitive.of(true)),
        Arguments.of(
            Literals.Primitive.newBuilder()
                .setDatetime(
                    com.google.protobuf.Timestamp.newBuilder()
                        .setSeconds(seconds)
                        .setNanos(nanos)
                        .build())
                .build(),
            Primitive.of(Timestamp.create(seconds, nanos))),
        Arguments.of(
            Literals.Primitive.newBuilder()
                .setDuration(
                    com.google.protobuf.Duration.newBuilder()
                        .setSeconds(seconds)
                        .setNanos(nanos)
                        .build())
                .build(),
            Primitive.of(Duration.create(seconds, nanos))));
  }
}
