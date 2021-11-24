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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.stream.Stream;
import org.flyte.api.v1.NodeMetadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class SdkNodeMetadataTest {

  @Test
  void shouldRejectEmptyNames() {
    SdkNodeMetadata.Builder builder = SdkNodeMetadata.builder();

    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> builder.name("").build());

    assertThat(ex.getMessage(), equalTo("Node metadata name cannot empty string"));
  }

  @Test
  void shouldRejectNegativeTimeout() {
    SdkNodeMetadata.Builder builder = SdkNodeMetadata.builder();

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class, () -> builder.timeout(Duration.ofMillis(-1)).build());

    assertThat(ex.getMessage(), equalTo("Node metadata timeout cannot be negative: PT-0.001S"));
  }

  @ParameterizedTest
  @MethodSource("metadataIdl")
  void shouldConvertToIdl(SdkNodeMetadata metadata, NodeMetadata expectedIdl) {
    assertThat(metadata.toIdl(), equalTo(expectedIdl));
  }

  public static Stream<Arguments> metadataIdl() {
    return Stream.of(
        Arguments.of(
            SdkNodeMetadata.builder().name("some name").timeout(Duration.ofHours(1)).build(),
            NodeMetadata.builder().name("some name").timeout(Duration.ofHours(1)).build()),
        Arguments.of(
            SdkNodeMetadata.builder().name("some name").build(),
            NodeMetadata.builder().name("some name").build()),
        Arguments.of(
            SdkNodeMetadata.builder().timeout(Duration.ofMinutes(1)).build(),
            NodeMetadata.builder().timeout(Duration.ofMinutes(1)).build()),
        Arguments.of(SdkNodeMetadata.builder().build(), NodeMetadata.builder().build()));
  }
}
