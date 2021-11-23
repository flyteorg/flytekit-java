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
package org.flyte.jflyte.gcp;

import static org.flyte.jflyte.gcp.GcpTokenSourceFactoryRegistrar.FLYTE_PLATFORM_URL;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.flyte.jflyte.api.TokenSource;
import org.flyte.jflyte.api.TokenSourceFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class GcpTokenSourceFactoryRegistrarTest {

  private final StubFactoryCreator stub = new StubFactoryCreator();
  private GcpTokenSourceFactoryRegistrar registrar;

  @BeforeEach
  void setUp() {
    registrar = new GcpTokenSourceFactoryRegistrar(stub);
  }

  @Test
  void testPlatformsUlrIsRequired() {
    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> registrar.load(ImmutableMap.of()));

    assertThat(ex.getMessage(), is("FLYTE_PLATFORM_URL env var must be a non empty string: null"));
  }

  @Test
  void testLoadExactlyOneFactory() {
    Iterable<TokenSourceFactory> factoryIterable =
        registrar.load(ImmutableMap.of("FLYTE_PLATFORM_URL", "test"));

    List<TokenSourceFactory> factories = new ArrayList<>();
    factoryIterable.forEach(factories::add);
    assertThat(factories, hasSize(1));
  }

  @Test
  void testPropagatesConfInEnv() {
    registrar.load(ImmutableMap.of(FLYTE_PLATFORM_URL, "http://flyte.example.com"));

    assertThat(stub.audience, is("http://flyte.example.com"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"", " "})
  void testThrowsExceptionForInvalidPlatformUrl(String platformUrl) {
    Map<String, String> env = ImmutableMap.of(FLYTE_PLATFORM_URL, platformUrl);
    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> registrar.load(env));
    assertThat(
        ex.getMessage(),
        is("FLYTE_PLATFORM_URL env var must be a non empty string: " + platformUrl));
  }

  private static class StubFactoryCreator implements Function<String, TokenSourceFactory> {
    private String audience;

    @Override
    public TokenSourceFactory apply(String audience) {
      this.audience = audience;
      return new TestTokenSource();
    }
  }

  private static class TestTokenSource implements TokenSourceFactory {

    @Override
    public String getMethod() {
      return null;
    }

    @Override
    public TokenSource getTokenSource() {
      return null;
    }
  }
}
