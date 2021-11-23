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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import org.flyte.jflyte.api.Token;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class GcpTokenSourceTest {
  private static final String TARGET_AUDIENCE = "http://test.org";

  @Mock private GoogleAuthHelper authHelper;
  private GcpTokenSource tokenSource;
  private final Duration expirationTime = Duration.ofHours(1);
  private final Clock clock = Clock.fixed(Instant.now(), ZoneOffset.UTC);
  private final Token token =
      Token.builder()
          .accessToken("token")
          .refreshToken(null)
          .tokenType("Bearer")
          .expiry(clock.instant().plus(expirationTime))
          .build();

  @BeforeEach
  void setUp() {
    tokenSource = new GcpTokenSource(authHelper, TARGET_AUDIENCE);
  }

  @Test
  void testGetToken() throws IOException {
    when(authHelper.getToken(TARGET_AUDIENCE)).thenReturn(token);
    Token result = tokenSource.token();
    verify(authHelper, times(1)).getToken(TARGET_AUDIENCE);
    assertEquals(result, token);
  }

  @Test
  void testGetTokenCachesToken() throws IOException {
    when(authHelper.getToken(TARGET_AUDIENCE)).thenReturn(token);
    // This should cache token
    Token res1 = tokenSource.token();
    assertSame(res1, token);
    // Subsequent calls should not make a call to generate new token
    Token res2 = tokenSource.token();
    verify(authHelper, times(1)).getToken(TARGET_AUDIENCE);
    assertSame(res2, res1);
  }
}
