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
package org.flyte.jflyte.gcp;

import com.google.api.client.googleapis.util.Utils;
import com.google.auth.oauth2.GoogleCredentials;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Clock;
import java.util.Objects;
import org.flyte.jflyte.api.TokenSource;
import org.flyte.jflyte.api.TokenSourceFactory;

public class GcpTokenSourceFactory implements TokenSourceFactory {

  private final String targetAudience;

  public GcpTokenSourceFactory(String targetAudience) {
    this.targetAudience = Objects.requireNonNull(targetAudience, "targetDuration");
  }

  @Override
  public String getMethod() {
    return "id_token";
  }

  @Override
  public TokenSource getTokenSource() {
    Clock clock = Clock.systemUTC();
    return new GcpTokenSource(
        new GoogleAuthHelper(Utils.getDefaultTransport(), getCredentials(), clock), targetAudience);
  }

  private static GoogleCredentials getCredentials() {
    try {
      return GoogleCredentials.getApplicationDefault();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
