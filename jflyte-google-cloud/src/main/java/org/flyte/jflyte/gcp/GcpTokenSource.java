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
package org.flyte.jflyte.gcp;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;
import org.flyte.jflyte.api.Token;
import org.flyte.jflyte.api.TokenSource;

/** Implementation of {@link TokenSource} for Google Cloud. */
class GcpTokenSource implements TokenSource {

  private final GoogleAuthHelper authHelper;
  private final String targetAudience;

  private Token token;

  GcpTokenSource(GoogleAuthHelper auth, String targetAudience) {
    this.authHelper = Objects.requireNonNull(auth, "auth");
    this.targetAudience = Objects.requireNonNull(targetAudience, "targetAudience");
  }

  @Override
  public synchronized Token token() {
    try {
      if (token == null) {
        token = authHelper.getToken(targetAudience);
      }
      return token;
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to generate token", e);
    }
  }
}
