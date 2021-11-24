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

import com.google.api.client.auth.oauth2.ClientParametersAuthentication;
import com.google.api.client.auth.oauth2.RefreshTokenRequest;
import com.google.api.client.auth.oauth2.TokenRequest;
import com.google.api.client.auth.openidconnect.IdTokenResponse;
import com.google.api.client.googleapis.util.Utils;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.IdToken;
import com.google.auth.oauth2.IdTokenCredentials;
import com.google.auth.oauth2.IdTokenProvider;
import com.google.auth.oauth2.UserCredentials;
import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.util.Objects;
import java.util.logging.Logger;
import org.flyte.jflyte.api.Token;

/** Helper class to generate {@link Token} in Google Cloud. */
class GoogleAuthHelper {

  private static final Logger LOG = Logger.getLogger(GoogleAuthHelper.class.getName());
  private static final JsonFactory JSON_FACTORY = Utils.getDefaultJsonFactory();

  private final HttpTransport httpTransport;
  private final GoogleCredentials credentials;
  private final Clock clock;

  GoogleAuthHelper(HttpTransport httpTransport, GoogleCredentials credentials, Clock clock) {
    this.httpTransport = Objects.requireNonNull(httpTransport, "httpTransport");
    this.credentials = Objects.requireNonNull(credentials, "credentials");
    this.clock = Objects.requireNonNull(clock, "clock");
  }

  public Token getToken(String targetAudience) throws IOException {
    if (credentials instanceof IdTokenProvider) {
      return getIdTokenFromIdTokenProvider(targetAudience);
    } else if (credentials instanceof UserCredentials) {
      return getUserToken((UserCredentials) credentials);
    } else {
      throw new UnsupportedOperationException(
          "Don't support credentials: " + credentials.getClass());
    }
  }

  private Token getIdTokenFromIdTokenProvider(String targetAudience) throws IOException {
    LOG.info("Fetching token from provider");
    IdTokenCredentials idTokenCredentials =
        IdTokenCredentials.newBuilder()
            .setIdTokenProvider((IdTokenProvider) credentials)
            .setTargetAudience(targetAudience)
            .build();
    idTokenCredentials.refresh();
    IdToken idToken = idTokenCredentials.getIdToken();
    if (idToken == null || idToken.getTokenValue() == null) {
      throw new IOException("Couldn't get id token for credential");
    }

    Instant expiryAt = Instant.ofEpochMilli(idToken.getExpirationTime().getTime());
    return Token.builder()
        .accessToken(idToken.getTokenValue())
        .expiry(expiryAt)
        .tokenType("Bearer")
        .build();
  }

  private Token getUserToken(UserCredentials credentials) throws IOException {
    LOG.info("Fetching user id token for user credential: " + credentials.getClientId());
    TokenRequest request = getRefreshTokenRequest(credentials);
    Instant base = clock.instant();
    IdTokenResponse response = request.executeUnparsed().parseAs(IdTokenResponse.class);
    if (response == null || response.getIdToken() == null) {
      String errMessage =
          String.format(
              "Couldn't get id token for user credential: [%s].%n"
                  + "UserCredentials can obtain an id token only when authenticated through "
                  + "gcloud running 'gcloud auth login --update-adc' or "
                  + "'gcloud auth application-default login'",
              credentials.getClientId());
      throw new IOException(errMessage);
    }

    Instant expiryAt = base.plusSeconds(response.getExpiresInSeconds());
    return Token.builder()
        .accessToken(response.getIdToken())
        .expiry(expiryAt)
        .tokenType(response.getTokenType())
        .build();
  }

  private RefreshTokenRequest getRefreshTokenRequest(UserCredentials credentials) {
    return new RefreshTokenRequest(
            this.httpTransport,
            JSON_FACTORY,
            new GenericUrl(credentials.toBuilder().getTokenServerUri()),
            credentials.getRefreshToken())
        .setClientAuthentication(
            new ClientParametersAuthentication(
                credentials.getClientId(), credentials.getClientSecret()))
        .setRequestInitializer(new HttpCredentialsAdapter(credentials));
  }
}
