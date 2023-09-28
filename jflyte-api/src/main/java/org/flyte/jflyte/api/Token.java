/*
 * Copyright 2020-2021 Flyte Authors
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
package org.flyte.jflyte.api;

import com.google.auto.value.AutoValue;
import java.time.Instant;
import javax.annotation.Nullable;

@AutoValue
public abstract class Token {
  public abstract String accessToken();

  public abstract String tokenType();

  @Nullable
  public abstract String refreshToken();

  public abstract Instant expiry();

  public abstract Builder toBuilder();

  public static Builder builder() {
    return new AutoValue_Token.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder accessToken(String accessToken);

    public abstract Builder tokenType(String tokenType);

    public abstract Builder refreshToken(String refreshToken);

    public abstract Builder expiry(Instant expiry);

    public abstract Token build();
  }
}
