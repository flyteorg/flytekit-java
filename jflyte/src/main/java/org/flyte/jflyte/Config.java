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

import com.google.auto.value.AutoValue;

/** Configuration file for jflyte. */
@AutoValue
public abstract class Config {

  public abstract String platformUrl();

  public abstract String image();

  public abstract String stagingLocation();

  public abstract String pluginDir();

  public abstract boolean platformInsecure();

  public static Config load() {
    return Config.builder()
        .platformUrl(getenv("FLYTE_PLATFORM_URL"))
        .pluginDir(getenv("FLYTE_INTERNAL_PLUGIN_DIR"))
        .image(getenv("FLYTE_INTERNAL_IMAGE"))
        .stagingLocation(getenv("FLYTE_STAGING_LOCATION"))
        .platformInsecure(Boolean.parseBoolean(getenv("FLYTE_PLATFORM_INSECURE")))
        .build();
  }

  private static String getenv(String name) {
    String value = System.getenv(name);

    if (value == null) {
      throw new IllegalArgumentException("Environment variable '" + name + "' isn't set");
    }

    return value;
  }

  public static Builder builder() {
    return new AutoValue_Config.Builder();
  }

  /** Builder for {@link Config}. */
  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder platformUrl(String platformUrl);

    public abstract Builder image(String image);

    public abstract Builder stagingLocation(String stagingLocation);

    public abstract Builder pluginDir(String pluginDir);

    public abstract Builder platformInsecure(boolean platformInsecure);

    public abstract Config build();
  }
}
