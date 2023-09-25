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
package org.flyte.jflyte.utils;

import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;

/** Configuration file for jflyte. */
@AutoValue
public abstract class Config {

  /**
   * Get flyte platform URL.
   *
   * @return flyte platform URL.
   */
  public abstract String platformUrl();

  /**
   * Get flyte docker image.
   *
   * @return flyte docker image.
   */
  public abstract String image();

  /**
   * Get flyte staging location path.
   *
   * @return flyte staging location path.
   */
  @Nullable
  public abstract String stagingLocation();

  /**
   * Get flyte module dir path.
   *
   * @return flyte module dir path.
   */
  public abstract String moduleDir();

  /**
   * Get if the auth with the platform is secure or not.
   *
   * @return boolean indicating if platform is insecure or not.
   */
  public abstract boolean platformInsecure();

  public static Config load() {
    return Config.builder()
        .platformUrl(getenv("FLYTE_PLATFORM_URL"))
        .moduleDir(getenv("FLYTE_INTERNAL_MODULE_DIR"))
        .image(getenv("FLYTE_INTERNAL_IMAGE"))
        .stagingLocation(getenvOrNull("FLYTE_STAGING_LOCATION"))
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

  private static String getenvOrNull(String name) {
    return System.getenv(name);
  }

  static Builder builder() {
    return new AutoValue_Config.Builder();
  }

  /** Builder for {@link Config}. */
  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder platformUrl(String platformUrl);

    abstract Builder image(String image);

    abstract Builder stagingLocation(String stagingLocation);

    abstract Builder moduleDir(String moduleDir);

    abstract Builder platformInsecure(boolean platformInsecure);

    abstract Config build();
  }
}
