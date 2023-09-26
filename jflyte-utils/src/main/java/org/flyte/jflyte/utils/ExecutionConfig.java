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
package org.flyte.jflyte.utils;

import static java.lang.System.getenv;

import com.google.auto.value.AutoValue;

/** Configuration values available only during task execution. */
@AutoValue
public abstract class ExecutionConfig {
  abstract String image();

  public abstract String project();

  public abstract String domain();

  public abstract String version();

  public static ExecutionConfig load() {
    return ExecutionConfig.builder()
        .project(getenv("FLYTE_INTERNAL_PROJECT"))
        .domain(getenv("FLYTE_INTERNAL_DOMAIN"))
        .version(getenv("FLYTE_INTERNAL_VERSION"))
        .image(getenv("FLYTE_INTERNAL_IMAGE"))
        .build();
  }

  /** Builder for {@link ExecutionConfig}. */
  public static Builder builder() {
    return new AutoValue_ExecutionConfig.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder project(String project);

    public abstract Builder domain(String domain);

    public abstract Builder version(String version);

    public abstract Builder image(String image);

    public abstract ExecutionConfig build();
  }
}
