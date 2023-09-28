/*
 * Copyright 2020-2023 Flyte Authors
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
package org.flyte.api.v1;

import com.google.auto.value.AutoValue;
import java.util.List;
import javax.annotation.Nullable;

/** Defines properties for a container. */
@AutoValue
public abstract class Container {

  /**
   * Command to be executed. If not provided, the default entrypoint in the container image will be
   * used.
   */
  public abstract List<String> command();

  /**
   * These will default to Flyte given paths. If provided, the system will not append known paths.
   * If the task still needs flyte's inputs and outputs path, add $(FLYTE_INPUT_FILE),
   * $(FLYTE_OUTPUT_FILE) wherever makes sense and the system will populate these before executing
   * the container.
   */
  public abstract List<String> args();

  /** Container image url. Eg: docker/redis:latest */
  public abstract String image();

  /** Environment variables will be set as the container is starting up. */
  public abstract List<KeyValuePair> env();

  /** Container resources requirement as specified by the container engine. */
  @Nullable
  public abstract Resources resources();

  // TODO: add ports and architecture from src/main/proto/flyteidl/core/tasks.proto

  public static Builder builder() {
    return new AutoValue_Container.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder command(List<String> command);

    public abstract Builder args(List<String> args);

    public abstract Builder image(String image);

    public abstract Builder env(List<KeyValuePair> env);

    public abstract Builder resources(Resources resources);

    public abstract Container build();
  }
}
