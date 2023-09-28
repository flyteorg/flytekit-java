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
import javax.annotation.Nullable;

/**
 * A Task structure that uniquely identifies a task in the system. Tasks are registered as a first
 * step in the system.
 */
@AutoValue
public abstract class TaskTemplate {

  @Nullable
  public abstract Container container();

  /**
   * A strongly typed interface for the task. This enables others to use this task within a workflow
   * and guarantees compile-time validation of the workflow to avoid costly runtime failures.
   */
  public abstract TypedInterface interface_();

  /**
   * A predefined yet extensible Task type identifier. This can be used to customize any of the
   * components. If no extensions are provided in the system, Flyte will resolve this task to its
   * TaskCategory and default the implementation registered for the TaskCategory.
   */
  public abstract String type();

  /** Number of retries per task. */
  public abstract RetryStrategy retries();

  /** Custom data about the task. This is extensible to allow various plugins in the system. */
  public abstract Struct custom();

  /**
   * Indicates whether the system should attempt to lookup this task's output to reuse existing
   * data.
   */
  public abstract boolean discoverable();

  /** Indicates a logical version to apply to this task for the purpose of discovery. */
  @Nullable
  public abstract String discoveryVersion();

  /**
   * Indicates whether the system should attempt to execute discoverable instances in serial to
   * avoid duplicate work.
   */
  public abstract boolean cacheSerializable();

  public abstract Builder toBuilder();

  public static Builder builder() {
    return new AutoValue_TaskTemplate.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder container(Container container);

    public abstract Builder interface_(TypedInterface interface_);

    public abstract Builder retries(RetryStrategy retries);

    public abstract Builder type(String type);

    public abstract Builder custom(Struct custom);

    public abstract Builder discoverable(boolean discoverable);

    public abstract Builder discoveryVersion(String discoveryVersion);

    public abstract Builder cacheSerializable(boolean cacheSerializable);

    public abstract TaskTemplate build();
  }
}
