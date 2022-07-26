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

  public abstract TypedInterface interface_();

  public abstract String type();

  public abstract RetryStrategy retries();

  public abstract Struct custom();

  public abstract boolean discoverable();

  public abstract String discoveryVersion();

  public abstract boolean cacheSerializable();

  public abstract Builder toBuilder();

  public static Builder builder() {
    return new AutoValue_TaskTemplate.Builder()
        .discoverable(false)
        .cacheSerializable(false)
        .discoveryVersion("");
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
