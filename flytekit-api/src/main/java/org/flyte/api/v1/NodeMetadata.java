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
package org.flyte.api.v1;

import com.google.auto.value.AutoValue;
import java.time.Duration;
import javax.annotation.Nullable;

/** Defines extra information about the Node. */
@AutoValue
public abstract class NodeMetadata {
  // A friendly name for the Node.
  @Nullable
  public abstract String name();

  // The overall timeout of a task.
  @Nullable
  public abstract Duration timeout();

  // Number of retries per task.
  @Nullable
  public abstract RetryStrategy retries();

  public static Builder builder() {
    return new AutoValue_NodeMetadata.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder name(String name);

    public abstract Builder timeout(Duration timeout);

    public abstract Builder retries(RetryStrategy retries);

    public abstract NodeMetadata build();
  }
}
