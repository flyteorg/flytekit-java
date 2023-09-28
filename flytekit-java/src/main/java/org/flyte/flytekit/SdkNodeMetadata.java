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
package org.flyte.flytekit;

import com.google.auto.value.AutoValue;
import java.time.Duration;
import java.util.Optional;
import javax.annotation.Nullable;
import org.flyte.api.v1.NodeMetadata;

/** Holds node metadata. */
@AutoValue
abstract class SdkNodeMetadata {

  @Nullable
  public abstract String name();

  @Nullable
  public abstract Duration timeout();

  public NodeMetadata toIdl() {
    NodeMetadata.Builder builder = NodeMetadata.builder();
    if (name() != null) {
      builder.name(name());
    }
    if (timeout() != null) {
      builder.timeout(timeout());
    }

    return builder.build();
  }

  public abstract Builder toBuilder();

  public static Builder builder() {
    return new AutoValue_SdkNodeMetadata.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder name(String name);

    public abstract Builder timeout(Duration timeout);

    abstract Optional<String> name();

    abstract Optional<Duration> timeout();

    abstract SdkNodeMetadata autoBuild(); // not public

    public final SdkNodeMetadata build() {
      if (name().map(String::isEmpty).orElse(false)) {
        throw new IllegalArgumentException("Node metadata name cannot empty string");
      }
      if (timeout().map(Duration::isNegative).orElse(false)) {
        throw new IllegalArgumentException(
            "Node metadata timeout cannot be negative: "
                + timeout().orElseThrow(AssertionError::new));
      }
      return autoBuild();
    }
  }
}
