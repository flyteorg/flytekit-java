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

import static java.util.Collections.unmodifiableMap;

import com.google.auto.value.AutoValue;
import java.util.EnumMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A customizable interface to convey resources requested for a container. This can be interpreted
 * differently for different container engines.
 */
@AutoValue
public abstract class Resources {
  // Known resource names.
  public enum ResourceName {
    UNKNOWN,
    CPU,
    GPU,
    MEMORY,
    STORAGE,
    // For Kubernetes-based deployments, pods use ephemeral local storage for scratch space,
    // caching, and for logs.
    EPHEMERAL_STORAGE
  }

  // Values must be a valid k8s quantity. See
  // https://github.com/kubernetes/apimachinery/blob/master/pkg/api/resource/quantity.go#L30-L80
  @Nullable
  public abstract Map<ResourceName, String> requests();

  @Nullable
  public abstract Map<ResourceName, String> limits();

  public static Builder builder() {
    return new AutoValue_Resources.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder requests(Map<ResourceName, String> requests);

    public abstract Map<ResourceName, String> requests();

    public abstract Builder limits(Map<ResourceName, String> limits);

    public abstract Map<ResourceName, String> limits();

    abstract Resources autoBuild();

    public Resources build() {
      if (requests() != null && !requests().isEmpty()) {
        requests(unmodifiableMap(new EnumMap<>(requests())));
      }
      if (limits() != null && !limits().isEmpty()) {
        limits(unmodifiableMap(new EnumMap<>(limits())));
      }
      return autoBuild();
    }
  }
}
