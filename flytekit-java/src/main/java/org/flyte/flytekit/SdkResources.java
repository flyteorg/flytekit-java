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
package org.flyte.flytekit;

import static java.util.Collections.unmodifiableMap;

import com.google.auto.value.AutoValue;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.flyte.api.v1.Resources;

@AutoValue
public abstract class SdkResources {
  // Known resource names.
  public enum ResourceName {
    UNKNOWN,
    CPU,
    GPU,
    MEMORY,
    STORAGE,
    // For Kubernetes-based deployments, pods use ephemeral local storage for scratch space,
    // caching, and for logs.
    EPHEMERAL_STORAGE;

    public Resources.ResourceName toResourceName() {
      switch (this) {
        case UNKNOWN:
          return Resources.ResourceName.UNKNOWN;
        case CPU:
          return Resources.ResourceName.CPU;
        case GPU:
          return Resources.ResourceName.GPU;
        case MEMORY:
          return Resources.ResourceName.MEMORY;
        case STORAGE:
          return Resources.ResourceName.STORAGE;
        case EPHEMERAL_STORAGE:
          return Resources.ResourceName.EPHEMERAL_STORAGE;
      }
      throw new AssertionError("Unexpected SdkResources.ResourceName: " + this);
    }
  }

  private static final SdkResources EMPTY = builder().build();

  // Values must be a valid k8s quantity. See
  // https://github.com/kubernetes/apimachinery/blob/master/pkg/api/resource/quantity.go#L30-L80
  @Nullable
  public abstract Map<SdkResources.ResourceName, String> requests();

  @Nullable
  public abstract Map<SdkResources.ResourceName, String> limits();

  public static SdkResources.Builder builder() {
    return new AutoValue_SdkResources.Builder();
  }

  public static SdkResources empty() {
    return EMPTY;
  }

  public Resources toResources() {
    Resources.Builder builder = Resources.builder();

    if (limits() != null) {
      builder.limits(toResourceMap(limits()));
    }
    if (requests() != null) {
      builder.requests(toResourceMap(requests()));
    }

    return builder.build();
  }

  private Map<Resources.ResourceName, String> toResourceMap(
      Map<SdkResources.ResourceName, String> sdkResources) {
    Map<Resources.ResourceName, String> result = new HashMap<>();
    sdkResources.forEach(
        (sdkResourceName, value) -> result.put(sdkResourceName.toResourceName(), value));
    return result;
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract SdkResources.Builder requests(Map<SdkResources.ResourceName, String> requests);

    public abstract Map<SdkResources.ResourceName, String> requests();

    public abstract SdkResources.Builder limits(Map<SdkResources.ResourceName, String> limits);

    public abstract Map<SdkResources.ResourceName, String> limits();

    abstract SdkResources autoBuild();

    public SdkResources build() {
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
