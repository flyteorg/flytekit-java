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

import static java.util.stream.Collectors.toMap;

import com.google.auto.value.AutoValue;
import java.util.Map;
import javax.annotation.Nullable;
import org.flyte.api.v1.Resources;

/**
 * Specify resources for requires and limits, like cpu and memory. Used by {@link SdkContainerTask}
 * and {@link SdkRunnableTask}.
 */
@AutoValue
public abstract class SdkResources {
  /** Known resource names. */
  public enum ResourceName {
    UNKNOWN,
    CPU,
    GPU,
    MEMORY,
    STORAGE,
    // For Kubernetes-based deployments, pods use ephemeral local storage for scratch space,
    // caching, and for logs.
    EPHEMERAL_STORAGE;

    Resources.ResourceName toIdl() {
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

  /** Returns the requests resources */
  @Nullable
  public abstract Map<SdkResources.ResourceName, String> requests();

  /** Returns the limits resources */
  @Nullable
  public abstract Map<SdkResources.ResourceName, String> limits();

  /** Returns returns a new {@link SdkResources.Builder} to create {@link SdkResources} */
  public static SdkResources.Builder builder() {
    return new AutoValue_SdkResources.Builder();
  }

  /** Returns returns an empty {@link SdkResources}, no limits or requests for anything */
  public static SdkResources empty() {
    return EMPTY;
  }

  Resources toIdl() {
    Resources.Builder builder = Resources.builder();

    if (limits() != null) {
      builder.limits(toIdl(limits()));
    }
    if (requests() != null) {
      builder.requests(toIdl(requests()));
    }

    return builder.build();
  }

  private Map<Resources.ResourceName, String> toIdl(
      Map<SdkResources.ResourceName, String> sdkResources) {
    return sdkResources.entrySet().stream()
        .map(entry -> Map.entry(entry.getKey().toIdl(), entry.getValue()))
        .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  /** Builder for {@link org.flyte.flytekit.SdkResources}. */
  @AutoValue.Builder
  public abstract static class Builder {

    /**
     * Sets the requests resources via a map of quantities per {@link ResourceName}. Quantities must
     * be a valid k8s quantity. See <a
     * href="https://github.com/kubernetes/apimachinery/blob/master/pkg/api/resource/quantity.go#L30-L80">quantity.go</a>
     *
     * @param requests requested resources
     */
    public abstract SdkResources.Builder requests(Map<SdkResources.ResourceName, String> requests);

    abstract Map<SdkResources.ResourceName, String> requests();

    /**
     * Sets the limits resources via a map of quantities per {@link ResourceName}. Quantities must
     * be a valid k8s quantity. See <a
     * href="https://github.com/kubernetes/apimachinery/blob/master/pkg/api/resource/quantity.go#L30-L80">quantity.go</a>
     *
     * @param limits limits resources
     */
    public abstract SdkResources.Builder limits(Map<SdkResources.ResourceName, String> limits);

    abstract Map<SdkResources.ResourceName, String> limits();

    abstract SdkResources autoBuild();

    /** Returns Builds a {@link SdkResources} */
    public SdkResources build() {
      // TODO move the check about valid quantities tot he builder
      if (requests() != null) {
        requests(Map.copyOf(requests()));
      }
      if (limits() != null) {
        limits(Map.copyOf(limits()));
      }
      return autoBuild();
    }
  }
}
