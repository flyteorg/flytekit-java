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
package org.flyte.flytekit;

import static java.util.Collections.unmodifiableMap;

import com.google.auto.value.AutoValue;
import java.util.EnumMap;
import java.util.Map;
import javax.annotation.Nullable;

@AutoValue
public abstract class SdkResources {
  // Known resource names.
  public enum ResourceName {
    UNKNOWN,
    CPU,
    GPU,
    MEMORY,
    STORAGE
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
