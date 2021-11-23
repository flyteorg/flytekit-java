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

/**
 * Refers to an offloaded set of files. It encapsulates the type of the store and a unique uri for
 * where the data is. There are no restrictions on how the uri is formatted since it will depend on
 * how to interact with the store.
 */
@AutoValue
public abstract class Blob {

  public abstract BlobMetadata metadata();

  public abstract String uri();

  public static Builder builder() {
    return new AutoValue_Blob.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder metadata(BlobMetadata metadata);

    public abstract Builder uri(String uri);

    public abstract Blob build();
  }
}
