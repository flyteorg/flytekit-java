/*
 * Copyright 2020-2022 Flyte Authors.
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

/** Defines type behavior for blob objects. */
@AutoValue
public abstract class BlobType {

  public static final BlobType DEFAULT =
      BlobType.builder().dimensionality(BlobDimensionality.SINGLE).format("").build();

  public enum BlobDimensionality {
    SINGLE,
    MULTIPART
  }

  /**
   * Returns the format of the Blob. Format can be a free form string understood by SDK/UI etc. like
   * csv, or parquet.
   *
   * @return the format of the Blob.
   */
  public abstract String format();

  public abstract BlobDimensionality dimensionality();

  public static Builder builder() {
    return new AutoValue_BlobType.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder format(String format);

    public abstract Builder dimensionality(BlobDimensionality dimensionality);

    public abstract BlobType build();
  }
}
