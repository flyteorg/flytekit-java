/*
 * Copyright 2020-2021 Flyte Authors.
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
 * A simple byte array with a tag to help different parts of the system communicate about what is in
 * the byte array. It's strongly advisable that consumers of this type define a unique tag and
 * validate the tag before parsing the data.
 */
@AutoValue
public abstract class Binary {
  public static final String TAG_FIELD = "tag";
  public static final String VALUE_FIELD = "value";

  public abstract byte[] value();

  public abstract String tag();

  public static Builder builder() {
    return new AutoValue_Binary.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder value(byte[] value);

    public abstract Builder tag(String tag);

    public abstract Binary build();
  }
}
