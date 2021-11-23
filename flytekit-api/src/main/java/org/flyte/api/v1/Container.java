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
import java.util.List;
import javax.annotation.Nullable;

/** Defines properties for a container. */
@AutoValue
public abstract class Container {

  public abstract List<String> command();

  public abstract List<String> args();

  public abstract String image();

  public abstract List<KeyValuePair> env();

  @Nullable
  public abstract Resources resources();

  public static Builder builder() {
    return new AutoValue_Container.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder command(List<String> command);

    public abstract Builder args(List<String> args);

    public abstract Builder image(String image);

    public abstract Builder env(List<KeyValuePair> env);

    public abstract Builder resources(Resources resources);

    public abstract Container build();
  }
}
