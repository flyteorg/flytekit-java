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

/** An input/output binding of a variable to either static value or a node output. */
@AutoValue
public abstract class Binding {
  /** Variable name must match an input/output variable of the node. */
  public abstract String var_();

  /** Data to use to bind this variable. */
  public abstract BindingData binding();

  public static Builder builder() {
    return new AutoValue_Binding.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder var_(String var_);

    public abstract Builder binding(BindingData binding);

    public abstract Binding build();
  }
}
