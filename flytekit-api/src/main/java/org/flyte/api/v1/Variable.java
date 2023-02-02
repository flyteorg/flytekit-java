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
import javax.annotation.Nullable;

/** Defines a strongly typed variable. */
@AutoValue
public abstract class Variable {

  /** Variable literal type. */
  public abstract LiteralType literalType();

  /** [Optional] string describing input variable. */
  @Nullable
  public abstract String description();

  public static Builder builder() {
    return new AutoValue_Variable.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder literalType(LiteralType literalType);

    public abstract Builder description(String description);

    public abstract Variable build();
  }
}
