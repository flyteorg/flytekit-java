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

/**
 * A parameter is used as input to a launch plan and has the special ability to have a default value
 * or mark itself as required.
 */
@AutoValue
public abstract class Parameter {

  // * [Required] Variable. Defines the type of the variable backing this parameter. */
  public abstract Variable var();

  // It's not required if it's null
  @Nullable
  public abstract Literal defaultValue();

  public static Parameter create(Variable var, Literal defaultValue) {
    return new AutoValue_Parameter(var, defaultValue);
  }
}
