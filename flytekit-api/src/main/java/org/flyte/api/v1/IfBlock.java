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
 * Defines a condition and the execution unit that should be executed if the condition is satisfied.
 */
@AutoValue
public abstract class IfBlock {

  public abstract BooleanExpression condition();

  public abstract Node thenNode();

  public static Builder builder() {
    return new AutoValue_IfBlock.Builder();
  }

  public abstract Builder toBuilder();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder condition(BooleanExpression condition);

    public abstract Builder thenNode(Node thenNode);

    public abstract IfBlock build();
  }
}
