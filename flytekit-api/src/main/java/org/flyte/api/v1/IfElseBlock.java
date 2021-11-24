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

/**
 * Defines a series of if/else blocks. The first branch whose condition evaluates to true is the one
 * to execute. If no conditions were satisfied, the else_node or the error will execute.
 */
@AutoValue
public abstract class IfElseBlock {

  /** @return first condition to evaluate. */
  public abstract IfBlock case_();

  /** @return additional branches to evaluate. */
  public abstract List<IfBlock> other();

  /** @return the node to execute in case none of the branches were taken. */
  @Nullable
  public abstract Node elseNode();

  /** @return an error to throw in case none of the branches were taken. */
  @Nullable
  public abstract NodeError error();

  public static Builder builder() {
    return new AutoValue_IfElseBlock.Builder();
  }

  public abstract Builder toBuilder();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder case_(IfBlock case_);

    public abstract Builder other(List<IfBlock> other);

    public abstract Builder elseNode(Node elseNode);

    public abstract Builder error(NodeError error);

    public abstract IfElseBlock build();
  }
}
