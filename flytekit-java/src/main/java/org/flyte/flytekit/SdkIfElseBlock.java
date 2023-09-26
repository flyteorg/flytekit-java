/*
 * Copyright 2020-2023 Flyte Authors.
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

import com.google.auto.value.AutoValue;
import java.util.List;
import javax.annotation.Nullable;

/** Holds case clauses and the node if none of the cases matched. */
@AutoValue
abstract class SdkIfElseBlock {

  abstract SdkIfBlock case_();

  abstract List<SdkIfBlock> other();

  @Nullable
  abstract SdkNode<?> elseNode();

  static Builder builder() {
    return new AutoValue_SdkIfElseBlock.Builder();
  }

  @AutoValue.Builder
  abstract static class Builder {
    public abstract Builder case_(SdkIfBlock case_);

    public abstract Builder other(List<SdkIfBlock> other);

    public abstract Builder elseNode(SdkNode<?> elseNode);

    public abstract SdkIfElseBlock build();
  }
}
