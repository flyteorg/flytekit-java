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
import org.flyte.api.v1.BlobType.Builder;
import org.flyte.api.v1.ContainerError.Kind;

/** Failure Handling Strategy. */
@AutoValue
public abstract class OnFailurePolicy {
  public enum Kind {
    FAIL_IMMEDIATELY,
    FAIL_AFTER_EXECUTABLE_NODES_COMPLETE
  }

  public abstract Kind getKind();

  public static OnFailurePolicy.Builder builder() {
    return new AutoValue_OnFailurePolicy.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder kind(Kind kind);

    public abstract OnFailurePolicy build();
  }
}
