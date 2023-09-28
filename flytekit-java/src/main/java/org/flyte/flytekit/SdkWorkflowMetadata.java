/*
 * Copyright 2020-2023 Flyte Authors
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
import org.flyte.api.v1.OnFailurePolicy;

/** Metadata for the entire workflow. */
@AutoValue
public abstract class SdkWorkflowMetadata {

  public abstract OnFailurePolicy onFailure();

  public static Builder builder() {
    return new AutoValue_SdkWorkflowMetadata.Builder()
        .onFailure(OnFailurePolicy.builder().kind(OnFailurePolicy.Kind.FAIL_IMMEDIATELY).build());
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder onFailure(OnFailurePolicy onFailure);

    public abstract SdkWorkflowMetadata build();
  }
}
