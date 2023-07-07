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

import org.flyte.api.v1.ContainerError.Kind;

/** Failure Handling Strategy. */
public class OnFailurePolicy {

  private Kind kind;

  public enum Kind {
    FAIL_IMMEDIATELY,
    FAIL_AFTER_EXECUTABLE_NODES_COMPLETE
  }

  private OnFailurePolicy(OnFailurePolicy.Kind kind) {
    this.kind = kind;
  }

  public static OnFailurePolicy create(OnFailurePolicy.Kind kind) {
    return new OnFailurePolicy(kind);
  }

  public Kind getKind() {
    return this.kind;
  }
}
