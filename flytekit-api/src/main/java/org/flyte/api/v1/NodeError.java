/*
 * Copyright 2020-2022 Flyte Authors.
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

// originally just Error, called NodeError because otherwise clashes with java.lang.Error

/** Represents an error thrown from a node. */
@AutoValue
public abstract class NodeError {

  /**
   * Returns the node id that threw the error.
   *
   * @return the node id.
   */
  public abstract String failedNodeId();

  /**
   * Returns the error message thrown.
   *
   * @return the error message.
   */
  public abstract String message();

  public static Builder builder() {
    return new AutoValue_NodeError.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder failedNodeId(String failedNodeId);

    public abstract Builder message(String message);

    public abstract NodeError build();
  }
}
