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
 * A reference to an output produced by a node. The type can be retrieved -and validated- from the
 * underlying interface of the node.
 */
@AutoValue
public abstract class OutputReference {
  /**
   * Returns node id referenced. Node id must exist at the graph layer.
   *
   * @return node id referenced.
   */
  public abstract String nodeId();

  /**
   * Returns variable reference. Variable name must refer to an output variable for the node.
   *
   * @return variable reference.
   */
  public abstract String var();

  public static Builder builder() {
    return new AutoValue_OutputReference.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder nodeId(String nodeId);

    public abstract Builder var(String var);

    public abstract OutputReference build();
  }
}
