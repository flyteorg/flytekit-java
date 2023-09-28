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
import java.io.Serializable;

/**
 * CompilerError represents all possible errors that can be reported while building workflow.
 *
 * <p>See <a
 * href="https://github.com/lyft/flytepropeller/blob/master/pkg/compiler/errors/compiler_errors.go">...</a>.
 */
@AutoValue
public abstract class CompilerError implements Serializable {

  private static final long serialVersionUID = 8867541889252290566L;

  public enum Kind {
    /** Two types expected to be compatible but aren't. */
    MISMATCHING_TYPES,

    /** A referenced variable (in a parameter or a condition) wasn't found. */
    VARIABLE_NAME_NOT_FOUND,

    /** One of the required input parameters or a Workflow output parameter wasn't bound. */
    PARAMETER_NOT_BOUND,

    /** An Id existed twice. */
    DUPLICATE_NODE_ID

    // TODO: complete kinds from upstream
  }

  /**
   * Error kind.
   *
   * @return error kind
   */
  public abstract Kind kind();

  /**
   * Node id associated with compiler error.
   *
   * @return node id
   */
  public abstract String nodeId();

  /**
   * Message explaining the cause.
   *
   * @return message
   */
  public abstract String message();

  /**
   * Creates a compiler error.
   *
   * @param kind specifies the {@link CompilerError.Kind} of error.
   * @param nodeId node id to which this error applies.
   * @param message error message.
   * @return the created compile error.
   */
  public static CompilerError create(Kind kind, String nodeId, String message) {
    return new AutoValue_CompilerError(kind, nodeId, message);
  }
}
