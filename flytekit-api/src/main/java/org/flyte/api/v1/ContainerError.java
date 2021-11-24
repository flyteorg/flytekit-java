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

/** Exception to propagate detailed errors from container to the execution. */
public class ContainerError extends RuntimeException {

  private static final long serialVersionUID = 5162780469952221158L;
  private final String code;
  private final Kind kind;

  /** Defines a generic error type that dictates the behavior of the retry strategy. */
  public enum Kind {
    RECOVERABLE,
    NON_RECOVERABLE
  }

  private ContainerError(String code, String message, Kind kind) {
    super(message);

    this.code = code;
    this.kind = kind;
  }

  /**
   * Creates {@link ContainerError}.
   *
   * @param code a simplified code for errors, so that we can provide a glossary of all possible
   *     errors.
   * @param message a detailed error message
   * @param kind an abstract error kind for this error.
   * @return container error
   */
  public static ContainerError create(String code, String message, Kind kind) {
    return new ContainerError(code, message, kind);
  }

  /**
   * Returns a simplified code for errors, so that we can provide a glossary of all possible errors.
   *
   * @return error code
   */
  public String getCode() {
    return code;
  }

  /**
   * Returns an abstract error kind for this error.
   *
   * @return error kind
   */
  public Kind getKind() {
    return kind;
  }
}
