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
package org.flyte.flytekit;

import static java.util.Collections.singletonList;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CompilerException extends RuntimeException {
  private static final long serialVersionUID = 766444357230118198L;
  private final List<CompilerError> errors;

  // would prefer to use factory methods, but using constructs makes stack traces clean

  CompilerException(List<CompilerError> errors) {
    super(formatMessage(errors));

    this.errors = errors;
  }

  CompilerException(CompilerError error) {
    super(formatMessage(singletonList(error)));

    this.errors = singletonList(error);
  }

  private static String formatMessage(List<CompilerError> errors) {
    if (errors.isEmpty()) {
      throw new IllegalArgumentException("CompileError list cannot be empty");
    }

    return IntStream.range(0, errors.size())
        .mapToObj(
            i -> {
              CompilerError error = errors.get(i);
              return String.format(
                  "Error %s: Code: %s, Node Id: %s, Description: %s",
                  i, error.kind(), error.nodeId(), error.message());
            })
        .collect(Collectors.joining("\n", "Failed to build workflow with errors:\n", ""));
  }

  public List<CompilerError> getErrors() {
    return errors;
  }
}
