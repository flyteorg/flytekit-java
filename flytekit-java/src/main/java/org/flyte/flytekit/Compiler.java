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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.Variable;

/**
 * Implements subset of flytepropeller compiler validations to surface as exceptions while building
 * workflows.
 */
class Compiler {

  static List<CompilerError> validateApply(
      String nodeId, Map<String, SdkBindingData<?>> inputs, Map<String, Variable> variableMap) {
    List<CompilerError> errors = new ArrayList<>();

    Set<String> allKeys = new HashSet<>();
    allKeys.addAll(inputs.keySet());
    allKeys.addAll(variableMap.keySet());

    for (String key : allKeys) {
      SdkBindingData<?> input = inputs.get(key);
      Variable variable = variableMap.get(key);

      if (variable == null) {
        String message = String.format("Variable [%s] not found on node [%s].", key, nodeId);
        errors.add(
            CompilerError.create(CompilerError.Kind.VARIABLE_NAME_NOT_FOUND, nodeId, message));

        continue;
      }

      if (input == null) {
        String message = String.format("Parameter not bound [%s].", key);
        errors.add(CompilerError.create(CompilerError.Kind.PARAMETER_NOT_BOUND, nodeId, message));

        continue;
      }

      LiteralType actualType = input.type().getLiteralType();
      LiteralType expectedType = variable.literalType();

      if (!actualType.equals(expectedType)) {
        String message =
            String.format(
                "Variable [%s] (type [%s]) doesn't match expected type [%s].",
                key, actualType, expectedType);
        errors.add(CompilerError.create(CompilerError.Kind.MISMATCHING_TYPES, nodeId, message));
      }
    }

    return errors;
  }
}
