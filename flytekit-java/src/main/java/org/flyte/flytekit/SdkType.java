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

import static java.util.stream.Collectors.toUnmodifiableMap;

import java.util.Map;
import java.util.Set;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.Variable;

/**
 * Bridge between the properties of a Java type and a set of variable set in Flyte. It is a
 * requirement that the properties of the value are of type {@link SdkBindingData}.
 *
 * @param <T> the Java native type to bridge.
 */
public abstract class SdkType<T> {

  /**
   * Coverts the value's properties into a {@link Literal} map by variable name.
   *
   * @param value value to convert.
   * @return the literal map.
   */
  public abstract Map<String, Literal> toLiteralMap(T value);

  /**
   * Coverts a {@link Literal} map by variable name into a value.
   *
   * @param value a {@link Literal} map by variable name.
   * @return the converted value.
   */
  public abstract T fromLiteralMap(Map<String, Literal> value);

  /**
   * Returns a value composed of {@link SdkBindingData#promise(SdkLiteralType, String, String)} for
   * the supplied node is.
   *
   * @param nodeId the node id that the value is a promise for.
   * @return the value.
   */
  public abstract T promiseFor(String nodeId);

  public final Map<String, SdkBindingData<?>> promiseMapFor(String nodeId) {
    return toLiteralTypes().entrySet().stream()
        .collect(
            toUnmodifiableMap(
                Map.Entry::getKey, e -> SdkBindingData.promise(e.getValue(), nodeId, e.getKey())));
  }

  /**
   * Returns a variable map for the properties for {@link T}.
   *
   * @return the variable map
   */
  public abstract Map<String, Variable> getVariableMap();

  public abstract Map<String, SdkLiteralType<?>> toLiteralTypes();

  /**
   * Returns the names for the properties for {@link T}.
   *
   * @return the variable map
   */
  public Set<String> variableNames() {
    return Set.copyOf(getVariableMap().keySet());
  }

  /**
   * Coverts the value's properties into a {@link SdkBindingData} map by variable name.
   *
   * @param value value to convert.
   * @return the binding data map.
   */
  public abstract Map<String, SdkBindingData<?>> toSdkBindingMap(T value);
}
