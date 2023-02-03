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

import java.util.Map;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.Variable;

/** A utility class for creating {@link SdkType} objects for different types. */
public class SdkTypes {

  private static final VoidSdkType VOID_SDK_TYPE = new VoidSdkType();

  private SdkTypes() {}

  /**
   * Returns a {@link org.flyte.flytekit.SdkType} for {@code Void} which contains no properties.
   *
   * @return the sdk type
   */
  public static SdkType<Void> nulls() {
    return VOID_SDK_TYPE;
  }

  private static class VoidSdkType extends SdkType<Void> {

    @Override
    public Map<String, Literal> toLiteralMap(Void value) {
      return Map.of();
    }

    @Override
    public Void fromLiteralMap(Map<String, Literal> value) {
      return null;
    }

    @Override
    public Void promiseFor(String nodeId) {
      return null;
    }

    @Override
    public Map<String, Variable> getVariableMap() {
      return Map.of();
    }

    @Override
    public Map<String, SdkBindingData<?>> toSdkBindingMap(Void value) {
      return Map.of();
    }
  }
}
