/*
 * Copyright 2020 Spotify AB.
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

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.Variable;

/** An utility class for creating {@link SdkType} objects for different types. */
public class SdkTypes {

  public static SdkType<Void> nulls() {
    return new VoidSdkType();
  }

  public static <T> SdkType<T> autoValue(Class<T> cls) {
    return new AutoValueSdkType<>(cls);
  }

  private static class AutoValueSdkType<T> extends SdkType<T> {
    private final Class<T> cls;

    public AutoValueSdkType(Class<T> cls) {
      this.cls = cls;
    }

    @Override
    public Map<String, Literal> toLiteralMap(T value) {
      throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public T fromLiteralMap(Map<String, Literal> value) {
      return AutoValueReflection.readValue(value, cls);
    }

    @Override
    public Map<String, Variable> getVariableMap() {
      return AutoValueReflection.interfaceOf(cls);
    }
  }

  private static class VoidSdkType extends SdkType<Void> {

    @Override
    public Map<String, Literal> toLiteralMap(Void value) {
      return ImmutableMap.of();
    }

    @Override
    public Void fromLiteralMap(Map<String, Literal> value) {
      return null;
    }

    @Override
    public Map<String, Variable> getVariableMap() {
      return ImmutableMap.of();
    }
  }
}
