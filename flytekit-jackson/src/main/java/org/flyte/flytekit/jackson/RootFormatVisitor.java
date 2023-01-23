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
package org.flyte.flytekit.jackson;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.introspect.AnnotatedMember;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonFormatVisitorWrapper;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonObjectFormatVisitor;
import java.util.Map;
import org.flyte.api.v1.Variable;

class RootFormatVisitor extends JsonFormatVisitorWrapper.Base {

  private VariableMapVisitor builder = null;

  RootFormatVisitor(SerializerProvider provider) {
    super(provider);
  }

  @Override
  public JsonObjectFormatVisitor expectObjectFormat(JavaType type) {
    builder = new VariableMapVisitor(getProvider());
    return builder;
  }

  public Map<String, Variable> getVariableMap() {
    if (builder == null) {
      throw new IllegalStateException("invariant failed: variableMap not set");
    }

    return builder.getVariableMap();
  }

  public Map<String, AnnotatedMember> getMembersMap() {
    if (builder == null) {
      throw new IllegalStateException("invariant failed: membersMap not set");
    }

    return builder.getMembersMap();
  }
}
