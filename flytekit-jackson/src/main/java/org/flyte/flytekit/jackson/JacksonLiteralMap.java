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
package org.flyte.flytekit.jackson;

import java.util.Map;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.LiteralType;

/** Class used to register custom serializer for maps. */
public class JacksonLiteralMap {
  private final Map<String, Literal> value;
  private final Map<String, LiteralType> type;

  public JacksonLiteralMap(Map<String, Literal> value, Map<String, LiteralType> type) {
    this.value = value;
    this.type = type;
  }

  public Map<String, Literal> getLiteralMap() {
    return value;
  }

  public Map<String, LiteralType> getLiteralTypeMap() {
    return type;
  }
}
