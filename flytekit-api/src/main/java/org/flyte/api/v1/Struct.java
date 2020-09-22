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
package org.flyte.api.v1;

import com.google.auto.value.AutoOneOf;
import com.google.auto.value.AutoValue;
import java.util.List;
import java.util.Map;

@AutoValue
public abstract class Struct {

  public abstract Map<String, Value> fields();

  public static Struct create(Map<String, Value> fields) {
    return new AutoValue_Struct(fields);
  }

  @AutoOneOf(Value.Kind.class)
  public abstract static class Value {
    public enum Kind {
      STRING_VALUE,
      BOOL_VALUE,
      LIST_VALUE,
      NULL_VALUE,
      NUMBER_VALUE,
      STRUCT_VALUE,
    }

    public abstract Kind kind();

    public abstract String stringValue();

    public abstract boolean boolValue();

    public abstract List<Value> listValue();

    public abstract Struct structValue();

    public abstract double numberValue();

    public abstract void nullValue();

    public static Value ofStringValue(String stringValue) {
      return AutoOneOf_Struct_Value.stringValue(stringValue);
    }

    public static Value ofBoolValue(boolean boolValue) {
      return AutoOneOf_Struct_Value.boolValue(boolValue);
    }

    public static Value ofListValue(List<Value> listValue) {
      return AutoOneOf_Struct_Value.listValue(listValue);
    }

    public static Value ofStructValue(Struct structValue) {
      return AutoOneOf_Struct_Value.structValue(structValue);
    }

    public static Value ofNullValue() {
      return AutoOneOf_Struct_Value.nullValue();
    }

    public static Value ofNumberValue(double numberValue) {
      return AutoOneOf_Struct_Value.numberValue(numberValue);
    }
  }
}
