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

import static org.flyte.flytekit.MoreCollectors.toUnmodifiableList;

import com.google.auto.value.AutoValue;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.flyte.api.v1.Struct;
import org.flyte.api.v1.Struct.Value;

@AutoValue
public abstract class SdkStruct {
  private static final SdkStruct EMPTY = builder().build();

  abstract Struct struct();

  public abstract Builder toBuilder();

  public static SdkStruct empty() {
    return EMPTY;
  }

  public static Builder builder() {
    return new AutoValue_SdkStruct.Builder().struct(Struct.of(Collections.emptyMap()));
  }

  @AutoValue.Builder
  public abstract static class Builder {
    abstract Builder struct(Struct struct);

    public Builder addBooleanField(String name, Boolean value) {
      return addValueField(name, ofNullable(value, Value::ofBoolValue));
    }

    public Builder addStringField(String name, String value) {
      return addValueField(name, ofNullable(value, Value::ofStringValue));
    }

    public Builder addIntegerField(String name, Long value) {
      return addValueField(name, Builder.<Long>ofNullable(value, Value::ofNumberValue));
    }

    public Builder addFloatField(String name, Double value) {
      return addValueField(name, ofNullable(value, Value::ofNumberValue));
    }

    public Builder addStructField(String name, SdkStruct value) {
      return addValueField(name, ofNullable(value, v -> Value.ofStructValue(v.struct())));
    }

    public Builder addIntegerCollectionField(String name, List<Long> values) {
      return this.<Long>addListValueField(name, values, Value::ofNumberValue);
    }

    public Builder addFloatCollectionField(String name, List<Double> values) {
      return this.<Double>addListValueField(name, values, Value::ofNumberValue);
    }

    public Builder addStringCollectionField(String name, List<String> values) {
      return this.<String>addListValueField(name, values, Value::ofStringValue);
    }

    public Builder addBooleanCollectionField(String name, List<Boolean> values) {
      return this.<Boolean>addListValueField(name, values, Value::ofBoolValue);
    }

    public Builder addStructCollectionField(String name, List<SdkStruct> values) {
      return this.<SdkStruct>addListValueField(name, values, x -> Value.ofStructValue(x.struct()));
    }

    Builder addValueField(String name, Value value) {
      Map<String, Value> fields = new HashMap<>();
      fields.putAll(build().struct().fields());
      fields.put(name, value);
      return struct(Struct.of(fields));
    }

    Builder addListValueField(String name, List<Value> values) {
      return addValueField(name, Value.ofListValue(values));
    }

    <T> Builder addListValueField(String name, List<T> values, Function<T, Value> fn) {
      if (values == null) {
        return addValueField(name, Value.ofNullValue());
      }

      List<Value> result =
          values.stream().map(value -> ofNullable(value, fn)).collect(toUnmodifiableList());
      return addValueField(name, Value.ofListValue(result));
    }

    private static <T> Value ofNullable(T value, Function<T, Value> fn) {
      return value == null ? Value.ofNullValue() : fn.apply(value);
    }

    public abstract SdkStruct build();
  }
}
