/*
 * Copyright 2021-2023 Flyte Authors.
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

import static java.util.stream.Collectors.toUnmodifiableList;

import com.google.auto.value.AutoValue;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.flyte.api.v1.Struct;
import org.flyte.api.v1.Struct.Value;

/**
 * Struct represents a structured data value, consisting of fields which map to dynamically typed
 * values.
 */
@AutoValue
public abstract class SdkStruct {
  private static final SdkStruct EMPTY = builder().build();

  abstract Struct struct();

  /** Returns returns a {@link SdkStruct.Builder} from this struct. */
  public abstract Builder toBuilder();

  /** Returns an empty struct, with no fields. */
  public static SdkStruct empty() {
    return EMPTY;
  }

  /** Returns returns a new {@link SdkStruct.Builder}. */
  public static Builder builder() {
    return new AutoValue_SdkStruct.Builder().struct(Struct.of(Collections.emptyMap()));
  }

  /** Builder for {@link SdkStruct}. */
  @AutoValue.Builder
  public abstract static class Builder {
    abstract Builder struct(Struct struct);

    /**
     * Adds a flyte boolean field.
     *
     * @param name name of the field
     * @param value value to set
     * @return this builder
     */
    public Builder addBooleanField(String name, Boolean value) {
      return addValueField(name, ofNullable(value, Value::ofBoolValue));
    }

    /**
     * Adds a flyte String field.
     *
     * @param name name of the field
     * @param value value to set
     * @return this builder
     */
    public Builder addStringField(String name, String value) {
      return addValueField(name, ofNullable(value, Value::ofStringValue));
    }

    /**
     * Adds a flyte integer field.
     *
     * @param name name of the field
     * @param value value to set
     * @return this builder
     */
    public Builder addIntegerField(String name, Long value) {
      return addValueField(name, Builder.<Long>ofNullable(value, Value::ofNumberValue));
    }

    /**
     * Adds a flyte float field.
     *
     * @param name name of the field
     * @param value value to set
     * @return this builder
     */
    public Builder addFloatField(String name, Double value) {
      return addValueField(name, ofNullable(value, Value::ofNumberValue));
    }

    /**
     * Adds a nested structs field.
     *
     * @param name name of the field
     * @param value value to set
     * @return this builder
     */
    public Builder addStructField(String name, SdkStruct value) {
      return addValueField(name, ofNullable(value, v -> Value.ofStructValue(v.struct())));
    }

    /**
     * Adds a list of flyte integers field.
     *
     * @param name name of the field
     * @param values values to set
     * @return this builder
     */
    public Builder addIntegerCollectionField(String name, List<Long> values) {
      return this.<Long>addListValueField(name, values, Value::ofNumberValue);
    }

    /**
     * Adds a list of flyte float field.
     *
     * @param name name of the field
     * @param values values to set
     * @return this builder
     */
    public Builder addFloatCollectionField(String name, List<Double> values) {
      return this.addListValueField(name, values, Value::ofNumberValue);
    }

    /**
     * Adds a list of flyte String field.
     *
     * @param name name of the field
     * @param values values to set
     * @return this builder
     */
    public Builder addStringCollectionField(String name, List<String> values) {
      return this.addListValueField(name, values, Value::ofStringValue);
    }

    /**
     * Adds a list of flyte booleans field.
     *
     * @param name name of the field
     * @param values values to set
     * @return this builder
     */
    public Builder addBooleanCollectionField(String name, List<Boolean> values) {
      return this.addListValueField(name, values, Value::ofBoolValue);
    }

    /**
     * Adds a list of nested structures field.
     *
     * @param name name of the field
     * @param values values to set
     * @return this builder
     */
    public Builder addStructCollectionField(String name, List<SdkStruct> values) {
      return this.addListValueField(name, values, x -> Value.ofStructValue(x.struct()));
    }

    Builder addValueField(String name, Value value) {
      Map<String, Value> fields = new HashMap<>(build().struct().fields());
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

    /** Returns builds a {@link SdkStruct}. */
    public abstract SdkStruct build();
  }
}
