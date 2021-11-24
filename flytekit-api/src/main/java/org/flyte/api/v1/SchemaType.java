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
package org.flyte.api.v1;

import com.google.auto.value.AutoValue;
import java.util.List;

/** Defines schema columns and types to strongly type-validate schemas interoperability. */
@AutoValue
public abstract class SchemaType {
  public enum ColumnType {
    INTEGER,
    FLOAT,
    STRING,
    BOOLEAN,
    DATETIME,
    DURATION
  }

  @AutoValue
  public abstract static class Column {
    /** @return A unique name -within the schema type- for the column. */
    public abstract String name();

    /** @return The column type. This allows a limited set of types currently. */
    public abstract ColumnType type();

    public static Builder builder() {
      return new AutoValue_SchemaType_Column.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder name(String name);

      public abstract Builder type(ColumnType type);

      public abstract Column build();
    }
  }

  /** @return A list of ordered columns this schema comprises of. */
  public abstract List<Column> columns();

  public static Builder builder() {
    return new AutoValue_SchemaType.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder columns(List<Column> columns);

    public abstract SchemaType build();
  }
}
