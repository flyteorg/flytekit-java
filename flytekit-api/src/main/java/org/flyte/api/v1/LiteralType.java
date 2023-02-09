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

import com.google.auto.value.AutoOneOf;

/** Defines a strong type to allow type checking between interfaces. */
@AutoOneOf(LiteralType.Kind.class)
public abstract class LiteralType {

  public enum Kind {
    /** A simple type that can be compared one-to-one with another. */
    SIMPLE_TYPE,

    /** A complex type that requires matching of inner fields. */
    SCHEMA_TYPE,

    /** Defines the type of the value of a collection. Only homogeneous collections are allowed. */
    COLLECTION_TYPE,

    /** Defines the type of the value of a map type. The type of the key is always a string. */
    MAP_VALUE_TYPE,

    /** A blob might have specialized implementation details depending on associated metadata. */
    BLOB_TYPE,

    // TODO: add ENUM_TYPE, STRUCTURED_DATASET_TYPE and UNION_TYPE from
    // src/main/proto/flyteidl/core/types.proto
  }

  public abstract Kind getKind();

  public abstract SimpleType simpleType();

  public abstract SchemaType schemaType();

  public abstract LiteralType collectionType();

  public abstract LiteralType mapValueType();

  public abstract BlobType blobType();

  public static LiteralType ofSimpleType(SimpleType simpleType) {
    return AutoOneOf_LiteralType.simpleType(simpleType);
  }

  public static LiteralType ofSchemaType(SchemaType schemaType) {
    return AutoOneOf_LiteralType.schemaType(schemaType);
  }

  public static LiteralType ofCollectionType(LiteralType elementType) {
    return AutoOneOf_LiteralType.collectionType(elementType);
  }

  public static LiteralType ofMapValueType(LiteralType mapValueType) {
    return AutoOneOf_LiteralType.mapValueType(mapValueType);
  }

  public static LiteralType ofBlobType(BlobType blobType) {
    return AutoOneOf_LiteralType.blobType(blobType);
  }
}
