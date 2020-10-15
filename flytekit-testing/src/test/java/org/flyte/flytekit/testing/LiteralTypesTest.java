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
package org.flyte.flytekit.testing;

import static java.util.Collections.emptyList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.flyte.api.v1.BlobType;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.SchemaType;
import org.junit.jupiter.api.Test;

public class LiteralTypesTest {

  @Test
  public void testStringToPrettyString() {
    assertThat(LiteralTypes.toPrettyString(LiteralTypes.STRING), equalTo("STRING"));
  }

  @Test
  public void testFloatToPrettyString() {
    assertThat(LiteralTypes.toPrettyString(LiteralTypes.FLOAT), equalTo("FLOAT"));
  }

  @Test
  public void testDatetimeToPrettyString() {
    assertThat(LiteralTypes.toPrettyString(LiteralTypes.DATETIME), equalTo("DATETIME"));
  }

  @Test
  public void testDurationToPrettyString() {
    assertThat(LiteralTypes.toPrettyString(LiteralTypes.DURATION), equalTo("DURATION"));
  }

  @Test
  public void testBooleanToPrettyString() {
    assertThat(LiteralTypes.toPrettyString(LiteralTypes.BOOLEAN), equalTo("BOOLEAN"));
  }

  @Test
  public void testIntegerToPrettyString() {
    assertThat(LiteralTypes.toPrettyString(LiteralTypes.INTEGER), equalTo("INTEGER"));
  }

  @Test
  public void testBlobToPrettyString() {
    BlobType blobType =
        BlobType.builder()
            .dimensionality(BlobType.BlobDimensionality.SINGLE)
            .format("application/csv")
            .build();

    assertThat(LiteralTypes.toPrettyString(LiteralType.ofBlobType(blobType)), equalTo("BLOB"));
  }

  @Test
  public void testSchemaToPrettyString() {
    SchemaType schemaType = SchemaType.builder().columns(emptyList()).build();

    assertThat(
        LiteralTypes.toPrettyString(LiteralType.ofSchemaType(schemaType)), equalTo("SCHEMA"));
  }

  @Test
  public void testCollectionToPrettyString() {
    assertThat(
        LiteralTypes.toPrettyString(LiteralType.ofCollectionType(LiteralTypes.INTEGER)),
        equalTo("LIST<INTEGER>"));
  }

  @Test
  public void testMapToPrettyString() {
    assertThat(
        LiteralTypes.toPrettyString(LiteralType.ofMapValueType(LiteralTypes.INTEGER)),
        equalTo("MAP<STRING, INTEGER>"));
  }
}
