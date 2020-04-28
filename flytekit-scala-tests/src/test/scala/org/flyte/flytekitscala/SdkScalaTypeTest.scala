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
package org.flyte.flytekitscala

import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap
import org.flyte.api.v1.{
  Literal,
  LiteralType,
  Primitive,
  Scalar,
  SimpleType,
  Variable
}
import org.junit.Assert.assertEquals
import org.junit.Test

class SdkScalaTypeTest {

  case class Input(
      string: String,
      integer: Long,
      float: Double,
      boolean: Boolean
  )

  @Test
  def testInterface(): Unit = {
    val expected = ImmutableMap
      .builder()
      .put("string", Variable.create(LiteralType.create(SimpleType.STRING), ""))
      .put(
        "integer",
        Variable.create(LiteralType.create(SimpleType.INTEGER), "")
      )
      .put("float", Variable.create(LiteralType.create(SimpleType.FLOAT), ""))
      .put(
        "boolean",
        Variable.create(LiteralType.create(SimpleType.BOOLEAN), "")
      )
      .build()

    val output = SdkScalaType[Input].getVariableMap

    assertEquals(expected, output)
  }

  // TODO duration
  // TODO timestamp

  @Test
  def testFromLiteralMap(): Unit = {
    val input = ImmutableMap
      .builder()
      .put("string", Literal.of(Scalar.create(Primitive.of("string"))))
      .put("integer", Literal.of(Scalar.create(Primitive.of(1337L))))
      .put("float", Literal.of(Scalar.create(Primitive.of(42.0))))
      .put("boolean", Literal.of(Scalar.create(Primitive.of(true))))
      .build()

    val expected =
      Input(string = "string", integer = 1337L, float = 42.0, boolean = true)

    val output = SdkScalaType[Input].fromLiteralMap(input)

    assertEquals(expected, output)
  }

  @Test
  def testToLiteralMap(): Unit = {
    val input =
      Input(string = "string", integer = 1337L, float = 42.0, boolean = true)

    val expected = ImmutableMap
      .builder()
      .put("string", Literal.of(Scalar.create(Primitive.of("string"))))
      .put("integer", Literal.of(Scalar.create(Primitive.of(1337L))))
      .put("float", Literal.of(Scalar.create(Primitive.of(42.0))))
      .put("boolean", Literal.of(Scalar.create(Primitive.of(true))))
      .build()

    val output = SdkScalaType[Input].toLiteralMap(input)

    assertEquals(expected, output)
  }

  // Typed[String] doesn't compile aka illtyped
}
