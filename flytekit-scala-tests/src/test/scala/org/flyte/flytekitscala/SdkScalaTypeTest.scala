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

import collection.JavaConverters.mapAsJavaMap
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
    val expected = Map(
      "string" -> Variable.create(LiteralType.create(SimpleType.STRING), ""),
      "integer" -> Variable.create(LiteralType.create(SimpleType.INTEGER), ""),
      "float" -> Variable.create(LiteralType.create(SimpleType.FLOAT), ""),
      "boolean" -> Variable.create(LiteralType.create(SimpleType.BOOLEAN), "")
    )

    val output = SdkScalaType[Input].getVariableMap

    assertEquals(mapAsJavaMap(expected), output)
  }

  // TODO duration
  // TODO timestamp

  @Test
  def testFromLiteralMap(): Unit = {
    val input = Map(
      "string" -> Literal.of(Scalar.of(Primitive.of("string"))),
      "integer" -> Literal.of(Scalar.of(Primitive.of(1337L))),
      "float" -> Literal.of(Scalar.of(Primitive.of(42.0))),
      "boolean" -> Literal.of(Scalar.of(Primitive.of(true)))
    )

    val expected =
      Input(string = "string", integer = 1337L, float = 42.0, boolean = true)

    val output = SdkScalaType[Input].fromLiteralMap(mapAsJavaMap(input))

    assertEquals(expected, output)
  }

  @Test
  def testToLiteralMap(): Unit = {
    val input =
      Input(string = "string", integer = 1337L, float = 42.0, boolean = true)

    val expected = Map(
      "string" -> Literal.of(Scalar.of(Primitive.of("string"))),
      "integer" -> Literal.of(Scalar.of(Primitive.of(1337L))),
      "float" -> Literal.of(Scalar.of(Primitive.of(42.0))),
      "boolean" -> Literal.of(Scalar.of(Primitive.of(true)))
    )

    val output = SdkScalaType[Input].toLiteralMap(input)

    assertEquals(mapAsJavaMap(expected), output)
  }

  // Typed[String] doesn't compile aka illtyped
}
