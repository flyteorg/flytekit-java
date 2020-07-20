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

import java.time.{Duration, Instant}

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
      boolean: Boolean,
      datetime: Instant,
      duration: Duration
  )

  @Test
  def testInterface(): Unit = {
    val expected = Map(
      "string" -> createVar(SimpleType.STRING),
      "integer" -> createVar(SimpleType.INTEGER),
      "float" -> createVar(SimpleType.FLOAT),
      "boolean" -> createVar(SimpleType.BOOLEAN),
      "datetime" -> createVar(SimpleType.DATETIME),
      "duration" -> createVar(SimpleType.DURATION)
    )

    val output = SdkScalaType[Input].getVariableMap

    assertEquals(mapAsJavaMap(expected), output)
  }

  private def createVar(simpleType: SimpleType) = {
    Variable
      .builder()
      .literalType(LiteralType.ofSimpleType(simpleType))
      .description("")
      .build()
  }

  @Test
  def testFromLiteralMap(): Unit = {
    val input = Map(
      "string" -> Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofString("string"))),
      "integer" -> Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofInteger(1337L))),
      "float" -> Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofFloat(42.0))),
      "boolean" -> Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofBoolean(true))),
      "datetime" -> Literal.ofScalar(
        Scalar.ofPrimitive(Primitive.ofDatetime(Instant.ofEpochMilli(123456L)))
      ),
      "duration" -> Literal.ofScalar(
        Scalar.ofPrimitive(Primitive.ofDuration(Duration.ofSeconds(123, 456)))
      )
    )

    val expected =
      Input(
        string = "string",
        integer = 1337L,
        float = 42.0,
        boolean = true,
        datetime = Instant.ofEpochMilli(123456L),
        duration = Duration.ofSeconds(123, 456)
      )

    val output = SdkScalaType[Input].fromLiteralMap(mapAsJavaMap(input))

    assertEquals(expected, output)
  }

  @Test
  def testToLiteralMap(): Unit = {
    val input =
      Input(
        string = "string",
        integer = 1337L,
        float = 42.0,
        boolean = true,
        datetime = Instant.ofEpochMilli(123456L),
        duration = Duration.ofSeconds(123, 456)
      )

    val expected = Map(
      "string" -> Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofString("string"))),
      "integer" -> Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofInteger(1337L))),
      "float" -> Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofFloat(42.0))),
      "boolean" -> Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofBoolean(true))),
      "datetime" -> Literal.ofScalar(
        Scalar.ofPrimitive(Primitive.ofDatetime(Instant.ofEpochMilli(123456L)))
      ),
      "duration" -> Literal.ofScalar(
        Scalar.ofPrimitive(Primitive.ofDuration(Duration.ofSeconds(123, 456)))
      )
    )

    val output = SdkScalaType[Input].toLiteralMap(input)

    assertEquals(mapAsJavaMap(expected), output)
  }

  // Typed[String] doesn't compile aka illtyped
}
