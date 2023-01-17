/*
 * Copyright 2021 Flyte Authors.
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
import scala.jdk.CollectionConverters._
import org.flyte.api.v1.{
  Literal,
  LiteralType,
  Primitive,
  Scalar,
  SimpleType,
  Variable
}
import org.flyte.flytekit.SdkBindingData
import org.junit.Assert.assertEquals
import org.junit.Test
import Implicits._
import org.flyte.examples.AllInputsTask.AutoAllInputsInput

import scala.collection.immutable.Map

class SdkScalaTypeTest {

  case class ScalarInput(
      string: SdkBindingData[String],
      integer: SdkBindingData[Long],
      float: SdkBindingData[Double],
      boolean: SdkBindingData[Boolean],
      datetime: SdkBindingData[Instant],
      duration: SdkBindingData[Duration]
  )

  case class CollectionInput(
      strings: SdkBindingData[List[String]],
      integers: SdkBindingData[List[Long]],
      floats: SdkBindingData[List[Double]],
      booleans: SdkBindingData[List[Boolean]],
      datetimes: SdkBindingData[List[Instant]],
      durations: SdkBindingData[List[Duration]]
  )

  case class MapInput(
      stringMap: SdkBindingData[Map[String, String]],
      integerMap: SdkBindingData[Map[String, Long]],
      floatMap: SdkBindingData[Map[String, Double]],
      booleanMap: SdkBindingData[Map[String, Boolean]],
      datetimeMap: SdkBindingData[Map[String, Instant]],
      durationMap: SdkBindingData[Map[String, Duration]]
  )

  case class ComplexInput(
      metadataList: SdkBindingData[List[Map[String, String]]]
  )

  @Test
  def testScalarInterface(): Unit = {
    val expected = Map(
      "string" -> createVar(SimpleType.STRING),
      "integer" -> createVar(SimpleType.INTEGER),
      "float" -> createVar(SimpleType.FLOAT),
      "boolean" -> createVar(SimpleType.BOOLEAN),
      "datetime" -> createVar(SimpleType.DATETIME),
      "duration" -> createVar(SimpleType.DURATION)
    ).asJava

    val output = SdkScalaType[ScalarInput].getVariableMap
    assertEquals(expected, output)
  }

  private def createVar(simpleType: SimpleType) = {
    Variable
      .builder()
      .literalType(LiteralType.ofSimpleType(simpleType))
      .description("")
      .build()
  }

  @Test
  def testScalarFromLiteralMap(): Unit = {
    val input = Map(
      "string" -> Literal.ofScalar(
        Scalar.ofPrimitive(Primitive.ofString("string"))
      ),
      "integer" -> Literal.ofScalar(
        Scalar.ofPrimitive(Primitive.ofInteger(1337L))
      ),
      "float" -> Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofFloat(42.0))),
      "boolean" -> Literal.ofScalar(
        Scalar.ofPrimitive(Primitive.ofBoolean(true))
      ),
      "datetime" -> Literal.ofScalar(
        Scalar.ofPrimitive(Primitive.ofDatetime(Instant.ofEpochMilli(123456L)))
      ),
      "duration" -> Literal.ofScalar(
        Scalar.ofPrimitive(Primitive.ofDuration(Duration.ofSeconds(123, 456)))
      )
    ).asJava

    val expected =
      ScalarInput(
        string = "string",
        integer = 1337L,
        float = 42.0,
        boolean = true,
        datetime = Instant.ofEpochMilli(123456L),
        duration = Duration.ofSeconds(123, 456)
      )

    val output = SdkScalaType[ScalarInput].fromLiteralMap(input)

    assertEquals(expected, output)
  }

  @Test
  def testScalarToLiteralMap(): Unit = {
    val input =
      ScalarInput(
        string = "string",
        integer = 1337L,
        float = 42.0,
        boolean = true,
        datetime = Instant.ofEpochMilli(123456L),
        duration = Duration.ofSeconds(123, 456)
      )

    val expected = Map(
      "string" -> Literal.ofScalar(
        Scalar.ofPrimitive(Primitive.ofString("string"))
      ),
      "integer" -> Literal.ofScalar(
        Scalar.ofPrimitive(Primitive.ofInteger(1337L))
      ),
      "float" -> Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofFloat(42.0))),
      "boolean" -> Literal.ofScalar(
        Scalar.ofPrimitive(Primitive.ofBoolean(true))
      ),
      "datetime" -> Literal.ofScalar(
        Scalar.ofPrimitive(Primitive.ofDatetime(Instant.ofEpochMilli(123456L)))
      ),
      "duration" -> Literal.ofScalar(
        Scalar.ofPrimitive(Primitive.ofDuration(Duration.ofSeconds(123, 456)))
      )
    ).asJava

    val output = SdkScalaType[ScalarInput].toLiteralMap(input)

    assertEquals(expected, output)
  }

  @Test
  def testCollectionInterface(): Unit = {
    val expected = Map(
      "strings" -> createCollectionVar(SimpleType.STRING),
      "integers" -> createCollectionVar(SimpleType.INTEGER),
      "floats" -> createCollectionVar(SimpleType.FLOAT),
      "booleans" -> createCollectionVar(SimpleType.BOOLEAN),
      "datetimes" -> createCollectionVar(SimpleType.DATETIME),
      "durations" -> createCollectionVar(SimpleType.DURATION)
    ).asJava

    val output = SdkScalaType[CollectionInput].getVariableMap

    assertEquals(expected, output)
  }

  private def createCollectionVar(simpleType: SimpleType) = {
    Variable
      .builder()
      .literalType(
        LiteralType.ofCollectionType(LiteralType.ofSimpleType(simpleType))
      )
      .description("")
      .build()
  }

  @Test
  def testRoundTripFromAndToCaseClassWithCollections(): Unit = {
    val input =
      CollectionInput(
        strings = List("foo", "bar"),
        integers = List(1337L, 321L),
        floats = List(42.0, 3.14),
        booleans = List(true, false),
        datetimes =
          List(Instant.ofEpochMilli(123456L), Instant.ofEpochMilli(321L)),
        durations =
          List(Duration.ofSeconds(123, 456), Duration.ofSeconds(543, 21))
      )

    val output = SdkScalaType[CollectionInput].fromLiteralMap(
      SdkScalaType[CollectionInput].toLiteralMap(input)
    )

    assertEquals(input, output)
  }

  @Test
  def testMapInterface(): Unit = {
    val expected = Map(
      "stringMap" -> createMapVar(SimpleType.STRING),
      "integerMap" -> createMapVar(SimpleType.INTEGER),
      "floatMap" -> createMapVar(SimpleType.FLOAT),
      "booleanMap" -> createMapVar(SimpleType.BOOLEAN),
      "datetimeMap" -> createMapVar(SimpleType.DATETIME),
      "durationMap" -> createMapVar(SimpleType.DURATION)
    ).asJava

    val output = SdkScalaType[MapInput].getVariableMap

    assertEquals(expected, output)
  }

  private def createMapVar(simpleType: SimpleType) = {
    Variable
      .builder()
      .literalType(
        LiteralType.ofMapValueType(LiteralType.ofSimpleType(simpleType))
      )
      .description("")
      .build()
  }

  @Test
  def testRoundTripFromAndToCaseClassWithMaps(): Unit = {
    val input =
      MapInput(
        stringMap = Map("k1" -> "foo"),
        integerMap = Map("k2" -> 321L),
        floatMap = Map("k3" -> 3.14),
        booleanMap = Map("k4" -> false),
        datetimeMap = Map("k5" -> Instant.ofEpochMilli(321L)),
        durationMap = Map("k6" -> Duration.ofSeconds(543, 21))
      )

    val output = SdkScalaType[MapInput].fromLiteralMap(
      SdkScalaType[MapInput].toLiteralMap(input)
    )

    assertEquals(input, output)
  }

  @Test
  def testComplexInterface(): Unit = {
    val expected = Map(
      "metadataList" -> Variable
        .builder()
        .literalType(
          LiteralType.ofCollectionType(
            LiteralType
              .ofMapValueType(LiteralType.ofSimpleType(SimpleType.STRING))
          )
        )
        .description("")
        .build()
    ).asJava

    val output = SdkScalaType[ComplexInput].getVariableMap

    assertEquals(expected, output)
  }

  @Test
  def testRoundTripFromAndToCaseClassWithListsOfMaps(): Unit = {
    val input =
      ComplexInput(
        metadataList = List(
          Map("Frodo" -> "Baggins", "Sam" -> "Gamgee"),
          Map("Clark" -> "Kent", "Loise" -> "Lane")
        )
      )

    val output = SdkScalaType[ComplexInput].fromLiteralMap(
      SdkScalaType[ComplexInput].toLiteralMap(input)
    )

    assertEquals(input, output)
  }

  @Test
  def testCreateAutoValueUsingScalaTypes(): Unit = {
    import org.flyte.flytekit.SdkBindingDataJavaConverters._

    val list: SdkBindingData[List[String]] = List("1", "2", "3")
    val map: SdkBindingData[Map[String, String]] = Map("a" -> "2", "b" -> "3")

    val input = AutoAllInputsInput.create(
      2L,
      2.0,
      "hello",
      true,
      Instant.parse("2023-01-01T00:00:00Z"),
      Duration.ZERO,
      list,
      map
    )

    val expected = AutoAllInputsInput.create(
      SdkBindingData.ofInteger(2L),
      SdkBindingData.ofFloat(2.0),
      SdkBindingData.ofString("hello"),
      SdkBindingData.ofBoolean(true),
      SdkBindingData.ofDatetime(Instant.parse("2023-01-01T00:00:00Z")),
      SdkBindingData.ofDuration(Duration.ZERO),
      SdkBindingData
        .ofCollection(List("1", "2", "3").asJava, SdkBindingData.ofString _),
      SdkBindingData.ofStringMap(Map("a" -> "2", "b" -> "3").asJava)
    )

    assertEquals(expected, input)
  }

  @Test
  def testUseAutoValueAttrIntoScalaClass(): Unit = {
    import org.flyte.flytekit.SdkBindingDataJavaConverters._

    val list: SdkBindingData[List[String]] = List("1", "2", "3")
    val map: SdkBindingData[Map[String, String]] = Map("a" -> "2", "b" -> "3")

    val input = AutoAllInputsInput.create(
      2L,
      2.0,
      "hello",
      true,
      Instant.parse("2023-01-01T00:00:00Z"),
      Duration.ZERO,
      list,
      map
    )

    case class AutoAllInputsInputScala(
        long: SdkBindingData[Long],
        double: SdkBindingData[Double],
        string: SdkBindingData[String],
        boolean: SdkBindingData[Boolean],
        instant: SdkBindingData[Instant],
        duration: SdkBindingData[Duration],
        list: SdkBindingData[List[String]],
        map: SdkBindingData[Map[String, String]]
    )

    val scalaClass = AutoAllInputsInputScala(
      input.i(),
      input.f(),
      input.s(),
      input.b(),
      input.t(),
      input.d(),
      input.l(),
      input.m()
    )

    val expected = AutoAllInputsInputScala(
      2L,
      2.0,
      "hello",
      true,
      Instant.parse("2023-01-01T00:00:00Z"),
      Duration.ZERO,
      List("1", "2", "3"),
      Map("a" -> "2", "b" -> "3")
    )

    assertEquals(expected, scalaClass)

  }

  // Typed[String] doesn't compile aka illtyped
}
