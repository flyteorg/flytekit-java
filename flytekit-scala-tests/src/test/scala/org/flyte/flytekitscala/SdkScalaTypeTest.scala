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
  BindingData,
  Literal,
  LiteralType,
  Primitive,
  Scalar,
  SimpleType,
  Variable
}
import org.flyte.flytekit.{SdkBindingData, SdkBindingDataFactory}
import org.flyte.flytekitscala.SdkBindingDataFactory._
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows}
import org.junit.jupiter.api.Test
import org.flyte.examples.AllInputsTask.AutoAllInputsInput
import org.flyte.flytekitscala.SdkLiteralTypes.{collections, maps, strings}

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

  case class InputWithDescription(
      @Description("name to greet")
      name: String
  )

  case class InputWithNullDescription(
      @Description(null)
      name: String
  )

  @Test
  def testFieldDescription(): Unit = {
    val expected = Map(
      "name" -> createVar(SimpleType.STRING, "name to greet")
    ).asJava

    val output = SdkScalaType[InputWithDescription].getVariableMap
    assertEquals(expected, output)
  }

  @Test
  def testNullFieldDescription(): Unit = {
    val ex = assertThrows(
      classOf[IllegalArgumentException],
      () => SdkScalaType[InputWithNullDescription].getVariableMap
    )

    assertEquals(
      "requirement failed: Description should not be null",
      ex.getMessage
    )
  }

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

  private def createVar(simpleType: SimpleType, description: String = "") = {
    Variable
      .builder()
      .literalType(LiteralType.ofSimpleType(simpleType))
      .description(description)
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
        string = ofString("string"),
        integer = ofInteger(1337L),
        float = ofFloat(42.0),
        boolean = ofBoolean(true),
        datetime = ofDatetime(Instant.ofEpochMilli(123456L)),
        duration = ofDuration(Duration.ofSeconds(123, 456))
      )

    val output = SdkScalaType[ScalarInput].fromLiteralMap(input)

    assertEquals(expected, output)
  }

  @Test
  def testScalarToLiteralMap(): Unit = {
    val input =
      ScalarInput(
        string = ofString("string"),
        integer = ofInteger(1337L),
        float = ofFloat(42.0),
        boolean = ofBoolean(true),
        datetime = ofDatetime(Instant.ofEpochMilli(123456L)),
        duration = ofDuration(Duration.ofSeconds(123, 456))
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

  @Test
  def testToSdkBindingMap(): Unit = {
    val input = ScalarInput(
      string = ofString("string"),
      integer = ofInteger(1337L),
      float = ofFloat(42.0),
      boolean = ofBoolean(true),
      datetime = ofDatetime(Instant.ofEpochMilli(123456L)),
      duration = ofDuration(Duration.ofSeconds(123, 456))
    )

    val output = SdkScalaType[ScalarInput].toSdkBindingMap(input)

    val expected = Map(
      "string" -> ofString("string"),
      "integer" -> ofInteger(1337L),
      "float" -> ofFloat(42.0),
      "boolean" -> ofBoolean(true),
      "datetime" -> ofDatetime(Instant.ofEpochMilli(123456L)),
      "duration" -> ofDuration(Duration.ofSeconds(123, 456))
    ).asJava

    assertEquals(expected, output)
  }

  case class InputWithoutSdkBinding(notSdkBinding: String)
  @Test
  def testCaseClassWithoutSdkBindingData(): Unit = {
    val exception = assertThrows(
      classOf[IllegalStateException],
      () => {
        SdkScalaType[InputWithoutSdkBinding].toSdkBindingMap(
          InputWithoutSdkBinding("test")
        )
      }
    )

    assertEquals(
      "All the fields of the case class InputWithoutSdkBinding must be SdkBindingData[_]",
      exception.getMessage
    )
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
        strings = ofCollection(List("foo", "bar")),
        integers = ofCollection(List(1337L, 321L)),
        floats = ofCollection(List(42.0, 3.14)),
        booleans = ofCollection(List(true, false)),
        datetimes = ofCollection(
          List(Instant.ofEpochMilli(123456L), Instant.ofEpochMilli(321L))
        ),
        durations = ofCollection(
          List(Duration.ofSeconds(123, 456), Duration.ofSeconds(543, 21))
        )
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
        stringMap = ofMap(Map("k1" -> "foo")),
        integerMap = ofMap(Map("k2" -> 321L)),
        floatMap = ofMap(Map("k3" -> 3.14)),
        booleanMap = ofMap(Map("k4" -> false)),
        datetimeMap = ofMap(Map("k5" -> Instant.ofEpochMilli(321L))),
        durationMap = ofMap(Map("k6" -> Duration.ofSeconds(543, 21)))
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
        metadataList = ofCollection(
          List(
            Map("Frodo" -> "Baggins", "Sam" -> "Gamgee"),
            Map("Clark" -> "Kent", "Loise" -> "Lane")
          )
        )
      )

    val output = SdkScalaType[ComplexInput].fromLiteralMap(
      SdkScalaType[ComplexInput].toLiteralMap(input)
    )

    assertEquals(input, output)
  }

  @Test
  def testUseAutoValueAttrIntoScalaClass(): Unit = {
    import SdkBindingDataConverters._

    val input = AutoAllInputsInput.create(
      SdkBindingDataFactory.of(2L),
      SdkBindingDataFactory.of(2.0),
      SdkBindingDataFactory.of("hello"),
      SdkBindingDataFactory.of(true),
      SdkBindingDataFactory.of(Instant.parse("2023-01-01T00:00:00Z")),
      SdkBindingDataFactory.of(Duration.ZERO),
      SdkBindingDataFactory.ofStringCollection(List("1", "2", "3").asJava),
      SdkBindingDataFactory.ofStringMap(Map("a" -> "2", "b" -> "3").asJava),
      SdkBindingDataFactory.ofStringCollection(List.empty[String].asJava),
      SdkBindingDataFactory.ofIntegerMap(
        Map.empty[String, java.lang.Long].asJava
      )
    )

    case class AutoAllInputsInputScala(
        long: SdkBindingData[Long],
        double: SdkBindingData[Double],
        string: SdkBindingData[String],
        boolean: SdkBindingData[Boolean],
        instant: SdkBindingData[Instant],
        duration: SdkBindingData[Duration],
        list: SdkBindingData[List[String]],
        map: SdkBindingData[Map[String, String]],
        emptyList: SdkBindingData[List[String]],
        emptyMap: SdkBindingData[Map[String, Long]]
    )

    val scalaClass = AutoAllInputsInputScala(
      toScalaLong(input.i()),
      toScalaDouble(input.f()),
      input.s(),
      toScalaBoolean(input.b()),
      input.t(),
      input.d(),
      toScalaList(input.l()),
      toScalaMap(input.m()),
      toScalaList(input.emptyList()),
      toScalaMap(input.emptyMap())
    )

    val expected = AutoAllInputsInputScala(
      ofInteger(2L),
      ofFloat(2.0),
      ofString("hello"),
      ofBoolean(true),
      ofDatetime(Instant.parse("2023-01-01T00:00:00Z")),
      ofDuration(Duration.ZERO),
      ofCollection(List("1", "2", "3")),
      ofMap(Map("a" -> "2", "b" -> "3")),
      ofStringCollection(List.empty[String]),
      ofIntegerMap(Map.empty[String, Long])
    )

    assertEquals(expected, scalaClass)

  }

  @Test
  def testEmptyCollection(): Unit = {
    val emptyList = ofStringCollection(List.empty[String])
    val expected =
      SdkBindingData.literal(collections(strings()), List.empty[String])

    assertEquals(emptyList, expected)
  }

  @Test
  def testEmptyMap(): Unit = {
    val emptyMap = ofStringMap(Map.empty[String, String])
    val expected =
      SdkBindingData.literal(maps(strings()), Map.empty[String, String])

    assertEquals(emptyMap, expected)
  }
}
