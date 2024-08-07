/*
 * Copyright 2020-2023 Flyte Authors.
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

import org.flyte.api.v1.BlobType.BlobDimensionality

import java.time.{Duration, Instant}
import scala.jdk.CollectionConverters._
import org.flyte.api.v1.{
  Blob,
  BlobMetadata,
  BlobType,
  Literal,
  LiteralType,
  Primitive,
  Scalar,
  SimpleType,
  Struct,
  Variable
}
import org.flyte.flytekit.{
  SdkBindingData,
  SdkBindingDataFactory => SdkJavaBindingDataFactory
}
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows}
import org.junit.jupiter.api.Test
import org.flyte.examples.AllInputsTask.{AutoAllInputsInput, Nested}
import org.flyte.flytekit.jackson.JacksonSdkLiteralType
import org.flyte.flytekitscala.SdkLiteralTypes.{
  __TYPE,
  collections,
  maps,
  strings
}

// The constructor is reflectedly invoked so it cannot be an inner class
case class ScalarNested(
    foo: String,
    bar: Option[String],
    nestedNested: Option[ScalarNestedNested],
    nestedNestedList: List[ScalarNestedNested],
    nestedNestedMap: Map[String, ScalarNestedNested]
)
case class ScalarNestedNested(foo: String, bar: Option[String])

sealed trait TestTrait {
  val traitData: String
}

case class TestTraitClass1(
    traitData: String,
    data: String
) extends TestTrait

case class TestTraitClass2(
    traitData: String,
    n: Long
) extends TestTrait

case class TestInnerCaseClass(
    subTestList: List[TestTrait],
    subTestMap: Map[String, TestTrait]
)

case class TestCaseClass(
    subTest: SdkBindingData[List[TestInnerCaseClass]]
)

class SdkScalaTypeTest {

  private val blob = Blob.builder
    .metadata(BlobMetadata.builder.`type`(BlobType.DEFAULT).build)
    .uri("file://test")
    .build

  case class ScalarInput(
      string: SdkBindingData[String],
      integer: SdkBindingData[Long],
      float: SdkBindingData[Double],
      boolean: SdkBindingData[Boolean],
      datetime: SdkBindingData[Instant],
      duration: SdkBindingData[Duration],
      blob: SdkBindingData[Blob],
      generic: SdkBindingData[ScalarNested],
      none: SdkBindingData[Option[String]],
      some: SdkBindingData[Option[String]]
  )

  case class CollectionInput(
      strings: SdkBindingData[List[String]],
      integers: SdkBindingData[List[Long]],
      floats: SdkBindingData[List[Double]],
      booleans: SdkBindingData[List[Boolean]],
      datetimes: SdkBindingData[List[Instant]],
      durations: SdkBindingData[List[Duration]],
      generics: SdkBindingData[List[ScalarNested]],
      options: SdkBindingData[List[Option[String]]]
  )

  case class MapInput(
      stringMap: SdkBindingData[Map[String, String]],
      integerMap: SdkBindingData[Map[String, Long]],
      floatMap: SdkBindingData[Map[String, Double]],
      booleanMap: SdkBindingData[Map[String, Boolean]],
      datetimeMap: SdkBindingData[Map[String, Instant]],
      durationMap: SdkBindingData[Map[String, Duration]],
      genericMap: SdkBindingData[Map[String, ScalarNested]],
      optionMap: SdkBindingData[Map[String, Option[String]]]
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
  def testTraitCaseClass(): Unit = {
    val expected = TestCaseClass(
      SdkBindingDataFactory.of(
        SdkLiteralTypes.collections(
          SdkLiteralTypes.generics[TestInnerCaseClass]()
        ),
        List(
          TestInnerCaseClass(
            List(
              TestTraitClass1("traitData", "data1"),
              TestTraitClass2("traitData", 222)
            ),
            Map(
              "key1" -> TestTraitClass1("traitData", "data1"),
              "key2" -> TestTraitClass2("traitData", 222)
            )
          )
        )
      )
    )
    val map = SdkScalaType[TestCaseClass].toLiteralMap(expected)
    val output = SdkScalaType[TestCaseClass].fromLiteralMap(map)

    assertEquals(expected, output)
  }

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
      "duration" -> createVar(SimpleType.DURATION),
      "blob" -> Variable
        .builder()
        .literalType(LiteralType.ofBlobType(BlobType.DEFAULT))
        .description("")
        .build(),
      "generic" -> createVar(SimpleType.STRUCT),
      "none" -> createVar(SimpleType.STRUCT),
      "some" -> createVar(SimpleType.STRUCT)
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
      ),
      "blob" -> Literal.ofScalar(Scalar.ofBlob(blob)),
      "generic" -> Literal.ofScalar(
        Scalar.ofGeneric(
          Struct.of(
            Map(
              "foo" -> Struct.Value.ofStringValue("foo"),
              "bar" -> Struct.Value.ofNullValue(),
              "nestedNested" -> Struct.Value.ofStructValue(
                Struct.of(
                  Map(
                    "foo" -> Struct.Value.ofStringValue("foo"),
                    "bar" -> Struct.Value.ofStringValue("bar")
                  ).asJava
                )
              ),
              "nestedNestedList" -> Struct.Value.ofListValue(
                List(
                  Struct.Value.ofStructValue(
                    Struct.of(
                      Map(
                        "foo" -> Struct.Value.ofStringValue("foo"),
                        "bar" -> Struct.Value.ofStringValue("bar")
                      ).asJava
                    )
                  )
                ).asJava
              ),
              "nestedNestedMap" -> Struct.Value.ofStructValue(
                Struct.of(
                  Map(
                    "foo" -> Struct.Value.ofStructValue(
                      Struct.of(
                        Map(
                          "foo" -> Struct.Value.ofStringValue("foo"),
                          "bar" -> Struct.Value.ofStringValue("bar")
                        ).asJava
                      )
                    )
                  ).asJava
                )
              )
            ).asJava
          )
        )
      ),
      "none" -> Literal.ofScalar(
        Scalar.ofGeneric(
          Struct.of(Map.empty[String, Struct.Value].asJava)
        )
      ),
      "some" -> Literal.ofScalar(
        Scalar.ofGeneric(
          Struct.of(Map("value" -> Struct.Value.ofStringValue("hello")).asJava)
        )
      )
    ).asJava

    val expected =
      ScalarInput(
        string = SdkBindingDataFactory.of("string"),
        integer = SdkBindingDataFactory.of(1337L),
        float = SdkBindingDataFactory.of(42.0),
        boolean = SdkBindingDataFactory.of(true),
        datetime = SdkBindingDataFactory.of(Instant.ofEpochMilli(123456L)),
        duration = SdkBindingDataFactory.of(Duration.ofSeconds(123, 456)),
        blob = SdkBindingDataFactory.of(blob),
        generic = SdkBindingDataFactory.of(
          SdkLiteralTypes.generics(),
          ScalarNested(
            "foo",
            None,
            Some(ScalarNestedNested("foo", Some("bar"))),
            List(ScalarNestedNested("foo", Some("bar"))),
            Map("foo" -> ScalarNestedNested("foo", Some("bar")))
          )
        ),
        none = SdkBindingDataFactory.of(
          SdkLiteralTypes.generics[Option[String]](),
          Option(null)
        ),
        some = SdkBindingDataFactory.of(
          SdkLiteralTypes.generics[Option[String]](),
          Option("hello")
        )
      )

    val output = SdkScalaType[ScalarInput].fromLiteralMap(input)

    assertEquals(expected, output)
  }

  @Test
  def testScalarToLiteralMap(): Unit = {
    val input =
      ScalarInput(
        string = SdkBindingDataFactory.of("string"),
        integer = SdkBindingDataFactory.of(1337L),
        float = SdkBindingDataFactory.of(42.0),
        boolean = SdkBindingDataFactory.of(true),
        datetime = SdkBindingDataFactory.of(Instant.ofEpochMilli(123456L)),
        duration = SdkBindingDataFactory.of(Duration.ofSeconds(123, 456)),
        blob = SdkBindingDataFactory.of(blob),
        generic = SdkBindingDataFactory.of(
          SdkLiteralTypes.generics(),
          ScalarNested(
            "foo",
            Some("bar"),
            Some(ScalarNestedNested("foo", Some("bar"))),
            List(ScalarNestedNested("foo", Some("bar"))),
            Map("foo" -> ScalarNestedNested("foo", Some("bar")))
          )
        ),
        none =
          SdkBindingDataFactory.of(SdkLiteralTypes.generics(), Option(null)),
        some =
          SdkBindingDataFactory.of(SdkLiteralTypes.generics(), Option("hello"))
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
      ),
      "blob" -> Literal.ofScalar(Scalar.ofBlob(blob)),
      "generic" -> Literal.ofScalar(
        Scalar.ofGeneric(
          Struct.of(
            Map(
              "foo" -> Struct.Value.ofStringValue("foo"),
              "bar" -> Struct.Value.ofStringValue("bar"),
              __TYPE -> Struct.Value.ofStringValue(
                classOf[ScalarNested].getTypeName
              ),
              "nestedNested" -> Struct.Value.ofStructValue(
                Struct.of(
                  Map(
                    "foo" -> Struct.Value.ofStringValue("foo"),
                    "bar" -> Struct.Value.ofStringValue("bar"),
                    __TYPE -> Struct.Value.ofStringValue(
                      classOf[ScalarNestedNested].getTypeName
                    )
                  ).asJava
                )
              ),
              "nestedNestedList" -> Struct.Value.ofListValue(
                List(
                  Struct.Value.ofStructValue(
                    Struct.of(
                      Map(
                        "foo" -> Struct.Value.ofStringValue("foo"),
                        "bar" -> Struct.Value.ofStringValue("bar"),
                        __TYPE -> Struct.Value.ofStringValue(
                          classOf[ScalarNestedNested].getTypeName
                        )
                      ).asJava
                    )
                  )
                ).asJava
              ),
              "nestedNestedMap" -> Struct.Value.ofStructValue(
                Struct.of(
                  Map(
                    "foo" -> Struct.Value.ofStructValue(
                      Struct.of(
                        Map(
                          "foo" -> Struct.Value.ofStringValue("foo"),
                          "bar" -> Struct.Value.ofStringValue("bar"),
                          __TYPE -> Struct.Value.ofStringValue(
                            classOf[ScalarNestedNested].getTypeName
                          )
                        ).asJava
                      )
                    )
                  ).asJava
                )
              )
            ).asJava
          )
        )
      ),
      "none" -> Literal.ofScalar(
        Scalar.ofGeneric(
          Struct.of(
            Map(__TYPE -> Struct.Value.ofStringValue("scala.None$")).asJava
          )
        )
      ),
      "some" -> Literal.ofScalar(
        Scalar.ofGeneric(
          Struct.of(
            Map(
              "value" -> Struct.Value.ofStringValue("hello"),
              __TYPE -> Struct.Value.ofStringValue("scala.Some")
            ).asJava
          )
        )
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
      "durations" -> createCollectionVar(SimpleType.DURATION),
      "generics" -> createCollectionVar(SimpleType.STRUCT),
      "options" -> createCollectionVar(SimpleType.STRUCT)
    ).asJava

    val output = SdkScalaType[CollectionInput].getVariableMap

    assertEquals(expected, output)
  }

  @Test
  def testToSdkBindingMap(): Unit = {
    val input = ScalarInput(
      string = SdkBindingDataFactory.of("string"),
      integer = SdkBindingDataFactory.of(1337L),
      float = SdkBindingDataFactory.of(42.0),
      boolean = SdkBindingDataFactory.of(true),
      datetime = SdkBindingDataFactory.of(Instant.ofEpochMilli(123456L)),
      duration = SdkBindingDataFactory.of(Duration.ofSeconds(123, 456)),
      blob = SdkBindingDataFactory.of(blob),
      generic = SdkBindingDataFactory.of(
        SdkLiteralTypes.generics(),
        ScalarNested(
          "foo",
          Some("bar"),
          Some(ScalarNestedNested("foo", Some("bar"))),
          List(ScalarNestedNested("foo", Some("bar"))),
          Map("foo" -> ScalarNestedNested("foo", Some("bar")))
        )
      ),
      none = SdkBindingDataFactory.of(
        SdkLiteralTypes.generics[Option[String]](),
        Option(null)
      ),
      some = SdkBindingDataFactory.of(
        SdkLiteralTypes.generics[Option[String]](),
        Option("hello")
      )
    )

    val output = SdkScalaType[ScalarInput].toSdkBindingMap(input)

    val expected = Map(
      "string" -> SdkBindingDataFactory.of("string"),
      "integer" -> SdkBindingDataFactory.of(1337L),
      "float" -> SdkBindingDataFactory.of(42.0),
      "boolean" -> SdkBindingDataFactory.of(true),
      "datetime" -> SdkBindingDataFactory.of(Instant.ofEpochMilli(123456L)),
      "duration" -> SdkBindingDataFactory.of(Duration.ofSeconds(123, 456)),
      "blob" -> SdkBindingDataFactory.of(blob),
      "generic" -> SdkBindingDataFactory.of(
        SdkLiteralTypes.generics[ScalarNested](),
        ScalarNested(
          "foo",
          Some("bar"),
          Some(ScalarNestedNested("foo", Some("bar"))),
          List(ScalarNestedNested("foo", Some("bar"))),
          Map("foo" -> ScalarNestedNested("foo", Some("bar")))
        )
      ),
      "none" -> SdkBindingDataFactory.of(
        SdkLiteralTypes.generics[Option[String]](),
        Option(null)
      ),
      "some" -> SdkBindingDataFactory.of(
        SdkLiteralTypes.generics[Option[String]](),
        Option("hello")
      )
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
        strings = SdkBindingDataFactory.of(List("foo", "bar")),
        integers = SdkBindingDataFactory.of(List(1337L, 321L)),
        floats = SdkBindingDataFactory.of(List(42.0, 3.14)),
        booleans = SdkBindingDataFactory.of(List(true, false)),
        datetimes = SdkBindingDataFactory.of(
          List(Instant.ofEpochMilli(123456L), Instant.ofEpochMilli(321L))
        ),
        durations = SdkBindingDataFactory.of(
          List(Duration.ofSeconds(123, 456), Duration.ofSeconds(543, 21))
        ),
        generics = SdkBindingDataFactory.of(
          SdkLiteralTypes.generics[ScalarNested](),
          List(
            ScalarNested(
              "foo",
              Some("bar"),
              Some(ScalarNestedNested("foo", Some("bar"))),
              List(ScalarNestedNested("foo", Some("bar"))),
              Map("foo" -> ScalarNestedNested("foo", Some("bar")))
            ),
            ScalarNested(
              "foo2",
              Some("bar2"),
              Some(ScalarNestedNested("foo2", Some("bar2"))),
              List(ScalarNestedNested("foo2", Some("bar2"))),
              Map("foo2" -> ScalarNestedNested("foo2", Some("bar2")))
            )
          )
        ),
        options = SdkBindingDataFactory.of(
          SdkLiteralTypes.generics[Option[String]](),
          List(Option("hello"), Option(null))
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
      "durationMap" -> createMapVar(SimpleType.DURATION),
      "genericMap" -> createMapVar(SimpleType.STRUCT),
      "optionMap" -> createMapVar(SimpleType.STRUCT)
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
        stringMap = SdkBindingDataFactory.of(Map("k1" -> "foo")),
        integerMap = SdkBindingDataFactory.of(Map("k2" -> 321L)),
        floatMap = SdkBindingDataFactory.of(Map("k3" -> 3.14)),
        booleanMap = SdkBindingDataFactory.of(Map("k4" -> false)),
        datetimeMap =
          SdkBindingDataFactory.of(Map("k5" -> Instant.ofEpochMilli(321L))),
        durationMap =
          SdkBindingDataFactory.of(Map("k6" -> Duration.ofSeconds(543, 21))),
        genericMap = SdkBindingDataFactory.of(
          SdkLiteralTypes.generics[ScalarNested](),
          Map(
            "a" -> ScalarNested(
              "foo2",
              Some("bar2"),
              Some(ScalarNestedNested("foo2", Some("bar2"))),
              List(ScalarNestedNested("foo2", Some("bar2"))),
              Map("foo2" -> ScalarNestedNested("foo2", Some("bar2")))
            ),
            "b" -> ScalarNested(
              "foo2",
              Some("bar2"),
              Some(ScalarNestedNested("foo2", Some("bar2"))),
              List(ScalarNestedNested("foo2", Some("bar2"))),
              Map("foo2" -> ScalarNestedNested("foo2", Some("bar2")))
            )
          )
        ),
        optionMap = SdkBindingDataFactory.of(
          SdkLiteralTypes.generics[Option[String]](),
          Map("none" -> Option(null), "some" -> Option("hello"))
        )
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
        metadataList = SdkBindingDataFactory.of(
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

    val blob = Blob
      .builder()
      .uri("file://test/test.csv")
      .metadata(
        BlobMetadata
          .builder()
          .`type`(
            BlobType
              .builder()
              .format("csv")
              .dimensionality(BlobDimensionality.MULTIPART)
              .build()
          )
          .build()
      )
      .build()

    val input = AutoAllInputsInput.create(
      SdkJavaBindingDataFactory.of(2L),
      SdkJavaBindingDataFactory.of(2.0),
      SdkJavaBindingDataFactory.of("hello"),
      SdkJavaBindingDataFactory.of(true),
      SdkJavaBindingDataFactory.of(Instant.parse("2023-01-01T00:00:00Z")),
      SdkJavaBindingDataFactory.of(Duration.ZERO),
      SdkJavaBindingDataFactory.of(blob),
      SdkJavaBindingDataFactory.of(
        JacksonSdkLiteralType.of(classOf[Nested]),
        Nested.create("hello", "world")
      ),
      SdkJavaBindingDataFactory.ofStringCollection(List("1", "2", "3").asJava),
      SdkJavaBindingDataFactory.ofStringMap(Map("a" -> "2", "b" -> "3").asJava),
      SdkJavaBindingDataFactory.ofStringCollection(List.empty[String].asJava),
      SdkJavaBindingDataFactory.ofIntegerMap(
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
        blob: SdkBindingData[Blob],
        generic: SdkBindingData[Nested],
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
      input.blob(),
      input.generic(),
      toScalaList(input.l()),
      toScalaMap(input.m()),
      toScalaList(input.emptyList()),
      toScalaMap(input.emptyMap())
    )

    val expected = AutoAllInputsInputScala(
      SdkBindingDataFactory.of(2L),
      SdkBindingDataFactory.of(2.0),
      SdkBindingDataFactory.of("hello"),
      SdkBindingDataFactory.of(true),
      SdkBindingDataFactory.of(Instant.parse("2023-01-01T00:00:00Z")),
      SdkBindingDataFactory.of(Duration.ZERO),
      SdkBindingDataFactory.of(blob),
      SdkBindingDataFactory.of(
        JacksonSdkLiteralType.of(classOf[Nested]),
        Nested.create("hello", "world")
      ),
      SdkBindingDataFactory.of(List("1", "2", "3")),
      SdkBindingDataFactory.of(Map("a" -> "2", "b" -> "3")),
      SdkBindingDataFactory.ofStringCollection(List.empty[String]),
      SdkBindingDataFactory.ofIntegerMap(Map.empty[String, Long])
    )

    assertEquals(expected, scalaClass)

  }

  @Test
  def testEmptyCollection(): Unit = {
    val emptyList = SdkBindingDataFactory.ofStringCollection(List.empty[String])
    val expected =
      SdkBindingData.literal(collections(strings()), List.empty[String])

    assertEquals(emptyList, expected)
  }

  @Test
  def testEmptyMap(): Unit = {
    val emptyMap = SdkBindingDataFactory.ofStringMap(Map.empty[String, String])
    val expected =
      SdkBindingData.literal(maps(strings()), Map.empty[String, String])

    assertEquals(emptyMap, expected)
  }
}
