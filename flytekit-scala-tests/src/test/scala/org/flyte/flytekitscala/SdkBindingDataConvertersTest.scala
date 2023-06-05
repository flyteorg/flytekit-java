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

import org.flyte.flytekit.{
  SdkBindingData,
  SdkBindingDataFactory => JavaSBD,
  SdkLiteralTypes => JavaSLT
}
import org.flyte.flytekitscala.SdkBindingDataConverters._
import org.flyte.flytekitscala.{
  SdkBindingDataFactory => ScalaSBD,
  SdkLiteralTypes => ScalaSLT
}
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows}
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{
  Arguments,
  ArgumentsProvider,
  ArgumentsSource
}

import java.time.ZoneOffset.UTC
import java.time.{Duration, Instant, LocalDate}
import java.util.stream.Stream
import java.{lang => j, util => ju}
import scala.collection.JavaConverters._

class SdkBindingDataConvertersTest {

  @ParameterizedTest
  @ArgumentsSource(classOf[TestRoundTripConversionForScalarProvider])
  def testRoundTripConversionForScalars[JavaT, ScalaT](
      javaScalar: SdkBindingData[JavaT],
      toScala: SdkBindingData[JavaT] => SdkBindingData[ScalaT],
      scalaScalar: SdkBindingData[ScalaT],
      toJava: SdkBindingData[ScalaT] => SdkBindingData[JavaT]
  ): Unit = {
    val scalaConverted = toScala(javaScalar)
    val javaConverted = toJava(scalaScalar)

    assertEquals(javaScalar, javaConverted)
    assertEquals(scalaScalar, scalaConverted)
  }

  @ParameterizedTest
  @ArgumentsSource(classOf[TestRoundTripConversionForCollectionsProvider])
  def testRoundTripConversionForCollections[JavaT, ScalaT](
      javaCollection: SdkBindingData[ju.List[JavaT]],
      scalaCollection: SdkBindingData[List[ScalaT]]
  ): Unit = {
    val scalaConverted = toScalaList(javaCollection)
    val javaConverted = toJavaList(scalaConverted)

    assertEquals(javaCollection, javaConverted)
    assertEquals(scalaCollection, scalaConverted)
  }

  @ParameterizedTest
  @ArgumentsSource(classOf[TestRoundTripConversionForMapProvider])
  def testRoundTripConversionForMap[JavaT, ScalaT](
      javaMap: SdkBindingData[ju.Map[String, JavaT]],
      scalaMap: SdkBindingData[Map[String, ScalaT]]
  ): Unit = {
    val scalaConverted = toScalaMap(javaMap)
    val javaConverted = toJavaMap(scalaMap)

    assertEquals(javaMap, javaConverted)
    assertEquals(scalaMap, scalaConverted)
  }

  @Test
  def testToScalaListForComplexBindCollectionsShouldWork(): Unit = {
    val javaLongList = ju.List.of(
      SdkBindingData.literal(
        JavaSLT.collections(JavaSLT.maps(JavaSLT.integers())),
        ju.List.of(
          ju.Map.of("a", j.Long.valueOf(1L)),
          ju.Map.of("b", j.Long.valueOf(2L)),
          ju.Map.of("c", j.Long.valueOf(3L))
        )
      ),
      SdkBindingData.literal(
        JavaSLT.collections(JavaSLT.maps(JavaSLT.integers())),
        ju.List.of(
          ju.Map.of("a", j.Long.valueOf(1L)),
          ju.Map.of("b", j.Long.valueOf(2L)),
          ju.Map.of("c", j.Long.valueOf(3L))
        )
      ),
      SdkBindingData.literal(
        JavaSLT.collections(JavaSLT.maps(JavaSLT.integers())),
        ju.List.of(
          ju.Map.of("a", j.Long.valueOf(1L)),
          ju.Map.of("b", j.Long.valueOf(2L)),
          ju.Map.of("c", j.Long.valueOf(3L))
        )
      )
    )

    val original =
      SdkBindingData.bindingCollection(
        JavaSLT.collections(JavaSLT.maps(JavaSLT.integers())),
        javaLongList
      )

    assertEquals(
      ScalaSBD.of(
        List(
          List(Map("a" -> 1L), Map("b" -> 2L), Map("c" -> 3L)),
          List(Map("a" -> 1L), Map("b" -> 2L), Map("c" -> 3L)),
          List(Map("a" -> 1L), Map("b" -> 2L), Map("c" -> 3L))
        )
      ),
      toScalaList(original)
    )
  }

  @Test
  def testToJavaListForComplexBindCollectionsShouldWork(): Unit = {
    val scalaLongList = List(
      SdkBindingData.literal(
        ScalaSLT.collections(ScalaSLT.maps(ScalaSLT.integers())),
        List(
          Map("a" -> 1L),
          Map("b" -> 2L),
          Map("c" -> 3L)
        )
      ),
      SdkBindingData.literal(
        ScalaSLT.collections(ScalaSLT.maps(ScalaSLT.integers())),
        List(
          Map("a" -> 1L),
          Map("b" -> 2L),
          Map("c" -> 3L)
        )
      ),
      SdkBindingData.literal(
        ScalaSLT.collections(ScalaSLT.maps(ScalaSLT.integers())),
        List(
          Map("a" -> 1L),
          Map("b" -> 2L),
          Map("c" -> 3L)
        )
      )
    )

    val original =
      ScalaSBD.ofBindingCollection(
        ScalaSLT.collections(ScalaSLT.maps(ScalaSLT.integers())),
        scalaLongList
      )

    assertEquals(
      JavaSBD.of(
        JavaSLT.collections(JavaSLT.maps(JavaSLT.integers())),
        ju.List.of(
          ju.List.of(ju.Map.of("a" , j.Long.valueOf(1L)), ju.Map.of("b" , j.Long.valueOf(2L)), ju.Map.of("c" , j.Long.valueOf(3L))),
          ju.List.of(ju.Map.of("a" , j.Long.valueOf(1L)), ju.Map.of("b" , j.Long.valueOf(2L)), ju.Map.of("c" , j.Long.valueOf(3L))),
          ju.List.of(ju.Map.of("a" , j.Long.valueOf(1L)), ju.Map.of("b" , j.Long.valueOf(2L)), ju.Map.of("c" , j.Long.valueOf(3L)))
        )
      ),
      toJavaList(original)
    )
  }

  @Test
  def testToScalaListForBindCollectionsShouldWork(): Unit = {
    val javaLongList = ju.List.of(
      SdkBindingData.literal(JavaSLT.integers(), j.Long.valueOf(1L)),
      SdkBindingData.literal(JavaSLT.integers(), j.Long.valueOf(2L)),
      SdkBindingData.literal(JavaSLT.integers(), j.Long.valueOf(3L))
    )
    val original =
      SdkBindingData.bindingCollection(JavaSLT.integers(), javaLongList)

    assertEquals(
      ScalaSBD.of(List(1L, 2L, 3L)),
      toScalaList(original)
    )
  }

  @Test
  def testToJavaListForBindCollectionsShouldWork(): Unit = {
    val scalaLongList = List(
      SdkBindingData.literal(ScalaSLT.integers(), 1L),
      SdkBindingData.literal(ScalaSLT.integers(), 2L),
      SdkBindingData.literal(ScalaSLT.integers(), 3L)
    )
    val original = ScalaSBD.ofBindingCollection(ScalaSLT.integers(), scalaLongList)

    assertEquals(
      JavaSBD.of(JavaSLT.integers(), ju.List.of(j.Long.valueOf(1L), j.Long.valueOf(2L), j.Long.valueOf(3L))),
      toJavaList(original)
    )
  }

  @Test
  def testToScalaListForBindMapsShouldWork(): Unit = {
    val javaLongList = ju.Map.of(
      "a",
      SdkBindingData.literal(JavaSLT.integers(), j.Long.valueOf(1L)),
      "b",
      SdkBindingData.literal(JavaSLT.integers(), j.Long.valueOf(2L)),
      "c",
      SdkBindingData.literal(JavaSLT.integers(), j.Long.valueOf(3L))
    )
    val original =
      SdkBindingData.bindingMap(JavaSLT.integers(), javaLongList)

    assertEquals(
      ScalaSBD.of(Map("a" -> 1L, "b" -> 2L, "c" -> 3L)),
      toScalaMap(original)
    )
  }

  @Test
  def testToJavaListForBindMapsShouldWork(): Unit = {
    val scalaLongList = Map(
      "a" -> SdkBindingData.literal(ScalaSLT.integers(), 1L),
      "b" -> SdkBindingData.literal(ScalaSLT.integers(), 2L),
      "c" -> SdkBindingData.literal(ScalaSLT.integers(), 3L)
    )

    val original = ScalaSBD.ofBindingMap(ScalaSLT.integers(), scalaLongList)

    assertEquals(
      JavaSBD.of(JavaSLT.integers(), ju.Map.of("a", j.Long.valueOf(1L), "b", j.Long.valueOf(2L), "c", j.Long.valueOf(3L))),
      toJavaMap(original)
    )
  }
}

class TestRoundTripConversionForScalarProvider extends ArgumentsProvider {
  override def provideArguments(
      context: ExtensionContext
  ): Stream[_ <: Arguments] = {
    Stream.of(
      Arguments.of(
        JavaSBD.of(j.Long.valueOf(1L)),
        d => toScalaLong(d),
        ScalaSBD.of(1L),
        d => toJavaLong(d)
      ),
      Arguments.of(
        JavaSBD.of(j.Double.valueOf(1.0)),
        d => toScalaDouble(d),
        ScalaSBD.of(1.0),
        d => toJavaDouble(d)
      ),
      Arguments.of(
        JavaSBD.of(j.Boolean.valueOf(true)),
        d => toScalaBoolean(d),
        ScalaSBD.of(true),
        d => toJavaBoolean(d)
      )
    )
  }
}

class TestRoundTripConversionForCollectionsProvider extends ArgumentsProvider {
  override def provideArguments(
      context: ExtensionContext
  ): Stream[_ <: Arguments] = {
    val date1 = LocalDate.now().atStartOfDay(UTC).toInstant
    val date2 = LocalDate.of(2023, 1, 1).atStartOfDay(UTC).toInstant
    Stream.of(
      Arguments.of(
        JavaSBD.ofIntegerCollection(ju.List.of[j.Long](1L, 2L, 3L)),
        ScalaSBD.ofIntegerCollection(List(1L, 2L, 3L))
      ),
      Arguments.of(
        JavaSBD.ofFloatCollection(ju.List.of[j.Double](1.0, 2.0, 3.0)),
        ScalaSBD.ofFloatCollection(List(1.0, 2.0, 3.0))
      ),
      Arguments.of(
        JavaSBD.ofStringCollection(ju.List.of[j.String]("a", "b", "c")),
        ScalaSBD.ofStringCollection(List("a", "b", "c"))
      ),
      Arguments.of(
        JavaSBD.ofBooleanCollection(ju.List.of[j.Boolean](true, false, true)),
        ScalaSBD.ofBooleanCollection(List(true, false, true))
      ),
      Arguments.of(
        JavaSBD.ofDatetimeCollection(ju.List.of[Instant](date1, date2)),
        ScalaSBD.ofDatetimeCollection(List(date1, date2))
      ),
      Arguments.of(
        JavaSBD.ofDurationCollection(
          ju.List.of[Duration](Duration.ZERO, Duration.ofSeconds(5))
        ),
        ScalaSBD.ofDurationCollection(
          List(Duration.ZERO, Duration.ofSeconds(5))
        )
      ),
      Arguments.of(
        JavaSBD.of(
          JavaSLT.collections(JavaSLT.strings()),
          ju.List.of(ju.List.of("frodo", "sam"), ju.List.of("harry", "ron"))
        ),
        ScalaSBD.of(
          ScalaSLT.collections(ScalaSLT.strings()),
          List(List("frodo", "sam"), List("harry", "ron"))
        )
      ),
      Arguments.of(
        JavaSBD.of(
          JavaSLT.maps(JavaSLT.strings()),
          ju.List.of(ju.Map.of("frodo", "sam"), ju.Map.of("harry", "ron"))
        ),
        ScalaSBD.of(
          ScalaSLT.maps(ScalaSLT.strings()),
          List(Map("frodo" -> "sam"), Map("harry" -> "ron"))
        )
      )
    )
  }
}

class TestRoundTripConversionForMapProvider extends ArgumentsProvider {
  override def provideArguments(
      context: ExtensionContext
  ): Stream[_ <: Arguments] = {
    val date1 = LocalDate.now().atStartOfDay(UTC).toInstant
    val date2 = LocalDate.of(2023, 1, 1).atStartOfDay(UTC).toInstant
    Stream.of(
      Arguments.of(
        JavaSBD.ofIntegerMap(
          ju.Map.of[String, j.Long]("a", 1L, "b", 2L, "c", 3L)
        ),
        ScalaSBD.ofIntegerMap(Map("a" -> 1L, "b" -> 2L, "c" -> 3L))
      ),
      Arguments.of(
        JavaSBD.ofFloatMap(
          ju.Map.of[String, j.Double]("a", 1.0, "b", 2.0, "c", 3.0)
        ),
        ScalaSBD.ofFloatMap(Map("a" -> 1.0, "b" -> 2.0, "c" -> 3.0))
      ),
      Arguments.of(
        JavaSBD.ofStringMap(
          ju.Map.of[String, j.String]("a", "a", "b", "b", "c", "c")
        ),
        ScalaSBD.ofStringMap(Map("a" -> "a", "b" -> "b", "c" -> "c"))
      ),
      Arguments.of(
        JavaSBD.ofBooleanMap(
          ju.Map.of[String, j.Boolean]("a", true, "b", false, "c", true)
        ),
        ScalaSBD.ofBooleanMap(Map("a" -> true, "b" -> false, "c" -> true))
      ),
      Arguments.of(
        JavaSBD.ofDatetimeMap(
          ju.Map.of[String, Instant]("a", date1, "b", date2)
        ),
        ScalaSBD.ofDatetimeMap(Map("a" -> date1, "b" -> date2))
      ),
      Arguments.of(
        JavaSBD.ofDurationMap(
          ju.Map.of[String, Duration](
            "a",
            Duration.ZERO,
            "b",
            Duration.ofSeconds(5)
          )
        ),
        ScalaSBD.ofDurationMap(
          Map("a" -> Duration.ZERO, "b" -> Duration.ofSeconds(5))
        )
      ),
      Arguments.of(
        JavaSBD.of(
          JavaSLT.maps(JavaSLT.strings()),
          ju.Map.of(
            "lotr",
            ju.Map.of("frodo", "sam"),
            "hp",
            ju.Map.of("harry", "ron")
          )
        ),
        ScalaSBD.of(
          ScalaSLT.maps(ScalaSLT.strings()),
          Map("lotr" -> Map("frodo" -> "sam"), "hp" -> Map("harry" -> "ron"))
        )
      ),
      Arguments.of(
        JavaSBD.of(
          JavaSLT.collections(JavaSLT.strings()),
          ju.Map.of(
            "lotr",
            ju.List.of("frodo", "sam"),
            "hp",
            ju.List.of("harry", "ron")
          )
        ),
        ScalaSBD.of(
          ScalaSLT.collections(ScalaSLT.strings()),
          Map("lotr" -> List("frodo", "sam"), "hp" -> List("harry", "ron"))
        )
      )
    )
  }
}
