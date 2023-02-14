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

import org.flyte.flytekit.SdkLiteralType
import org.flyte.flytekitscala.SdkLiteralTypes.{of, _}
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows}
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{
  Arguments,
  ArgumentsProvider,
  ArgumentsSource
}

import java.time.{Duration, Instant}
import java.util.stream.Stream
import scala.annotation.unused

class SdkLiteralTypesTest {

  @ParameterizedTest
  @ArgumentsSource(classOf[TestOfReturnsProperTypeProvider])
  def testOfReturnsProperType(
      expected: SdkLiteralType[_],
      actual: SdkLiteralType[_]
  ): Unit = {
    assertEquals(expected, actual)
  }

  @ParameterizedTest(name = "{index} {0}")
  @ArgumentsSource(classOf[testOfThrowExceptionsForUnsupportedTypesProvider])
  def testOfThrowExceptionsForUnsupportedTypes(
      @unused reason: String,
      create: () => SdkLiteralType[_]
  ): Unit = {
    assertThrows(classOf[IllegalArgumentException], () => create())
  }

}

class TestOfReturnsProperTypeProvider extends ArgumentsProvider {
  override def provideArguments(
      context: ExtensionContext
  ): Stream[_ <: Arguments] = {
    Stream.of(
      Arguments.of(integers(), of[Long]()),
      Arguments.of(floats(), of[Double]()),
      Arguments.of(strings(), of[String]()),
      Arguments.of(booleans(), of[Boolean]()),
      Arguments.of(datetimes(), of[Instant]()),
      Arguments.of(durations(), of[Duration]()),
      Arguments.of(collections(integers()), of[List[Long]]()),
      Arguments.of(collections(floats()), of[List[Double]]()),
      Arguments.of(collections(strings()), of[List[String]]()),
      Arguments.of(collections(booleans()), of[List[Boolean]]()),
      Arguments.of(collections(datetimes()), of[List[Instant]]()),
      Arguments.of(collections(durations()), of[List[Duration]]()),
      Arguments.of(maps(integers()), of[Map[String, Long]]()),
      Arguments.of(maps(floats()), of[Map[String, Double]]()),
      Arguments.of(maps(strings()), of[Map[String, String]]()),
      Arguments.of(maps(booleans()), of[Map[String, Boolean]]()),
      Arguments.of(maps(datetimes()), of[Map[String, Instant]]()),
      Arguments.of(maps(durations()), of[Map[String, Duration]]()),
      Arguments
        .of(collections(collections(integers())), of[List[List[Long]]]()),
      Arguments
        .of(collections(collections(floats())), of[List[List[Double]]]()),
      Arguments
        .of(collections(collections(strings())), of[List[List[String]]]()),
      Arguments
        .of(collections(collections(booleans())), of[List[List[Boolean]]]()),
      Arguments
        .of(collections(collections(datetimes())), of[List[List[Instant]]]()),
      Arguments
        .of(collections(collections(durations())), of[List[List[Duration]]]()),
      Arguments
        .of(maps(maps(integers())), of[Map[String, Map[String, Long]]]()),
      Arguments
        .of(maps(maps(floats())), of[Map[String, Map[String, Double]]]()),
      Arguments
        .of(maps(maps(strings())), of[Map[String, Map[String, String]]]()),
      Arguments
        .of(maps(maps(booleans())), of[Map[String, Map[String, Boolean]]]()),
      Arguments
        .of(maps(maps(datetimes())), of[Map[String, Map[String, Instant]]]()),
      Arguments
        .of(maps(maps(durations())), of[Map[String, Map[String, Duration]]]()),
      Arguments
        .of(maps(collections(integers())), of[Map[String, List[Long]]]()),
      Arguments
        .of(maps(collections(floats())), of[Map[String, List[Double]]]()),
      Arguments
        .of(maps(collections(strings())), of[Map[String, List[String]]]()),
      Arguments
        .of(maps(collections(booleans())), of[Map[String, List[Boolean]]]()),
      Arguments
        .of(maps(collections(datetimes())), of[Map[String, List[Instant]]]()),
      Arguments
        .of(maps(collections(durations())), of[Map[String, List[Duration]]]()),
      Arguments
        .of(collections(maps(integers())), of[List[Map[String, Long]]]()),
      Arguments
        .of(collections(maps(floats())), of[List[Map[String, Double]]]()),
      Arguments
        .of(collections(maps(strings())), of[List[Map[String, String]]]()),
      Arguments
        .of(collections(maps(booleans())), of[List[Map[String, Boolean]]]()),
      Arguments
        .of(collections(maps(datetimes())), of[List[Map[String, Instant]]]()),
      Arguments.of(
        collections(maps(durations())),
        of[List[Map[String, Duration]]]()
      )
    )
  }
}

class testOfThrowExceptionsForUnsupportedTypesProvider
    extends ArgumentsProvider {
  override def provideArguments(
      context: ExtensionContext
  ): Stream[_ <: Arguments] = {
    Stream.of(
      Arguments
        .of("java type, must use java factory", () => of[java.lang.Long]()),
      Arguments.of("not a supported type", () => of[Object]()),
      Arguments.of(
        "triple nesting not supported in of",
        () => of[List[List[List[Long]]]]()
      )
    )
  }
}
