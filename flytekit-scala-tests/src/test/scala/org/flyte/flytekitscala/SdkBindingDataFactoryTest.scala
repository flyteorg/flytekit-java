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

import org.flyte.flytekit.SdkBindingData
import org.flyte.flytekitscala.SdkLiteralTypes._
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.equalTo
import org.junit.jupiter.api.Test

import java.time.ZoneOffset.UTC
import java.time.{Duration, LocalDate}
class SdkBindingDataFactoryTest {
  @Test
  def testOfBindingCollection(): Unit = {
    val collection = List(42L, 1337L)
    val bindingCollection = collection.map(SdkBindingDataFactory.of)
    val output =
      SdkBindingDataFactory.ofBindingCollection(integers(), bindingCollection)
    assertThat(output.get, equalTo(collection))
    assertThat(output.`type`, equalTo(collections(integers())))
  }

  @Test
  def testOfBindingCollection_empty(): Unit = {
    val output = SdkBindingDataFactory.ofBindingCollection(integers(), List())
    assertThat(output.get, equalTo(List[Long]()))
    assertThat(output.`type`, equalTo(collections(integers())))
  }

  @Test
  def testOfStringCollection(): Unit = {
    val expectedValue = List("1", "2")
    val output = SdkBindingDataFactory.ofStringCollection(expectedValue)
    assertThat(output.get, equalTo(expectedValue))
    assertThat(output.`type`, equalTo(collections(strings())))
  }

  @Test
  def testOfFloatCollection(): Unit = {
    val expectedValue = List(1.1, 1.2)
    val output = SdkBindingDataFactory.ofFloatCollection(expectedValue)
    assertThat(output.get, equalTo(expectedValue))
    assertThat(output.`type`, equalTo(collections(floats())))
  }

  @Test
  def testOfIntegerCollection(): Unit = {
    val expectedValue = List(1L, 2L)
    val output = SdkBindingDataFactory.ofIntegerCollection(expectedValue)
    assertThat(output.get, equalTo(expectedValue))
    assertThat(output.`type`, equalTo(collections(integers())))
  }

  @Test
  def testOfBooleanCollection(): Unit = {
    val expectedValue = List(true, false)
    val output = SdkBindingDataFactory.ofBooleanCollection(expectedValue)
    assertThat(output.get, equalTo(expectedValue))
    assertThat(output.`type`, equalTo(collections(booleans())))
  }

  @Test
  def testOfDurationCollection(): Unit = {
    val expectedValue = List(Duration.ofDays(1), Duration.ofDays(2))
    val output = SdkBindingDataFactory.ofDurationCollection(expectedValue)
    assertThat(output.get, equalTo(expectedValue))
    assertThat(output.`type`, equalTo(collections(durations())))
  }

  @Test
  def testOfDatetimeCollection(): Unit = {
    val first = LocalDate.of(2022, 1, 16).atStartOfDay.toInstant(UTC)
    val second = LocalDate.of(2022, 1, 17).atStartOfDay.toInstant(UTC)
    val expectedValue = List(first, second)
    val output = SdkBindingDataFactory.ofDatetimeCollection(expectedValue)
    assertThat(output.get, equalTo(expectedValue))
    assertThat(output.`type`, equalTo(collections(datetimes())))
  }

  @Test
  def testOfBindingMap(): Unit = {
    val input = Map(
      "a" -> SdkBindingDataFactory.of(42L),
      "b" -> SdkBindingDataFactory.of(1337L)
    )
    val output = SdkBindingDataFactory.ofBindingMap(integers(), input)
    assertThat(output.get, equalTo(Map("a" -> 42L, "b" -> 1337L)))
    assertThat(output.`type`, equalTo(maps(integers())))
  }

  @Test
  def testOfBindingMap_empty(): Unit = {
    val output = SdkBindingDataFactory.ofBindingMap(
      integers(),
      Map[String, SdkBindingData[Long]]()
    )
    assertThat(output.get, equalTo(Map[String, Long]()))
    assertThat(output.`type`, equalTo(maps(integers())))
  }

  @Test def testOfStringMap(): Unit = {
    val expectedValue = Map("a" -> "1", "b" -> "2")
    val output: SdkBindingData[Map[String, String]] =
      SdkBindingDataFactory.ofStringMap(expectedValue)
    assertThat(output.get, equalTo(expectedValue))
    assertThat(output.`type`, equalTo(maps(strings())))
  }

  @Test def testOfFloatMap(): Unit = {
    val expectedValue = Map("a" -> 1.1, "b" -> 1.2)
    val output = SdkBindingDataFactory.ofFloatMap(expectedValue)
    assertThat(output.get, equalTo(expectedValue))
    assertThat(output.`type`, equalTo(maps(floats())))
  }

  @Test def testOfIntegerMap(): Unit = {
    val expectedValue = Map("a" -> 1L, "b" -> 2L)
    val output = SdkBindingDataFactory.ofIntegerMap(expectedValue)
    assertThat(output.get, equalTo(expectedValue))
    assertThat(output.`type`, equalTo(maps(integers())))
  }

  @Test def testOfBooleanMap(): Unit = {
    val expectedValue = Map("a" -> true, "b" -> false)
    val output = SdkBindingDataFactory.ofBooleanMap(expectedValue)
    assertThat(output.get, equalTo(expectedValue))
    assertThat(output.`type`, equalTo(maps(booleans())))
  }

  @Test def testOfDurationMap(): Unit = {
    val expectedValue =
      Map("a" -> Duration.ofDays(1), "b" -> Duration.ofDays(2))
    val output = SdkBindingDataFactory.ofDurationMap(expectedValue)
    assertThat(output.get, equalTo(expectedValue))
    assertThat(output.`type`, equalTo(maps(durations())))
  }

  @Test def testOfDatetimeMap(): Unit = {
    val first = LocalDate.of(2022, 1, 16).atStartOfDay.toInstant(UTC)
    val second = LocalDate.of(2022, 1, 17).atStartOfDay.toInstant(UTC)
    val expectedValue = Map("a" -> first, "b" -> second)
    val output = SdkBindingDataFactory.ofDatetimeMap(expectedValue)
    assertThat(output.get, equalTo(expectedValue))
    assertThat(output.`type`, equalTo(maps(datetimes())))
  }
}
