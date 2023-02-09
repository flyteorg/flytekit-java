package org.flyte.flytekitscala

import org.flyte.flytekit.{SdkBindingData, SdkBindingDatas => JavaSBD, SdkLiteralTypes => JavaSLT}
import org.flyte.flytekitscala.SdkBindingDataConverters.toScalaList
import org.flyte.flytekitscala.{SdkBindingDatas => ScalaSBD, SdkLiteralTypes => ScalaSLT}
import org.junit.Test
import org.junit.Assert._

import java.{lang => j}
import java.{util => ju}

class SdkBindingDataConvertersTest {

  @Test
  def testToScalaListForIntegerCollections(): Unit = {
    val original = JavaSBD.ofIntegerCollection(ju.List.of[j.Long](1L, 2L, 3L))
    val expected = ScalaSBD.ofIntegerCollection(List(1L, 2L, 3L))

    val converted = toScalaList(original)

    assertEquals(expected, converted)
  }

  @Test
  def testToScalaListForBindCollections(): Unit = {
    val javaLongList = ju.List.of(
      SdkBindingData.literal(JavaSLT.integers(), j.Long.valueOf(1L)),
      SdkBindingData.literal(JavaSLT.integers(), j.Long.valueOf(2L)),
      SdkBindingData.literal(JavaSLT.integers(), j.Long.valueOf(3L))
    )
    val original = SdkBindingData.bindingCollection(JavaSLT.integers(), javaLongList)
    val scalaLongList = ju.List.of(
      SdkBindingData.literal(ScalaSLT.integers(), 1L),
      SdkBindingData.literal(ScalaSLT.integers(), 2L),
      SdkBindingData.literal(ScalaSLT.integers(), 3L)
    )
    val expected = SdkBindingData.bindingCollection(ScalaSLT.integers(), scalaLongList)

    val converted = toScalaList(original)

    assertEquals(expected, converted)
  }

}
