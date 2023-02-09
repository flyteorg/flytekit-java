package org.flyte.flytekitscala

import org.flyte.flytekit.SdkBindingData
import org.junit.Test


class SdkBindingDataConvertersTest {

  val sdkBindingDataConverters = SdkBindingDataConverters

  @Test
  def testToScalaListForLongList(): Unit = {
    val javaLongList = java.util.List.of(
      SdkBindingData.literal(SdkLiteralTypes.integers(), 1L),
      SdkBindingData.literal(SdkLiteralTypes.integers(), 2L),
      SdkBindingData.literal(SdkLiteralTypes.integers(), 3L)
    )
    val result = sdkBindingDataConverters.toScalaList(SdkBindingData.bindingCollection(SdkLiteralTypes.integers(), javaLongList))

  }

}
