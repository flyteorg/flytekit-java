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
  SdkLiteralTypes => SdkJavaLiteralTypes
}
import org.flyte.flytekitscala.{SdkLiteralTypes => SdkScalaLiteralTypes}

/** The [[SdkBindingDataConverters]] allows you to do java <-> scala conversions
  * for [[SdkBindingData]]
  */
object SdkBindingDataConverters {

  /** Transform from java.lang.Long to scala Long.
    *
    * @param sdkBindingData
    *   the value to transform
    * @return
    *   the value transformed.
    */
  def toScalaLong(
      sdkBindingData: SdkBindingData[java.lang.Long]
  ): SdkBindingData[Long] = {
    sdkBindingData.as(SdkScalaLiteralTypes.integers(), l => l)
  }

  /** Transform from scala Long to java.lang.Long.
    *
    * @param sdkBindingData
    *   the value to transform
    * @return
    *   the value transformed.
    */
  def toJavaLong(
      sdkBindingData: SdkBindingData[Long]
  ): SdkBindingData[java.lang.Long] = {
    sdkBindingData.as(SdkJavaLiteralTypes.integers(), l => l)
  }

  /** Transform from java.lang.Boolean to scala Boolean.
    *
    * @param sdkBindingData
    *   the value to transform
    * @return
    *   the value transformed.
    */
  def toScalaBoolean(
      sdkBindingData: SdkBindingData[java.lang.Boolean]
  ): SdkBindingData[Boolean] = {
    sdkBindingData.as(SdkScalaLiteralTypes.booleans(), b => b)
  }

  /** Transform from scala Boolean to java.lang.Boolean.
    *
    * @param sdkBindingData
    *   the value to transform
    * @return
    *   the value transformed.
    */
  def toJavaBoolean(
      sdkBindingData: SdkBindingData[Boolean]
  ): SdkBindingData[java.lang.Boolean] = {
    sdkBindingData.as(SdkJavaLiteralTypes.booleans(), b => b)
  }

  /** Transform from scala Double to java.lang.Double.
    *
    * @param sdkBindingData
    *   the value to transform
    * @return
    *   the value transformed.
    */
  def toScalaDouble(
      sdkBindingData: SdkBindingData[java.lang.Double]
  ): SdkBindingData[Double] = {
    sdkBindingData.as(SdkScalaLiteralTypes.floats(), f => f)
  }

  /** Transform from scala Double to java.lang.Double.
    *
    * @param sdkBindingData
    *   the value to transform
    * @return
    *   the value transformed.
    */
  def toJavaDouble(
      sdkBindingData: SdkBindingData[Double]
  ): SdkBindingData[java.lang.Double] = {
    sdkBindingData.as(SdkJavaLiteralTypes.floats(), f => f)
  }

  /** Transform from java.util.List to scala List.
    *
    * @param sdkBindingData
    *   the value to transform
    * @return
    *   the value transformed.
    */
  def toScalaList[K, T](
      sdkBindingData: SdkBindingData[java.util.List[K]]
  ): SdkBindingData[List[T]] = {
    ???
  }

  /** Transform from scala List to java.util.List.
    *
    * @param sdkBindingData
    *   the value to transform
    * @return
    *   the value transformed.
    */
  def toJavaList[K, T](
      sdkBindingData: SdkBindingData[List[K]]
  ): SdkBindingData[java.util.List[T]] = {
    ???
  }

  /** Transform from scala Map to java.util.Map.
    *
    * @param sdkBindingData
    *   the value to transform
    * @return
    *   the value transformed.
    */
  def toScalaMap[K, T](
      sdkBindingData: SdkBindingData[java.util.Map[String, K]]
  ): SdkBindingData[Map[String, T]] = {
    ???
  }

  /** Transform from scala Map to java.util.Map.
    *
    * @param sdkBindingData
    *   the value to transform
    * @return
    *   the value transformed.
    */
  def toJavaMap[K, T](
      sdkBindingData: SdkBindingData[Map[String, K]]
  ): SdkBindingData[java.util.Map[String, T]] = {
    ???
  }
}
