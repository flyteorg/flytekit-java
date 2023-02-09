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
package org.flyte.flytekit

import scala.collection.JavaConverters._

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
    SdkBindingData.create(
      sdkBindingData.idl(),
      sdkBindingData.`type`(),
      sdkBindingData.value()
    )
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
    SdkBindingData.create(
      sdkBindingData.idl(),
      sdkBindingData.`type`(),
      sdkBindingData.value()
    )
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
    SdkBindingData.create(
      sdkBindingData.idl(),
      sdkBindingData.`type`(),
      sdkBindingData.value()
    )
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
    SdkBindingData.create(
      sdkBindingData.idl(),
      sdkBindingData.`type`(),
      sdkBindingData.value()
    )
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
    SdkBindingData.create(
      sdkBindingData.idl(),
      sdkBindingData.`type`(),
      sdkBindingData.value()
    )
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
    SdkBindingData.create(
      sdkBindingData.idl(),
      sdkBindingData.`type`(),
      sdkBindingData.value()
    )
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
    SdkBindingData.create(
      sdkBindingData.idl(),
      sdkBindingData.`type`(),
      sdkBindingData.value().asScala.map(_.asInstanceOf[T]).toList
    )
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
    SdkBindingData.create(
      sdkBindingData.idl(),
      sdkBindingData.`type`(),
      sdkBindingData.value().toList.map(_.asInstanceOf[T]).asJava
    )
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
    SdkBindingData.create(
      sdkBindingData.idl(),
      sdkBindingData.`type`(),
      sdkBindingData.value().asScala.mapValues(_.asInstanceOf[T]).toMap
    )
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
    SdkBindingData.create(
      sdkBindingData.idl(),
      sdkBindingData.`type`(),
      sdkBindingData.value().mapValues(_.asInstanceOf[T]).toMap.asJava
    )
  }

}
