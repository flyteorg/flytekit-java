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

object SdkBindingDataConverters {

  def toScalaLong(
      sdkBindingData: SdkBindingData[java.lang.Long]
  ): SdkBindingData[Long] = {
    SdkBindingData.create(
      sdkBindingData.idl(),
      sdkBindingData.`type`(),
      sdkBindingData.value()
    )
  }

  def toJavaLong(
      sdkBindingData: SdkBindingData[Long]
  ): SdkBindingData[java.lang.Long] = {
    SdkBindingData.create(
      sdkBindingData.idl(),
      sdkBindingData.`type`(),
      sdkBindingData.value()
    )
  }

  def toScalaBoolean(
      sdkBindingData: SdkBindingData[java.lang.Boolean]
  ): SdkBindingData[Boolean] = {
    SdkBindingData.create(
      sdkBindingData.idl(),
      sdkBindingData.`type`(),
      sdkBindingData.value()
    )
  }

  def toJavaBoolean(
      sdkBindingData: SdkBindingData[Boolean]
  ): SdkBindingData[java.lang.Boolean] = {
    SdkBindingData.create(
      sdkBindingData.idl(),
      sdkBindingData.`type`(),
      sdkBindingData.value()
    )
  }

  def toScalaDouble(
      sdkBindingData: SdkBindingData[java.lang.Double]
  ): SdkBindingData[Double] = {
    SdkBindingData.create(
      sdkBindingData.idl(),
      sdkBindingData.`type`(),
      sdkBindingData.value()
    )
  }

  def toJavaDouble(
      sdkBindingData: SdkBindingData[Double]
  ): SdkBindingData[java.lang.Double] = {
    SdkBindingData.create(
      sdkBindingData.idl(),
      sdkBindingData.`type`(),
      sdkBindingData.value()
    )
  }

  def toScalaList[T](
      sdkBindingData: SdkBindingData[java.util.List[T]]
  ): SdkBindingData[List[T]] = {
    SdkBindingData.create(
      sdkBindingData.idl(),
      sdkBindingData.`type`(),
      sdkBindingData.value().asScala.toList
    )
  }

  def toJavaList[T](
      sdkBindingData: SdkBindingData[List[T]]
  ): SdkBindingData[java.util.List[T]] = {
    SdkBindingData.create(
      sdkBindingData.idl(),
      sdkBindingData.`type`(),
      sdkBindingData.value().toList.asJava
    )
  }

  def toScalaMap[T](
      sdkBindingData: SdkBindingData[java.util.Map[String, T]]
  ): SdkBindingData[Map[String, T]] = {
    SdkBindingData.create(
      sdkBindingData.idl(),
      sdkBindingData.`type`(),
      sdkBindingData.value().asScala.toMap
    )
  }

  def toJavaMap[T](
      sdkBindingData: SdkBindingData[Map[String, T]]
  ): SdkBindingData[java.util.Map[String, T]] = {
    SdkBindingData.create(
      sdkBindingData.idl(),
      sdkBindingData.`type`(),
      sdkBindingData.value().toMap.asJava
    )
  }

}
