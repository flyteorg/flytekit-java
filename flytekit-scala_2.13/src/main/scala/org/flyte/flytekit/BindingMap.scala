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

import org.flyte.api.v1.BindingData
import org.flyte.flytekitscala.SdkLiteralTypes.maps

import java.util.function
import scala.collection.JavaConverters._

private[flyte] class BindingMap[T](
    valuesType: SdkLiteralType[T],
    bindingMap: Map[String, SdkBindingData[T]]
) extends SdkBindingData[Map[String, T]] {
  SdkBindingData.checkIncompatibleTypes(
    valuesType,
    bindingMap.values.toSeq.asJava
  )

  override def idl: BindingData =
    BindingData.ofMap(bindingMap.mapValues(_.idl()).toMap.asJava)

  override def `type`: SdkLiteralType[Map[String, T]] = maps(valuesType)

  override def get(): Map[String, T] = bindingMap.mapValues(_.get()).toMap

  override def as[NewT](
      newType: SdkLiteralType[NewT],
      castFunction: function.Function[Map[String, T], NewT]
  ): SdkBindingData[NewT] =
    throw new UnsupportedOperationException(
      "SdkBindingData of binding map cannot be casted"
    )

}
