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
import org.flyte.flytekitscala.SdkLiteralTypes.collections

import java.util.function
import scala.collection.JavaConverters._

private[flyte] class BindingCollection[T](
    elementType: SdkLiteralType[T],
    bindingCollection: List[SdkBindingData[T]]
) extends SdkBindingData[List[T]] {
  SdkBindingData.checkIncompatibleTypes(elementType, bindingCollection.asJava)

  override def idl: BindingData =
    BindingData.ofCollection(bindingCollection.map(_.idl()).asJava)

  override def `type`: SdkLiteralType[List[T]] = collections(elementType)

  override def get(): List[T] = bindingCollection.map(_.get())

  override def as[NewT](
      newType: SdkLiteralType[NewT],
      castFunction: function.Function[List[T], NewT]
  ): SdkBindingData[NewT] =
    throw new UnsupportedOperationException(
      "SdkBindingData of binding collection cannot be casted"
    )
}
