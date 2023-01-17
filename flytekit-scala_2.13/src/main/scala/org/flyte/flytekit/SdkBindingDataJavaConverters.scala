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

object SdkBindingDataJavaConverters {

  private def converter[T, K](
      sdkBindingData: SdkBindingData[T]
  ): SdkBindingData[K] = {
    SdkBindingData.create(
      sdkBindingData.idl(),
      sdkBindingData.`type`(),
      sdkBindingData.value()
    )
  }

  implicit def longConverter(
      sdkBindingData: SdkBindingData[java.lang.Long]
  ): SdkBindingData[Long] = {
    converter[java.lang.Long, Long](sdkBindingData)
  }

  implicit def booleanConverter(
      sdkBindingData: SdkBindingData[java.lang.Boolean]
  ): SdkBindingData[Boolean] = {
    converter[java.lang.Boolean, Boolean](sdkBindingData)
  }

  implicit def doubleConverter(
      sdkBindingData: SdkBindingData[java.lang.Double]
  ): SdkBindingData[Double] = {
    converter[java.lang.Double, Double](sdkBindingData)
  }

}
