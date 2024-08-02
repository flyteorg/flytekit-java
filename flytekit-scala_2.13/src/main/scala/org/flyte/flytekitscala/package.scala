/*
 * Copyright 2023 Flyte Authors.
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
package org.flyte

package object flytekitscala {
  private[flytekitscala] def productElementNames(
      product: Product
  ): List[String] = {
    try {
      // scala 2.13
      product.getClass
        .getMethod("productElementNames")
        .invoke(product)
        .asInstanceOf[Iterator[String]]
        .toList
    } catch {
      case _: Throwable =>
        // fall back to java's way, less reliable and with limitations
        val methodNames = product.getClass.getDeclaredMethods.map(_.getName)
        product.getClass.getDeclaredFields
          .map(_.getName)
          .filter(methodNames.contains)
          .toList
    }
  }
}
