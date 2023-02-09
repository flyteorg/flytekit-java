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

import org.flyte.api.v1.{LiteralType, SimpleType}
import org.flyte.flytekit.{SdkBindingData, SdkLiteralType, SdkLiteralTypes => SdkJavaLiteralTypes}
import org.flyte.flytekitscala.{SdkLiteralTypes => SdkScalaLiteralTypes}

import java.{lang => j}
import java.{util => ju}
import java.util.{function => jf}
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
      sdkBindingData: SdkBindingData[j.Long]
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
  ): SdkBindingData[j.Long] = {
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
      sdkBindingData: SdkBindingData[j.Boolean]
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
  ): SdkBindingData[j.Boolean] = {
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
      sdkBindingData: SdkBindingData[j.Double]
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
  ): SdkBindingData[j.Double] = {
    sdkBindingData.as(SdkJavaLiteralTypes.floats(), f => f)
  }

  /** Transform from java.util.List to scala List.
    *
    * @param sdkBindingData
    *   the value to transform
    * @return
    *   the value transformed.
    */
  def toScalaList[JavaT, ScalaT](
      sdkBindingData: SdkBindingData[java.util.List[JavaT]]
  ): SdkBindingData[List[ScalaT]] = {
    val literalType = toScalaType(sdkBindingData.`type`().getLiteralType)
    val elementType = literalType._1.asInstanceOf[SdkLiteralType[ScalaT]]
    val value = literalType._2.asInstanceOf[jf.Function[ju.List[JavaT], List[ScalaT]]]
    sdkBindingData.as(elementType.asInstanceOf[SdkLiteralType[List[ScalaT]]], value)
  }

  private def toScalaType(lt: LiteralType): (SdkLiteralType[_], jf.Function[Any, Any]) = {
    lt.getKind match {
      case LiteralType.Kind.SIMPLE_TYPE =>
        lt.simpleType() match {
          case SimpleType.FLOAT => (SdkScalaLiteralTypes.floats(), (f: Any) => Double.unbox(f.asInstanceOf[j.Double]))
          case SimpleType.STRING => (SdkScalaLiteralTypes.strings(), jf.Function.identity())
          case SimpleType.STRUCT => ??? // TODO how to handle? do we support structs already?
          case SimpleType.BOOLEAN => (SdkScalaLiteralTypes.booleans(), (b: Any) => Boolean.unbox(b.asInstanceOf[j.Boolean]))
          case SimpleType.INTEGER => (SdkScalaLiteralTypes.integers(), (i: Any) => Long.unbox(i.asInstanceOf[j.Long]))
          case SimpleType.DATETIME => (SdkScalaLiteralTypes.datetimes(), jf.Function.identity())
          case SimpleType.DURATION => (SdkScalaLiteralTypes.durations(), jf.Function.identity())
        }
      case LiteralType.Kind.BLOB_TYPE => ??? // TODO do we support blob?
      case LiteralType.Kind.SCHEMA_TYPE => ??? // TODO do we support schema type?
      case LiteralType.Kind.COLLECTION_TYPE =>
        val (convertedElementType, convFunction) = toScalaType(lt.collectionType())
        (
          SdkScalaLiteralTypes.collections(convertedElementType),
          (l: Any) => l.asInstanceOf[ju.List[_]].asScala.map(e => convFunction.apply(e)).toList
        )
      case LiteralType.Kind.MAP_VALUE_TYPE =>
        val (convertedElementType, convFunction) = toScalaType(lt.collectionType())
        (
          SdkScalaLiteralTypes.maps(convertedElementType),
          (m: Any) => m.asInstanceOf[ju.Map[String, _]].asScala.mapValues(e => convFunction.apply(e)).toMap
        )
    }
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
  ): SdkBindingData[ju.List[T]] = {
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
