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
package org.flyte.flytekitscala

import org.flyte.api.v1.{LiteralType, SimpleType}
import org.flyte.flytekit.{
  SdkBindingData,
  SdkLiteralType,
  SdkLiteralTypes => SdkJavaLiteralTypes
}
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

  private case class TypeCastingResult(
      convertedType: SdkLiteralType[_],
      convFunction: jf.Function[Any, Any]
  )

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
    val result = toScalaType(sdkBindingData.`type`().getLiteralType)
    val elementType =
      result.convertedType.asInstanceOf[SdkLiteralType[List[ScalaT]]]
    val value = result.convFunction
      .asInstanceOf[jf.Function[ju.List[JavaT], List[ScalaT]]]

    sdkBindingData.as(elementType, value)
  }

  private def toScalaType(lt: LiteralType): TypeCastingResult = {
    lt.getKind match {
      case LiteralType.Kind.SIMPLE_TYPE =>
        lt.simpleType() match {
          case SimpleType.FLOAT =>
            TypeCastingResult(
              SdkScalaLiteralTypes.floats(),
              (f: Any) => Double.unbox(f.asInstanceOf[j.Double])
            )
          case SimpleType.STRING =>
            TypeCastingResult(
              SdkScalaLiteralTypes.strings(),
              jf.Function.identity()
            )
          case SimpleType.STRUCT => ??? // TODO not yet supported
          case SimpleType.BOOLEAN =>
            TypeCastingResult(
              SdkScalaLiteralTypes.booleans(),
              (b: Any) => Boolean.unbox(b.asInstanceOf[j.Boolean])
            )
          case SimpleType.INTEGER =>
            TypeCastingResult(
              SdkScalaLiteralTypes.integers(),
              (i: Any) => Long.unbox(i.asInstanceOf[j.Long])
            )
          case SimpleType.DATETIME =>
            TypeCastingResult(
              SdkScalaLiteralTypes.datetimes(),
              jf.Function.identity()
            )
          case SimpleType.DURATION =>
            TypeCastingResult(
              SdkScalaLiteralTypes.durations(),
              jf.Function.identity()
            )
        }
      case LiteralType.Kind.SCHEMA_TYPE => ??? // TODO not yet supported
      case LiteralType.Kind.COLLECTION_TYPE =>
        val TypeCastingResult(convertedElementType, convFunction) = toScalaType(
          lt.collectionType()
        )
        TypeCastingResult(
          SdkScalaLiteralTypes.collections(convertedElementType),
          (l: Any) =>
            l.asInstanceOf[ju.List[_]]
              .asScala
              .map(e => convFunction.apply(e))
              .toList
        )
      case LiteralType.Kind.MAP_VALUE_TYPE =>
        val TypeCastingResult(convertedElementType, convFunction) = toScalaType(
          lt.mapValueType()
        )
        TypeCastingResult(
          SdkScalaLiteralTypes.maps(convertedElementType),
          (m: Any) =>
            m.asInstanceOf[ju.Map[String, _]]
              .asScala
              .mapValues(e => convFunction.apply(e))
              .toMap
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
  def toJavaList[ScalaT, JavaT](
      sdkBindingData: SdkBindingData[List[ScalaT]]
  ): SdkBindingData[ju.List[JavaT]] = {
    val result = toJavaType(sdkBindingData.`type`().getLiteralType)
    val elementType =
      result.convertedType.asInstanceOf[SdkLiteralType[ju.List[JavaT]]]
    val value = result.convFunction
      .asInstanceOf[jf.Function[List[ScalaT], ju.List[JavaT]]]

    sdkBindingData.as(elementType, value)
  }

  private def toJavaType(lt: LiteralType): TypeCastingResult = {
    lt.getKind match {
      case LiteralType.Kind.SIMPLE_TYPE =>
        lt.simpleType() match {
          case SimpleType.FLOAT =>
            TypeCastingResult(
              SdkJavaLiteralTypes.floats(),
              (f: Any) => j.Double.valueOf(f.asInstanceOf[Double])
            )
          case SimpleType.STRING =>
            TypeCastingResult(
              SdkJavaLiteralTypes.strings(),
              jf.Function.identity()
            )
          case SimpleType.STRUCT =>
            ??? // TODO how to handle? do we support structs already?
          case SimpleType.BOOLEAN =>
            TypeCastingResult(
              SdkJavaLiteralTypes.booleans(),
              (b: Any) => j.Boolean.valueOf(b.asInstanceOf[Boolean])
            )
          case SimpleType.INTEGER =>
            TypeCastingResult(
              SdkJavaLiteralTypes.integers(),
              (i: Any) => j.Long.valueOf(i.asInstanceOf[Long])
            )
          case SimpleType.DATETIME =>
            TypeCastingResult(
              SdkJavaLiteralTypes.datetimes(),
              jf.Function.identity()
            )
          case SimpleType.DURATION =>
            TypeCastingResult(
              SdkJavaLiteralTypes.durations(),
              jf.Function.identity()
            )
        }
      case LiteralType.Kind.SCHEMA_TYPE =>
        ??? // TODO do we support schema type?
      case LiteralType.Kind.COLLECTION_TYPE =>
        val TypeCastingResult(convertedElementType, convFunction) = toJavaType(
          lt.collectionType()
        )
        TypeCastingResult(
          SdkJavaLiteralTypes.collections(convertedElementType),
          (l: Any) =>
            l.asInstanceOf[List[_]].map(e => convFunction.apply(e)).asJava
        )
      case LiteralType.Kind.MAP_VALUE_TYPE =>
        val TypeCastingResult(convertedElementType, convFunction) = toJavaType(
          lt.mapValueType()
        )
        TypeCastingResult(
          SdkJavaLiteralTypes.maps(convertedElementType),
          (m: Any) =>
            m.asInstanceOf[Map[String, _]]
              .mapValues(e => convFunction.apply(e))
              .toMap
              .asJava
        )
    }
  }

  /** Transform from scala Map to java.util.Map.
    *
    * @param sdkBindingData
    *   the value to transform
    * @return
    *   the value transformed.
    */
  def toScalaMap[JavaT, ScalaT](
      sdkBindingData: SdkBindingData[java.util.Map[String, JavaT]]
  ): SdkBindingData[Map[String, ScalaT]] = {
    val literalType = toScalaType(sdkBindingData.`type`().getLiteralType)
    val elementType =
      literalType.convertedType.asInstanceOf[SdkLiteralType[ScalaT]]
    val value = literalType.convFunction
      .asInstanceOf[jf.Function[ju.Map[String, JavaT], Map[String, ScalaT]]]
    sdkBindingData.as(
      elementType.asInstanceOf[SdkLiteralType[Map[String, ScalaT]]],
      value
    )
  }

  /** Transform from scala Map to java.util.Map.
    *
    * @param sdkBindingData
    *   the value to transform
    * @return
    *   the value transformed.
    */
  def toJavaMap[ScalaT, JavaT](
      sdkBindingData: SdkBindingData[Map[String, ScalaT]]
  ): SdkBindingData[java.util.Map[String, JavaT]] = {
    val literalType = toJavaType(sdkBindingData.`type`().getLiteralType)
    val elementType =
      literalType.convertedType.asInstanceOf[SdkLiteralType[JavaT]]
    val value = literalType.convFunction
      .asInstanceOf[jf.Function[Map[String, ScalaT], ju.Map[String, JavaT]]]
    sdkBindingData.as(
      elementType.asInstanceOf[SdkLiteralType[ju.Map[String, JavaT]]],
      value
    )
  }
}
