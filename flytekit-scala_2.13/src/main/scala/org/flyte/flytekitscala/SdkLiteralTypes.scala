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

import org.flyte.api.v1.{Literal, LiteralType, Primitive, Scalar, SimpleType}
import org.flyte.flytekit.SdkBindingDataConverters.{toScalaList, toScalaMap}
import org.flyte.flytekit.{
  SdkBindingData,
  SdkLiteralType,
  SdkLiteralTypes => SdkJavaLiteralTypes
}

import java.time.{Duration, Instant}
import scala.collection.JavaConverters._

object SdkLiteralTypes {
  def integers(): SdkLiteralType[Long] = ScalaSdkLiteralType[Long](
    LiteralType.ofSimpleType(SimpleType.INTEGER),
    value =>
      Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(value))),
    _.scalar().primitive().integerValue(),
    SdkBindingDatas.ofInteger
  )
  def floats(): SdkLiteralType[Double] = ScalaSdkLiteralType[Double](
    LiteralType.ofSimpleType(SimpleType.FLOAT),
    value =>
      Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofFloatValue(value))),
    _.scalar().primitive().floatValue(),
    SdkBindingDatas.ofFloat
  )
  def strings(): SdkLiteralType[String] = SdkJavaLiteralTypes.strings()
  def booleans(): SdkLiteralType[Boolean] = ScalaSdkLiteralType[Boolean](
    LiteralType.ofSimpleType(SimpleType.BOOLEAN),
    value =>
      Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofBooleanValue(value))),
    _.scalar().primitive().booleanValue(),
    SdkBindingDatas.ofBoolean
  )
  def datetimes(): SdkLiteralType[Instant] = SdkJavaLiteralTypes.datetimes()
  def durations(): SdkLiteralType[Duration] = SdkJavaLiteralTypes.durations()
  def collections[T](elementType: SdkLiteralType[T]): SdkLiteralType[List[T]] =
    new SdkLiteralType[List[T]] {
      override def getLiteralType: LiteralType =
        LiteralType.ofCollectionType(elementType.getLiteralType)

      override def toLiteral(values: List[T]): Literal =
        Literal.ofCollection(values.map(elementType.toLiteral).asJava)

      override def fromLiteral(literal: Literal): List[T] =
        literal.collection().asScala.map(elementType.fromLiteral).toList

      override def toSdkBinding(value: List[T]): SdkBindingData[List[T]] = {
        toScalaList(
          SdkBindingData.create(
            value.map(elementType.toSdkBinding).asJava,
            elementType.getLiteralType,
            value.asJava
          )
        )
      }
    }

  def maps[T](valuesType: SdkLiteralType[T]): SdkLiteralType[Map[String, T]] =
    new SdkLiteralType[Map[String, T]] {
      override def getLiteralType: LiteralType =
        LiteralType.ofCollectionType(valuesType.getLiteralType)

      override def toLiteral(values: Map[String, T]): Literal =
        Literal.ofMap(values.mapValues(valuesType.toLiteral).toMap.asJava)

      override def fromLiteral(literal: Literal): Map[String, T] =
        literal.map().asScala.mapValues(valuesType.fromLiteral).toMap

      override def toSdkBinding(
          value: Map[String, T]
      ): SdkBindingData[Map[String, T]] = {
        toScalaMap(
          SdkBindingData.create(
            value.mapValues(valuesType.toSdkBinding).toMap.asJava,
            valuesType.getLiteralType,
            value.asJava
          )
        )
      }
    }
}

private object ScalaSdkLiteralType {
  def apply[T](
      literalType: LiteralType,
      to: T => Literal,
      from: Literal => T,
      toData: T => SdkBindingData[T]
  ): SdkLiteralType[T] =
    new SdkLiteralType[T] {
      override def getLiteralType: LiteralType = literalType

      override def toLiteral(value: T): Literal = to(value)

      override def fromLiteral(literal: Literal): T = from(literal)

      override def toSdkBinding(value: T): SdkBindingData[T] = toData(value)
    }
}
