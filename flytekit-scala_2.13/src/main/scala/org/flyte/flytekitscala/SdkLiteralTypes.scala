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

import org.flyte.api.v1._
import org.flyte.flytekit.{
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
    v => BindingData.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(v)))
  )
  def floats(): SdkLiteralType[Double] = ScalaSdkLiteralType[Double](
    LiteralType.ofSimpleType(SimpleType.FLOAT),
    value =>
      Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofFloatValue(value))),
    _.scalar().primitive().floatValue(),
    v => BindingData.ofScalar(Scalar.ofPrimitive(Primitive.ofFloatValue(v)))
  )
  def strings(): SdkLiteralType[String] = SdkJavaLiteralTypes.strings()
  def booleans(): SdkLiteralType[Boolean] = ScalaSdkLiteralType[Boolean](
    LiteralType.ofSimpleType(SimpleType.BOOLEAN),
    value =>
      Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofBooleanValue(value))),
    _.scalar().primitive().booleanValue(),
    v => BindingData.ofScalar(Scalar.ofPrimitive(Primitive.ofBooleanValue(v)))
  )
  def datetimes(): SdkLiteralType[Instant] = SdkJavaLiteralTypes.datetimes()
  def durations(): SdkLiteralType[Duration] = SdkJavaLiteralTypes.durations()
  def collections[T](
      elementType: SdkLiteralType[T]
  ): SdkCollectionLiteralType[T] =
    new SdkCollectionLiteralType[T] {
      override def getLiteralType: LiteralType =
        LiteralType.ofCollectionType(elementType.getLiteralType)

      override def toLiteral(values: List[T]): Literal =
        Literal.ofCollection(values.map(elementType.toLiteral).asJava)

      override def fromLiteral(literal: Literal): List[T] =
        literal.collection().asScala.map(elementType.fromLiteral).toList

      override def toBindingData(value: List[T]): BindingData = {
        BindingData.ofCollection(value.map(elementType.toBindingData).asJava)
      }

      override def getElementType: SdkLiteralType[T] = elementType

      override def toString: String = "collection of " + elementType
    }

  def maps[T](valuesType: SdkLiteralType[T]): SdkMapLiteralType[T] =
    new SdkMapLiteralType[T] {
      override def getLiteralType: LiteralType =
        LiteralType.ofMapValueType(valuesType.getLiteralType)

      override def toLiteral(values: Map[String, T]): Literal =
        Literal.ofMap(values.mapValues(valuesType.toLiteral).toMap.asJava)

      override def fromLiteral(literal: Literal): Map[String, T] =
        literal.map().asScala.mapValues(valuesType.fromLiteral).toMap

      override def toBindingData(value: Map[String, T]): BindingData = {
        BindingData.ofMap(
          value.mapValues(valuesType.toBindingData).toMap.asJava
        )
      }

      override def getValuesType: SdkLiteralType[T] = valuesType

      override def toString: String = "map of " + valuesType
    }
}

trait SdkCollectionLiteralType[T] extends SdkLiteralType[List[T]] {
  def getElementType: SdkLiteralType[T]
}

trait SdkMapLiteralType[T] extends SdkLiteralType[Map[String, T]] {
  def getValuesType: SdkLiteralType[T]
}

private object ScalaSdkLiteralType {
  def apply[T](
      literalType: LiteralType,
      to: T => Literal,
      from: Literal => T,
      toData: T => BindingData
  ): SdkLiteralType[T] =
    new SdkLiteralType[T] {
      override def getLiteralType: LiteralType = literalType

      override def toLiteral(value: T): Literal = to(value)

      override def fromLiteral(literal: Literal): T = from(literal)

      override def toBindingData(value: T): BindingData = toData(value)

      override def toString: String = getLiteralType.toString
    }
}
