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
import scala.reflect.runtime.universe.{TypeTag, typeOf}

object SdkLiteralTypes {

  /** [[SdkLiteralType]] for the specified Scala type.
    *
    * | Scala type   | Returned type                                                 |
    * |:-------------|:--------------------------------------------------------------|
    * | [[Long]]     | {{{SdkLiteralType[Long]}}}, equivalent to [[integers()]]      |
    * | [[Double]]   | {{{SdkLiteralType[Double]}}}, equivalent to [[floats()]]      |
    * | [[String]]   | {{{SdkLiteralType[String]}}}, equivalent to [[strings]]       |
    * | [[Boolean]]  | {{{SdkLiteralType[Boolean]}}}, equivalent to [[booleans()]]   |
    * | [[Instant]]  | {{{SdkLiteralType[Instant]}}}, equivalent to [[datetimes()]]  |
    * | [[Duration]] | {{{SdkLiteralType[Duration]}}}, equivalent to [[durations()]] |
    * @tparam T
    *   Scala type used to decide what [[SdkLiteralType]] to return.
    * @return
    *   the [[SdkLiteralType]] based on the java type
    */
  def of[T: TypeTag](): SdkLiteralType[T] = {
    typeOf[T] match {
      case t if t =:= typeOf[Long] => integers().asInstanceOf[SdkLiteralType[T]]
      case t if t =:= typeOf[Double] => floats().asInstanceOf[SdkLiteralType[T]]
      case t if t =:= typeOf[String] =>
        strings().asInstanceOf[SdkLiteralType[T]]
      case t if t =:= typeOf[Boolean] =>
        booleans().asInstanceOf[SdkLiteralType[T]]
      case t if t =:= typeOf[Instant] =>
        datetimes().asInstanceOf[SdkLiteralType[T]]
      case t if t =:= typeOf[Duration] =>
        durations().asInstanceOf[SdkLiteralType[T]]

      case t if t =:= typeOf[List[Long]] =>
        collections(integers()).asInstanceOf[SdkLiteralType[T]]
      case t if t =:= typeOf[List[Double]] =>
        collections(floats()).asInstanceOf[SdkLiteralType[T]]
      case t if t =:= typeOf[List[String]] =>
        collections(strings()).asInstanceOf[SdkLiteralType[T]]
      case t if t =:= typeOf[List[Boolean]] =>
        collections(booleans()).asInstanceOf[SdkLiteralType[T]]
      case t if t =:= typeOf[List[Instant]] =>
        collections(datetimes()).asInstanceOf[SdkLiteralType[T]]
      case t if t =:= typeOf[List[Duration]] =>
        collections(durations()).asInstanceOf[SdkLiteralType[T]]

      case t if t =:= typeOf[Map[String, Long]] =>
        maps(integers()).asInstanceOf[SdkLiteralType[T]]
      case t if t =:= typeOf[Map[String, Double]] =>
        maps(floats()).asInstanceOf[SdkLiteralType[T]]
      case t if t =:= typeOf[Map[String, String]] =>
        maps(strings()).asInstanceOf[SdkLiteralType[T]]
      case t if t =:= typeOf[Map[String, Boolean]] =>
        maps(booleans()).asInstanceOf[SdkLiteralType[T]]
      case t if t =:= typeOf[Map[String, Instant]] =>
        maps(datetimes()).asInstanceOf[SdkLiteralType[T]]
      case t if t =:= typeOf[Map[String, Duration]] =>
        maps(durations()).asInstanceOf[SdkLiteralType[T]]

      case t if t =:= typeOf[List[List[Long]]] =>
        collections(collections(integers())).asInstanceOf[SdkLiteralType[T]]
      case t if t =:= typeOf[List[List[Double]]] =>
        collections(collections(floats())).asInstanceOf[SdkLiteralType[T]]
      case t if t =:= typeOf[List[List[String]]] =>
        collections(collections(strings())).asInstanceOf[SdkLiteralType[T]]
      case t if t =:= typeOf[List[List[Boolean]]] =>
        collections(collections(booleans())).asInstanceOf[SdkLiteralType[T]]
      case t if t =:= typeOf[List[List[Instant]]] =>
        collections(collections(datetimes())).asInstanceOf[SdkLiteralType[T]]
      case t if t =:= typeOf[List[List[Duration]]] =>
        collections(collections(durations())).asInstanceOf[SdkLiteralType[T]]

      case t if t =:= typeOf[List[Map[String, Long]]] =>
        collections(maps(integers())).asInstanceOf[SdkLiteralType[T]]
      case t if t =:= typeOf[List[Map[String, Double]]] =>
        collections(maps(floats())).asInstanceOf[SdkLiteralType[T]]
      case t if t =:= typeOf[List[Map[String, String]]] =>
        collections(maps(strings())).asInstanceOf[SdkLiteralType[T]]
      case t if t =:= typeOf[List[Map[String, Boolean]]] =>
        collections(maps(booleans())).asInstanceOf[SdkLiteralType[T]]
      case t if t =:= typeOf[List[Map[String, Instant]]] =>
        collections(maps(datetimes())).asInstanceOf[SdkLiteralType[T]]
      case t if t =:= typeOf[List[Map[String, Duration]]] =>
        collections(maps(durations())).asInstanceOf[SdkLiteralType[T]]

      case t if t =:= typeOf[Map[String, Map[String, Long]]] =>
        maps(maps(integers())).asInstanceOf[SdkLiteralType[T]]
      case t if t =:= typeOf[Map[String, Map[String, Double]]] =>
        maps(maps(floats())).asInstanceOf[SdkLiteralType[T]]
      case t if t =:= typeOf[Map[String, Map[String, String]]] =>
        maps(maps(strings())).asInstanceOf[SdkLiteralType[T]]
      case t if t =:= typeOf[Map[String, Map[String, Boolean]]] =>
        maps(maps(booleans())).asInstanceOf[SdkLiteralType[T]]
      case t if t =:= typeOf[Map[String, Map[String, Instant]]] =>
        maps(maps(datetimes())).asInstanceOf[SdkLiteralType[T]]
      case t if t =:= typeOf[Map[String, Map[String, Duration]]] =>
        maps(maps(durations())).asInstanceOf[SdkLiteralType[T]]

      case t if t =:= typeOf[Map[String, List[Long]]] =>
        maps(collections(integers())).asInstanceOf[SdkLiteralType[T]]
      case t if t =:= typeOf[Map[String, List[Double]]] =>
        maps(collections(floats())).asInstanceOf[SdkLiteralType[T]]
      case t if t =:= typeOf[Map[String, List[String]]] =>
        maps(collections(strings())).asInstanceOf[SdkLiteralType[T]]
      case t if t =:= typeOf[Map[String, List[Boolean]]] =>
        maps(collections(booleans())).asInstanceOf[SdkLiteralType[T]]
      case t if t =:= typeOf[Map[String, List[Instant]]] =>
        maps(collections(datetimes())).asInstanceOf[SdkLiteralType[T]]
      case t if t =:= typeOf[Map[String, List[Duration]]] =>
        maps(collections(durations())).asInstanceOf[SdkLiteralType[T]]

      case _ =>
        throw new IllegalArgumentException(s"Unsupported type: ${typeOf[T]}")
    }
  }

  /** Returns a [[SdkLiteralType]] for flyte integers.
    *
    * @return
    *   the [[SdkLiteralType]]
    */
  def integers(): SdkLiteralType[Long] = ScalaLiteralType[Long](
    LiteralType.ofSimpleType(SimpleType.INTEGER),
    value =>
      Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(value))),
    _.scalar().primitive().integerValue(),
    v => BindingData.ofScalar(Scalar.ofPrimitive(Primitive.ofIntegerValue(v))),
    "integers"
  )

  /** Returns a [[SdkLiteralType]] for flyte floats.
    *
    * @return
    *   the [[SdkLiteralType]]
    */
  def floats(): SdkLiteralType[Double] = ScalaLiteralType[Double](
    LiteralType.ofSimpleType(SimpleType.FLOAT),
    value =>
      Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofFloatValue(value))),
    _.scalar().primitive().floatValue(),
    v => BindingData.ofScalar(Scalar.ofPrimitive(Primitive.ofFloatValue(v))),
    "floats"
  )

  /** Returns a [[SdkLiteralType]] for string.
    *
    * @return
    *   the [[SdkLiteralType]]
    */
  def strings(): SdkLiteralType[String] = SdkJavaLiteralTypes.strings()

  /** Returns a [[SdkLiteralType]] for booleans.
    *
    * @return
    *   the [[SdkLiteralType]]
    */
  def booleans(): SdkLiteralType[Boolean] = ScalaLiteralType[Boolean](
    LiteralType.ofSimpleType(SimpleType.BOOLEAN),
    value =>
      Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofBooleanValue(value))),
    _.scalar().primitive().booleanValue(),
    v => BindingData.ofScalar(Scalar.ofPrimitive(Primitive.ofBooleanValue(v))),
    "booleans"
  )

  /** Returns a [[SdkLiteralType]] for flyte date times.
    *
    * @return
    *   the [[SdkLiteralType]]
    */
  def datetimes(): SdkLiteralType[Instant] = SdkJavaLiteralTypes.datetimes()

  /** Returns a [[SdkLiteralType]] for durations.
    *
    * @return
    *   the [[SdkLiteralType]]
    */
  def durations(): SdkLiteralType[Duration] = SdkJavaLiteralTypes.durations()

  /** Returns a [[SdkLiteralType]] for flyte collections.
    *
    * @param elementType
    *   the [[SdkLiteralType]] representing the types of the elements of the
    *   collection.
    * @tparam T
    *   the Scala type of the elements of the collection.
    * @return
    *   the [[SdkLiteralType]]
    */
  def collections[T](
      elementType: SdkLiteralType[T]
  ): SdkLiteralType[List[T]] =
    new SdkLiteralType[List[T]] {
      override def getLiteralType: LiteralType =
        LiteralType.ofCollectionType(elementType.getLiteralType)

      override def toLiteral(values: List[T]): Literal =
        Literal.ofCollection(values.map(elementType.toLiteral).asJava)

      override def fromLiteral(literal: Literal): List[T] =
        literal.collection().asScala.map(elementType.fromLiteral).toList

      override def toBindingData(value: List[T]): BindingData =
        BindingData.ofCollection(value.map(elementType.toBindingData).asJava)

      override def toString = s"collection of [$elementType]"
    }

  /** Returns a [[SdkLiteralType]] for flyte maps.
    *
    * @param valuesType
    *   the [[SdkLiteralType]] representing the types of the map's values.
    * @tparam T
    *   the Scala type of the map's values, keys are always string.
    * @return
    *   the [[SdkLiteralType]]
    */
  def maps[T](valuesType: SdkLiteralType[T]): SdkLiteralType[Map[String, T]] =
    new SdkLiteralType[Map[String, T]] {
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

      override def toString: String = s"map of [$valuesType]"
    }
}

private object ScalaLiteralType {
  def apply[T](
      literalType: LiteralType,
      to: T => Literal,
      from: Literal => T,
      toData: T => BindingData,
      strRep: String
  ): SdkLiteralType[T] =
    new SdkLiteralType[T] {
      override def getLiteralType: LiteralType = literalType

      override def toLiteral(value: T): Literal = to(value)

      override def fromLiteral(literal: Literal): T = from(literal)

      override def toBindingData(value: T): BindingData = toData(value)

      override def toString: String = strRep
    }
}
