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

import org.flyte.api.v1.{
  BindingData,
  LiteralType,
  Primitive,
  Scalar,
  SimpleType
}
import org.flyte.flytekit.{SdkBindingData => SdkJavaBindinigData}

import java.time.{Duration, Instant}
import scala.collection.JavaConverters._

object SdkBindingData {
  def ofString(string: String): SdkJavaBindinigData[String] =
    createSdkBindingData(string)

  def ofInteger(long: Long): SdkJavaBindinigData[Long] =
    createSdkBindingData(long)

  def ofFloat(double: Double): SdkJavaBindinigData[Double] =
    createSdkBindingData(double)

  def ofBoolean(
                 boolean: Boolean
               ): SdkJavaBindinigData[Boolean] =
    createSdkBindingData(boolean)

  def ofDateTime(instant: Instant): SdkJavaBindinigData[Instant] =
    createSdkBindingData(instant)

  def ofDuration(
                  duration: Duration
                ): SdkJavaBindinigData[Duration] = createSdkBindingData(duration)

  def ofCollection[T](
                       collection: List[T]
                     ): SdkJavaBindinigData[List[T]] = createSdkBindingData(collection)

  def ofCollection[T](
                       literalType: LiteralType,
                       collection: List[T]
                     ): SdkJavaBindinigData[List[T]] =
    createSdkBindingData(collection, Option(literalType))

  def ofStringCollection(
                          collection: List[String]
                        ): SdkJavaBindinigData[List[String]] =
    createSdkBindingData(
      collection,
      Option(
        LiteralType.ofCollectionType(
          LiteralType.ofSimpleType(SimpleType.STRING)
        )
      )
    )

  def ofIntegerCollection(
                           collection: List[Long]
                         ): SdkJavaBindinigData[List[Long]] =
    createSdkBindingData(
      collection,
      Option(
        LiteralType.ofCollectionType(
          LiteralType.ofSimpleType(SimpleType.INTEGER)
        )
      )
    )

  def ofBooleanCollection(
                           collection: List[Boolean]
                         ): SdkJavaBindinigData[List[Boolean]] =
    createSdkBindingData(
      collection,
      Option(
        LiteralType.ofCollectionType(
          LiteralType.ofSimpleType(SimpleType.BOOLEAN)
        )
      )
    )

  def ofFloatCollection(
                         collection: List[Double]
                       ): SdkJavaBindinigData[List[Double]] =
    createSdkBindingData(
      collection,
      Option(
        LiteralType.ofCollectionType(LiteralType.ofSimpleType(SimpleType.FLOAT))
      )
    )

  def ofInstantCollection(
                           collection: List[Instant]
                         ): SdkJavaBindinigData[List[Instant]] =
    createSdkBindingData(
      collection,
      Option(
        LiteralType.ofCollectionType(
          LiteralType.ofSimpleType(SimpleType.DATETIME)
        )
      )
    )

  def ofDurationCollection(
                            collection: List[Duration]
                          ): SdkJavaBindinigData[List[Duration]] =
    createSdkBindingData(
      collection,
      Option(
        LiteralType.ofCollectionType(
          LiteralType.ofSimpleType(SimpleType.DURATION)
        )
      )
    )

  def ofMap[T](
                map: Map[String, T]
              ): SdkJavaBindinigData[Map[String, T]] = createSdkBindingData(map)

  def ofStringMap(
                   map: Map[String, String]
                 ): SdkJavaBindinigData[Map[String, String]] =
    createSdkBindingData(
      map,
      Option(
        LiteralType.ofMapValueType(LiteralType.ofSimpleType(SimpleType.STRING))
      )
    )

  def ofIntegerMap(
                    map: Map[String, Long]
                  ): SdkJavaBindinigData[Map[String, Long]] =
    createSdkBindingData(
      map,
      Option(
        LiteralType.ofMapValueType(LiteralType.ofSimpleType(SimpleType.INTEGER))
      )
    )

  def ofBooleanMap(
                    map: Map[String, Boolean]
                  ): SdkJavaBindinigData[Map[String, Boolean]] =
    createSdkBindingData(
      map,
      Option(
        LiteralType.ofMapValueType(LiteralType.ofSimpleType(SimpleType.BOOLEAN))
      )
    )

  def ofFloatMap(
                  map: Map[String, Double]
                ): SdkJavaBindinigData[Map[String, Double]] =
    createSdkBindingData(
      map,
      Option(
        LiteralType.ofMapValueType(LiteralType.ofSimpleType(SimpleType.FLOAT))
      )
    )

  def ofInstantMap(
                    map: Map[String, Instant]
                  ): SdkJavaBindinigData[Map[String, Instant]] =
    createSdkBindingData(
      map,
      Option(
        LiteralType.ofMapValueType(
          LiteralType.ofSimpleType(SimpleType.DATETIME)
        )
      )
    )

  def ofDurationMap(
                     map: Map[String, Duration]
                   ): SdkJavaBindinigData[Map[String, Duration]] =
    createSdkBindingData(
      map,
      Option(
        LiteralType.ofMapValueType(
          LiteralType.ofSimpleType(SimpleType.DURATION)
        )
      )
    )

  def ofMap[T](
                literalType: LiteralType,
                map: Map[String, T]
              ): SdkJavaBindinigData[Map[String, T]] =
    createSdkBindingData(map, Option(literalType))

  private def toBindingData(
                             value: Any,
                             literalTypeOpt: Option[LiteralType]
                           ): (BindingData, LiteralType) = {
    value match {
      case string: String =>
        (
          BindingData.ofScalar(
            Scalar.ofPrimitive(Primitive.ofStringValue(string))
          ),
          LiteralType.ofSimpleType(SimpleType.STRING)
        )
      case boolean: Boolean =>
        (
          BindingData.ofScalar(
            Scalar.ofPrimitive(Primitive.ofBooleanValue(boolean))
          ),
          LiteralType.ofSimpleType(SimpleType.BOOLEAN)
        )
      case long: Long =>
        (
          BindingData.ofScalar(
            Scalar.ofPrimitive(Primitive.ofIntegerValue(long))
          ),
          LiteralType.ofSimpleType(SimpleType.INTEGER)
        )
      case double: Double =>
        (
          BindingData.ofScalar(
            Scalar.ofPrimitive(Primitive.ofFloatValue(double))
          ),
          LiteralType.ofSimpleType(SimpleType.FLOAT)
        )
      case instant: Instant =>
        (
          BindingData.ofScalar(
            Scalar.ofPrimitive(Primitive.ofDatetime(instant))
          ),
          LiteralType.ofSimpleType(SimpleType.DATETIME)
        )
      case duration: Duration =>
        (
          BindingData.ofScalar(
            Scalar.ofPrimitive(Primitive.ofDuration(duration))
          ),
          LiteralType.ofSimpleType(SimpleType.DURATION)
        )
      case list: Seq[_] =>
        val literalType = literalTypeOpt.getOrElse {
          val (_, innerLiteralType) = toBindingData(
            list.headOption.getOrElse(
              throw new RuntimeException(
                "Can't create binding for an empty list without knowing the type, use SdkBindingData.of<type>Collection(...)"
              )
            ),
            literalTypeOpt = None
          )

          LiteralType.ofCollectionType(innerLiteralType)
        }

        (
          BindingData.ofCollection(
            list
              .map { innerValue =>
                val (bindingData, _) = toBindingData(innerValue, literalTypeOpt)
                bindingData
              }
              .toList
              .asJava
          ),
          literalType
        )
      case map: Map[String, _] =>
        val literalType = literalTypeOpt.getOrElse {
          val (_, innerLiteralType) = toBindingData(
            map.headOption
              .map(_._2)
              .getOrElse(
                throw new RuntimeException(
                  "Can't create binding for an empty map without knowing the type, use SdkBindingData.of<type>Map(...)"
                )
              ),
            literalTypeOpt = None
          )

          LiteralType.ofMapValueType(innerLiteralType)
        }
        (
          BindingData.ofMap(
            map
              .mapValues { innerValue =>
                val (bindingData, _) = toBindingData(innerValue, literalTypeOpt)
                bindingData
              }
              .toMap
              .asJava
          ),
          literalType
        )
      case other =>
        throw new IllegalStateException(
          s"${other.getClass.getSimpleName} class is not supported as SdkBindingData inner class"
        )
    }
  }

  private def createSdkBindingData[T](
                                       value: T,
                                       literalTypeOpt: Option[LiteralType] = None
                                     ): SdkJavaBindinigData[T] = {
    val (bindingData, literalType) = toBindingData(value, literalTypeOpt)
    SdkJavaBindinigData.create(bindingData, literalType, value)
  }
}