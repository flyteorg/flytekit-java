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
import org.flyte.flytekit.{SdkBindingData, SdkTransform, SdkWorkflowBuilder}

import java.time.{Duration, Instant}
import scala.collection.JavaConverters._

object Implicits {
  implicit class ScalaSdkTransform[T](transform: SdkTransform[T]) {
    def getOutputs()(implicit builder: SdkWorkflowBuilder): T = {
      builder.apply(transform).getOutputs
    }
  }

  implicit def stringSdkBinding(string: String): SdkBindingData[String] =
    createSdkBindingData(string)

  implicit def longSdkBinding(long: Long): SdkBindingData[Long] =
    createSdkBindingData(long)

  implicit def doubleSdkBinding(double: Double): SdkBindingData[Double] =
    createSdkBindingData(double)

  implicit def booleanSdkBindingData(
      boolean: Boolean
  ): SdkBindingData[Boolean] =
    createSdkBindingData(boolean)

  implicit def instantSdkBinding(instant: Instant): SdkBindingData[Instant] =
    createSdkBindingData(instant)

  implicit def durationSdkBinding(
      duration: Duration
  ): SdkBindingData[Duration] = createSdkBindingData(duration)

  implicit def collectionSdkBinding[T](
      collection: List[T]
  ): SdkBindingData[List[T]] = createSdkBindingData(collection)

  implicit def mapSdkBinding[T](
      map: Map[String, T]
  ): SdkBindingData[Map[String, T]] = createSdkBindingData(map)

  implicit def javaLongSdkBinding(
      long: Long
  ): SdkBindingData[java.lang.Long] =
    createSdkBindingData(long)

  implicit def javaDoubleSdkBinding(
      double: Double
  ): SdkBindingData[java.lang.Double] =
    createSdkBindingData(double)

  implicit def javaBooleanSdkBindingData(
      boolean: Boolean
  ): SdkBindingData[java.lang.Boolean] =
    createSdkBindingData(boolean)

  implicit def javaCollectionSdkBinding[T](
      collection: List[T]
  ): SdkBindingData[java.util.List[T]] = createSdkBindingData(collection.asJava)

  implicit def javaMapSdkBinding[T](
      map: Map[String, T]
  ): SdkBindingData[java.util.Map[String, T]] = createSdkBindingData(map.asJava)

  def inputOfInteger(name: String, help: String = "")(implicit
      builder: SdkWorkflowBuilder
  ): SdkBindingData[Long] =
    builder
      .inputOf[Long](name, LiteralType.ofSimpleType(SimpleType.INTEGER), help)

  def inputOfBoolean(name: String, help: String = "")(implicit
      builder: SdkWorkflowBuilder
  ): SdkBindingData[Boolean] =
    builder.inputOf[Boolean](
      name,
      LiteralType.ofSimpleType(SimpleType.BOOLEAN),
      help
    )

  def inputOfFloat(name: String, help: String = "")(implicit
      builder: SdkWorkflowBuilder
  ): SdkBindingData[Double] =
    builder
      .inputOf[Double](name, LiteralType.ofSimpleType(SimpleType.FLOAT), help)

  def inputOfDatetime(name: String, help: String = "")(implicit
      builder: SdkWorkflowBuilder
  ): SdkBindingData[Instant] =
    builder.inputOfDatetime(name, help)

  def inputOfDuration(name: String, help: String = "")(implicit
      builder: SdkWorkflowBuilder
  ): SdkBindingData[Duration] =
    builder.inputOfDuration(name, help)

  def inputOfString(name: String, help: String = "")(implicit
      builder: SdkWorkflowBuilder
  ): SdkBindingData[String] =
    builder.inputOfString(name, help)

  private def toBindingData(value: Any): (BindingData, LiteralType) = {
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
        val (_, innerLiteralType) = toBindingData(
          list.headOption.getOrElse(
            throw new RuntimeException(
              "Can't create binding for an empty list without knowing the type, use SdkBindingData.create(...)"
            )
          )
        )
        (
          BindingData.ofCollection(
            list.map(innerValue => toBindingData(innerValue)._1).toList.asJava
          ),
          LiteralType.ofCollectionType(innerLiteralType)
        )
      case map: Map[String, _] =>
        val (_, innerLiteralType) = toBindingData(
          map.headOption
            .map(_._2)
            .getOrElse(
              throw new RuntimeException(
                "Can't create binding for an empty map without knowing the type, use SdkBindingData.create(...)"
              )
            )
        )

        (
          BindingData.ofMap(
            map
              .mapValues(innerValue => toBindingData(innerValue)._1)
              .toMap
              .asJava
          ),
          LiteralType.ofMapValueType(innerLiteralType)
        )
      case other =>
        throw new IllegalStateException(
          s"${other.getClass.getSimpleName} class is not supported as SdkBindingData inner class"
        )
    }
  }

  private def createSdkBindingData[T](value: T): SdkBindingData[T] = {
    val (bindingData, literalType) = toBindingData(value)
    SdkBindingData.create(bindingData, literalType, value)
  }
}
