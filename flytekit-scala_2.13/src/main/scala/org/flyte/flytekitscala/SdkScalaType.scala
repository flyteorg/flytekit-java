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

import java.time.{Duration, Instant}
import java.{util => ju}
import magnolia.{CaseClass, Magnolia, Param, SealedTrait}
import org.flyte.api.v1._
import org.flyte.flytekit.{
  SdkBindingData,
  SdkLiteralType,
  SdkType,
  SdkLiteralTypes => SdkJavaLiteralTypes
}

import scala.annotation.implicitNotFound
import scala.collection.JavaConverters._

/** Type class to map between Flyte `Variable` and `Literal` and Scala case
  * classes.
  */
sealed trait SdkScalaType[T]

// products (e.g. case classes and tuples) can map into literal map
// while primitive types (e.g. integer) can't because they don't have
// fields, therefore, we separate them.
//
// combine always returns SdkType for product, but not SdkScalaLiteralType

trait SdkScalaProductType[T] extends SdkType[T] with SdkScalaType[T]

trait SdkScalaLiteralType[T] extends SdkLiteralType[T] with SdkScalaType[T]

case class DelegateLiteralType[T](delegate: SdkLiteralType[T])
    extends SdkScalaLiteralType[T] {
  override def getLiteralType: LiteralType = delegate.getLiteralType

  override def toLiteral(value: T): Literal = delegate.toLiteral(value)

  override def fromLiteral(literal: Literal): T = delegate.fromLiteral(literal)

  override def toBindingData(value: T): BindingData =
    delegate.toBindingData(value)

  override def toString: String = delegate.toString
}

/** Applied to a case classes fields to denote the description of such field
  * when it is used on [[SdkScalaType]].
  *
  * @param value
  *   the denoted description, must be non null.
  */
case class Description(value: String)
    extends scala.annotation.StaticAnnotation {
  require(value != null, "Description should not be null")
}

/** The [[SdkScalaType]] allows you to create a [[SdkType]] using a case class.
  * Example:
  *
  * case class MyCaseClass(name: SdkBindingData[String])
  *
  * val sdkType = SdkScalaType[MyCaseClass]
  */
object SdkScalaType {
  type Typeclass[T] = SdkScalaType[T]

  def combine[T](ctx: CaseClass[SdkScalaType, T]): SdkScalaProductType[T] = {
    // throwing an exception will abort implicit resolution for this case
    // very dirty down casting, but we need some evidence that all parameters are TypedLiterals
    // and that's the best we can do with magnolia unless we want to play with phantom types
    case class ParamsWithDesc(
        param: Param[SdkScalaLiteralType, T],
        desc: Option[String]
    )
    val params = ctx.parameters.map { param =>
      param.typeclass match {
        case _: SdkScalaProductType[_] =>
          sys.error("nested structs aren't supported")
        case _: SdkScalaLiteralType[_] =>
          val optDesc = param.annotations.collectFirst {
            case ann: Description => ann.value
          }
          ParamsWithDesc(
            param.asInstanceOf[Param[SdkScalaLiteralType, T]],
            optDesc
          )
      }
    }

    new SdkScalaProductType[T] {
      def getVariableMap: ju.Map[String, Variable] = {
        val scalaMap = params.map { case ParamsWithDesc(param, desc) =>
          val variable =
            Variable
              .builder()
              .literalType(param.typeclass.getLiteralType)
              .description(desc.getOrElse(""))
              .build()

          param.label -> variable
        }.toMap

        ju.Map.copyOf(mapAsJavaMap(scalaMap))
      }

      def toLiteralMap(value: T): ju.Map[String, Literal] = {
        val scalaMap = params.map { case ParamsWithDesc(param, _) =>
          param.label -> param.typeclass.toLiteral(param.dereference(value))
        }.toMap

        ju.Map.copyOf(mapAsJavaMap(scalaMap))
      }

      def fromLiteralMap(literal: ju.Map[String, Literal]): T = {
        ctx.rawConstruct(params.map { case ParamsWithDesc(param, _) =>
          val paramLiteral = literal.get(param.label)

          require(
            paramLiteral != null,
            s"field ${param.label} not found in literal map"
          )

          param.typeclass.fromLiteral(paramLiteral)
        })
      }

      def promiseFor(nodeId: String): T = {
        ctx.rawConstruct(params.map { case ParamsWithDesc(param, _) =>
          val paramLiteralType = getVariableMap.get(param.label)

          require(
            paramLiteralType != null,
            s"field ${param.label} not found in variable map"
          )

          SdkBindingData.promise(
            param.typeclass,
            nodeId,
            param.label
          )
        })
      }

      override def toSdkBindingMap(
          value: T
      ): ju.Map[String, SdkBindingData[_]] = {
        value match {
          case product: Product =>
            value.getClass.getDeclaredFields
              .map(_.getName)
              .zip(product.productIterator.toSeq)
              .toMap
              .mapValues {
                case value: SdkBindingData[_] => value
                case _ =>
                  throw new IllegalStateException(
                    s"All the fields of the case class ${value.getClass.getSimpleName} must be SdkBindingData[_]"
                  )
              }
              .toMap
              .asJava
          case _ =>
            throw new IllegalStateException(
              s"The class ${value.getClass.getSimpleName} must be a case class"
            )
        }
      }

      override def toLiteralTypes: ju.Map[String, SdkLiteralType[_]] = {
        params
          .map { case ParamsWithDesc(param, _) =>
            val value: SdkLiteralType[_] = param.typeclass
            param.label -> value
          }
          .toMap[String, SdkLiteralType[_]]
          .asJava
      }
    }
  }

  implicit def sdkBindingLiteralType[T](implicit
      sdkLiteral: SdkScalaLiteralType[T]
  ): SdkScalaLiteralType[SdkBindingData[T]] = {

    new SdkScalaLiteralType[SdkBindingData[T]]() {
      override def getLiteralType: LiteralType = sdkLiteral.getLiteralType

      override def toLiteral(value: SdkBindingData[T]): Literal =
        sdkLiteral.toLiteral(value.get())

      override def fromLiteral(literal: Literal): SdkBindingData[T] =
        SdkBindingData.literal(sdkLiteral, sdkLiteral.fromLiteral(literal))

      override def toBindingData(value: SdkBindingData[T]): BindingData =
        sdkLiteral.toBindingData(value.get())
    }
  }

  implicit def stringLiteralType: SdkScalaLiteralType[String] =
    DelegateLiteralType(SdkLiteralTypes.strings())

  implicit def longLiteralType: SdkScalaLiteralType[Long] =
    DelegateLiteralType(SdkLiteralTypes.integers())

  implicit def doubleLiteralType: SdkScalaLiteralType[Double] =
    DelegateLiteralType(SdkLiteralTypes.floats())

  implicit def booleanLiteralType: SdkScalaLiteralType[Boolean] =
    DelegateLiteralType(SdkLiteralTypes.booleans())

  implicit def instantLiteralType: SdkScalaLiteralType[Instant] =
    DelegateLiteralType(SdkLiteralTypes.datetimes())

  implicit def durationLiteralType: SdkScalaLiteralType[Duration] =
    DelegateLiteralType(SdkLiteralTypes.durations())

  // TODO we are forced to do this because SdkDataBinding.ofInteger returns a SdkBindingData<java.util.Long>
  //  This makes Scala dev mad when they are forced to use the java types instead of scala types
  //  We need to think what to do, maybe move the factory methods out of SdkDataBinding into their own class
  //  So java and scala can have their own factory class/companion object using their own native types
  //  In the meantime, we need to duplicate all the literal types to use also the java types
  implicit def javaLongLiteralType: SdkScalaLiteralType[java.lang.Long] =
    DelegateLiteralType(SdkJavaLiteralTypes.integers())

  implicit def javaDoubleLiteralType: SdkScalaLiteralType[java.lang.Double] =
    DelegateLiteralType(SdkJavaLiteralTypes.floats())

  implicit def javaBooleanLiteralType: SdkScalaLiteralType[java.lang.Boolean] =
    DelegateLiteralType(SdkJavaLiteralTypes.booleans())

  implicit def collectionLiteralType[T](implicit
      sdkLiteral: SdkScalaLiteralType[T]
  ): SdkScalaLiteralType[List[T]] =
    DelegateLiteralType(SdkLiteralTypes.collections(sdkLiteral))

  implicit def mapLiteralType[T](implicit
      sdkLiteral: SdkScalaLiteralType[T]
  ): SdkScalaLiteralType[Map[String, T]] =
    DelegateLiteralType(SdkLiteralTypes.maps(sdkLiteral))

  @implicitNotFound("Cannot derive SdkScalaType for sealed trait")
  sealed trait Dispatchable[T]

  def dispatch[T: Dispatchable](
      sealedTrait: SealedTrait[SdkScalaProductType, T]
  ): SdkScalaProductType[T] =
    sys.error("Cannot derive SdkScalaType for sealed trait")

  def apply[T <: Product]: SdkScalaProductType[T] = macro Magnolia.gen[T]

  /** Returns a [[SdkType]] for [[Unit]] which contains no properties.
    *
    * @return
    *   the sdk type
    */
  def unit: SdkScalaProductType[Unit] = SdkUnitType

  /** Returns a [[SdkType]] with only one variable with the specified
    * [[SdkLiteralType]], name and description.
    *
    * @param literalType
    *   the type of the single variable of the returned type.
    * @param varName
    *   the name of the single variable of the returned type.
    * @param varDescription
    *   the description of the single variable of the returned type, defaults to
    *   empty String.
    * @return
    *   the SdkType with a single variable.
    * @tparam T
    *   the native type of the single variable type.
    */
  def apply[T](
      literalType: SdkLiteralType[T],
      varName: String,
      varDescription: String = ""
  ): SdkType[SdkBindingData[T]] = literalType.asSdkType(varName, varDescription)
}

private object SdkUnitType extends SdkScalaProductType[Unit] {
  def getVariableMap: ju.Map[String, Variable] =
    Map.empty[String, Variable].asJava

  def toLiteralMap(value: Unit): ju.Map[String, Literal] =
    Map.empty[String, Literal].asJava

  def fromLiteralMap(literal: ju.Map[String, Literal]): Unit = ()

  def promiseFor(nodeId: String): Unit = ()

  override def toSdkBindingMap(
      value: Unit
  ): ju.Map[String, SdkBindingData[_]] =
    Map.empty[String, SdkBindingData[_]].asJava

  override def toLiteralTypes: ju.Map[String, SdkLiteralType[_]] =
    Map.empty[String, SdkLiteralType[_]].asJava
}
