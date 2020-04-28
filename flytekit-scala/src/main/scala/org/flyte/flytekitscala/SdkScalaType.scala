/*
 * Copyright 2020 Spotify AB.
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

import java.{util => ju}

import magnolia.{CaseClass, Magnolia, Param, SealedTrait}
import org.flyte.api.v1._
import org.flyte.flytekit.SdkType

import scala.annotation.implicitNotFound
import scala.collection.JavaConverters

/** Type class to map between Flyte `Variable` and `Literal` and Scala case classes. */
sealed trait SdkScalaType[T]

// products (e.g. case classes and tuples) can map into literal map
// while primitive types (e.g. integer) can't because they don't have
// fields, therefore, we separate them.
//
// combine always returns SdkType for product, but not SdkScalaLiteralType

trait SdkScalaProductType[T] extends SdkType[T] with SdkScalaType[T]

trait SdkScalaLiteralType[T] extends SdkScalaType[T] {
  def getLiteralType: LiteralType

  def toLiteral(value: T): Literal

  def fromLiteral(literal: Literal): T
}

object SdkScalaLiteralType {
  def apply[T](
      literalType: LiteralType,
      to: T => Literal,
      from: Literal => T
  ): SdkScalaLiteralType[T] =
    new SdkScalaLiteralType[T] {
      override def getLiteralType: LiteralType = literalType

      override def toLiteral(value: T): Literal = to(value)

      override def fromLiteral(literal: Literal): T = from(literal)
    }
}

object SdkScalaType {
  type Typeclass[T] = SdkScalaType[T]

  private[flytekitscala] def combine[T](
      ctx: CaseClass[SdkScalaType, T]
  ): SdkScalaProductType[T] = {
    // throwing an exception will abort implicit resolution for this case
    // very dirty down casting, but we need some evidence that all parameters are TypedLiterals
    // and that's the best we can do with magnolia unless we want to play with phantom types
    val params = ctx.parameters.map { param =>
      param.typeclass match {
        case _: SdkScalaProductType[_] =>
          sys.error("nested structs aren't supported")
        case other: SdkScalaLiteralType[_] =>
          (param.asInstanceOf[Param[SdkScalaLiteralType, T]])
      }
    }

    new SdkScalaProductType[T] {
      def getVariableMap: ju.Map[String, Variable] = {
        val scalaMap = params.map { param =>
          val variable =
            Variable.create(
              param.typeclass.getLiteralType, /* description= */ ""
            )

          param.label -> variable
        }.toMap

        new ju.HashMap(JavaConverters.mapAsJavaMap(scalaMap))
      }

      def toLiteralMap(value: T): ju.Map[String, Literal] = {
        val scalaMap = params.map { param =>
          param.label -> param.typeclass.toLiteral(param.dereference(value))
        }.toMap

        new ju.HashMap(JavaConverters.mapAsJavaMap(scalaMap))
      }

      def fromLiteralMap(literal: ju.Map[String, Literal]): T = {
        ctx.rawConstruct(params.map { param =>
          val paramLiteral = literal.get(param.label)

          require(
            paramLiteral != null,
            s"field ${param.label} not found in literal map"
          )

          param.typeclass.fromLiteral(paramLiteral)
        })
      }
    }
  }

  implicit def stringLiteralType: SdkScalaLiteralType[String] =
    SdkScalaLiteralType[String](
      LiteralType.create(SimpleType.STRING),
      value => Literal.of(Scalar.create(Primitive.of(value))),
      _.scalar().primitive().string()
    )

  implicit def longLiteralType: SdkScalaLiteralType[Long] =
    SdkScalaLiteralType[Long](
      LiteralType.create(SimpleType.INTEGER),
      value => Literal.of(Scalar.create(Primitive.of(value))),
      _.scalar().primitive().integer()
    )

  implicit def doubleLiteralType: SdkScalaLiteralType[Double] =
    SdkScalaLiteralType[Double](
      LiteralType.create(SimpleType.FLOAT),
      value => Literal.of(Scalar.create(Primitive.of(value))),
      literal => literal.scalar().primitive().float_()
    )

  implicit def booleanLiteralType: SdkScalaLiteralType[Boolean] =
    SdkScalaLiteralType[Boolean](
      LiteralType.create(SimpleType.BOOLEAN),
      value => Literal.of(Scalar.create(Primitive.of(value))),
      _.scalar().primitive().boolean_()
    )

  @implicitNotFound("Cannot derive SdkScalaType for sealed trait")
  private sealed trait Dispatchable[T]

  private[flytekitscala] def dispatch[T: Dispatchable](
      sealedTrait: SealedTrait[SdkScalaProductType, T]
  ): SdkScalaProductType[T] =
    sys.error("Cannot derive SdkScalaType for sealed trait")

  def apply[T <: Product]: SdkScalaProductType[T] = macro Magnolia.gen[T]

  def unit: SdkScalaProductType[Unit] = SdkUnitType

}

private object SdkUnitType extends SdkScalaProductType[Unit] {
  def getVariableMap: ju.Map[String, Variable] = ju.Collections.emptyMap()

  def toLiteralMap(value: Unit): ju.Map[String, Literal] =
    ju.Collections.emptyMap()

  def fromLiteralMap(literal: ju.Map[String, Literal]): Unit = ()
}
