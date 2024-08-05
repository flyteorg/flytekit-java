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

import org.flyte.api.v1._
import org.flyte.flytekit.{
  SdkLiteralType,
  SdkLiteralTypes => SdkJavaLiteralTypes
}

import java.time.{Duration, Instant}
import scala.collection.JavaConverters._
import scala.reflect.api.{Mirror, TypeCreator, Universe}
import scala.reflect.runtime.universe
import scala.reflect.{ClassTag, classTag}
import scala.reflect.runtime.universe.{
  ClassSymbol,
  NoPrefix,
  Symbol,
  Type,
  TypeTag,
  runtimeMirror,
  termNames,
  typeOf,
  typeTag
}
import scala.tools.nsc.doc.model.Trait

object SdkLiteralTypes {
  val __TYPE = "__type"

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
      case t if t =:= typeOf[Blob] =>
        blobs(BlobType.DEFAULT).asInstanceOf[SdkLiteralType[T]]
      case t if t =:= typeOf[Binary] =>
        binary().asInstanceOf[SdkLiteralType[T]]
      case t if t <:< typeOf[Product] =>
        generics().asInstanceOf[SdkLiteralType[T]]

      case t if t <:< typeOf[List[Any]] =>
        collections(of()(createTypeTag(typeTag[T].mirror, t.typeArgs.head)))
          .asInstanceOf[SdkLiteralType[T]]
      case t if t <:< typeOf[Map[String, Any]] =>
        maps(of()(createTypeTag(typeTag[T].mirror, t.typeArgs.last)))
          .asInstanceOf[SdkLiteralType[T]]

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

  /** Returns a [[SdkLiteralType]] for products.
    * @return
    *   the [[SdkLiteralType]]
    */
  def generics[T <: Product: TypeTag: ClassTag](): SdkLiteralType[T] = {
    ScalaLiteralType[T](
      LiteralType.ofSimpleType(SimpleType.STRUCT),
      (value: T) => Literal.ofScalar(Scalar.ofGeneric(toStruct(value))),
      (x: Literal) => toProduct(x.scalar().generic()),
      (v: T) => BindingData.ofScalar(Scalar.ofGeneric(toStruct(v))),
      "generics"
    )
  }

  private def toStruct(product: Product): Struct = {
    def productToMap(product: Product): Map[String, Any] = {
      productElementNames(product)
        .zip(product.productIterator.toList)
        .toMap ++
        // Add the type of the product to the map, this could be used later to convert
        // the struct back to the product if the type can not be inferred from the scala type.
        // For example: when the product is a trait or an abstract class.
        Map(__TYPE -> product.getClass.getTypeName)
    }

    def mapToStruct(map: Map[String, Any]): Struct = {
      val fields = map.map({ case (key, value) =>
        (key, anyToStructValue(value))
      })
      Struct.of(fields.asJava)
    }

    def anyToStructValue(value: Any): Struct.Value = {
      def anyToStructureValue0(value: Any): Struct.Value = {
        value match {
          case s: String => Struct.Value.ofStringValue(s)
          case n @ (_: Byte | _: Short | _: Int | _: Long | _: Float |
              _: Double) =>
            Struct.Value.ofNumberValue(n.toString.toDouble)
          case b: Boolean => Struct.Value.ofBoolValue(b)
          case l: List[Any] =>
            Struct.Value.ofListValue(l.map(anyToStructValue).asJava)
          case m: Map[_, _] =>
            Struct.Value.ofStructValue(
              mapToStruct(m.asInstanceOf[Map[String, Any]])
            )
          case null => Struct.Value.ofNullValue()
          case p: Product =>
            Struct.Value.ofStructValue(mapToStruct(productToMap(p)))
          case _ =>
            throw new IllegalArgumentException(
              s"Unsupported type: ${value.getClass}"
            )
        }
      }

      value match {
        case Some(v) => anyToStructureValue0(v)
        case None    => Struct.Value.ofNullValue()
        case _       => anyToStructureValue0(value)
      }
    }

    mapToStruct(productToMap(product))
  }

  private def toProduct[T <: Product: TypeTag: ClassTag](
      struct: Struct
  ): T = {
    def structToMap(struct: Struct): Map[String, Any] = {
      struct
        .fields()
        .asScala
        .map({ case (key, value) =>
          (key, structValueToAny(value))
        })
        .toMap
    }

    def mapToProduct[S <: Product: TypeTag: ClassTag](
        map: Map[String, Any]
    ): S = {
      val mirror = runtimeMirror(classTag[S].runtimeClass.getClassLoader)

      def valueToParamValue(value: Any, tpe: Type): Any = {
        if (tpe =:= typeOf[Byte]) {
          value.asInstanceOf[Double].toByte
        } else if (tpe =:= typeOf[Short]) {
          value.asInstanceOf[Double].toShort
        } else if (tpe =:= typeOf[Int]) {
          value.asInstanceOf[Double].toInt
        } else if (tpe =:= typeOf[Long]) {
          value.asInstanceOf[Double].toLong
        } else if (tpe =:= typeOf[Float]) {
          value.asInstanceOf[Double].toFloat
        } else if (tpe <:< typeOf[List[Any]]) {
          value
            .asInstanceOf[List[Any]]
            .map(v => {
              valueToParamValue(v, tpe.typeArgs.head)
            })
        } else if (tpe <:< typeOf[Map[String, Any]]) {
          value
            .asInstanceOf[Map[String, Any]]
            .mapValues(v => {
              valueToParamValue(v, tpe.typeArgs(1))
            })
            .toMap
        } else if (tpe <:< typeOf[Option[Any]]) { // this has to be before Product check because Option is a Product
          if (value == None) { // None is used to represent Struct.Value.Kind.NULL_VALUE when converting struct to map
            None
          } else {
            Some(
              valueToParamValue(
                value,
                tpe.dealias.typeArgs.head
              )
            )
          }
        } else if (tpe <:< typeOf[Product]) {
          val typeTag = createTypeTag(mirror, tpe)
          val classTag = ClassTag(
            typeTag.mirror.runtimeClass(tpe)
          )
          mapToProduct(value.asInstanceOf[Map[String, Any]])(
            typeTag,
            classTag
          )
        } else {
          value match {
            // This is a special case where the type of the product can not be inferred from the scala type.
            // This can happen when the product is a trait or an abstract class.
            // In this case, we use the __TYPE field to get the type of the product.
            case map: Map[String, Any] if map.contains(__TYPE) =>
              val typeTag = createTypeTag(
                mirror,
                mirror
                  .staticClass(map(__TYPE).asInstanceOf[String])
                  .typeSignature
              )
              val classTag = ClassTag(
                typeTag.mirror.runtimeClass(typeTag.tpe)
              )
              mapToProduct(map)(typeTag, classTag)
            case _ => value
          }
        }
      }

      val clazz = typeOf[S].typeSymbol.asClass

      def instantiateViaConstructor(cls: ClassSymbol): S = {
        val classMirror = mirror.reflectClass(cls)
        val constructor = typeOf[S].decl(termNames.CONSTRUCTOR).asMethod
        val constructorMirror = classMirror.reflectConstructor(constructor)

        val constructorArgs =
          constructor.paramLists.flatten.map((param: Symbol) => {
            val paramName = param.name.toString
            val value = map.getOrElse(
              paramName,
              throw new IllegalArgumentException(
                s"Map is missing required parameter named $paramName"
              )
            )
            valueToParamValue(value, param.typeSignature.dealias)
          })

        constructorMirror(constructorArgs: _*).asInstanceOf[S]
      }

      // special handling of scala.Option as it is a Product, but can't be instantiated like common
      // case classes
      if (clazz.name.toString == "Option")
        map
          .get("value")
          .map(valueToParamValue(_, typeOf[S].typeArgs.head))
          .asInstanceOf[S]
      else
        instantiateViaConstructor(clazz)

    }

    def structValueToAny(value: Struct.Value): Any = {
      value.kind() match {
        case Struct.Value.Kind.STRING_VALUE => value.stringValue()
        case Struct.Value.Kind.NUMBER_VALUE => value.numberValue()
        case Struct.Value.Kind.BOOL_VALUE   => value.boolValue()
        case Struct.Value.Kind.LIST_VALUE =>
          value.listValue().asScala.map(structValueToAny).toList
        case Struct.Value.Kind.STRUCT_VALUE => structToMap(value.structValue())
        case Struct.Value.Kind.NULL_VALUE   => None
      }
    }

    mapToProduct[T](structToMap(struct))
  }

  /** Returns a [[SdkLiteralType]] for binary.
    *
    * @return
    *   the [[SdkLiteralType]]
    */
  def binary(): SdkLiteralType[Binary] =
    SdkJavaLiteralTypes.binary()

  /** Returns a [[SdkLiteralType]] for blob.
    *
    * @return
    *   the [[SdkLiteralType]]
    */
  def blobs(blobType: BlobType): SdkLiteralType[Blob] =
    SdkJavaLiteralTypes.blobs(blobType)

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

  private def createTypeTag[U](
      mirror: universe.Mirror,
      tpe: Type
  ): TypeTag[U] = {
    val typSym = mirror.staticClass(tpe.typeSymbol.fullName)
    // note: this uses internal API, otherwise we will need to depend on scala-compiler at runtime
    val typeRef =
      universe.internal.typeRef(NoPrefix, typSym, tpe.typeArgs)

    TypeTag(
      mirror,
      new TypeCreator {
        override def apply[V <: Universe with Singleton](
            m: Mirror[V]
        ): V#Type = {
          assert(
            m == mirror,
            s"TypeTag[$typeRef] defined in $mirror cannot be migrated to $m."
          )
          typeRef.asInstanceOf[V#Type]
        }
      }
    )
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
