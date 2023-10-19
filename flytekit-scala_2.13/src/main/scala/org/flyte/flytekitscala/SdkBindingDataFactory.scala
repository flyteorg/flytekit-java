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

import org.flyte.api.v1.Blob
import org.flyte.flytekit.{
  BindingCollection,
  BindingMap,
  SdkBindingData,
  SdkLiteralType
}
import org.flyte.flytekitscala.SdkLiteralTypes._

import java.time.{Duration, Instant}

/** Utility to create [[SdkBindingData]] using scala raw types.
  */
object SdkBindingDataFactory {

  /** Creates a [[SdkBindingData]] for a flyte string ([[String]] for scala)
    * with the given value.
    *
    * @param string
    *   the simple value for this data
    * @return
    *   the new {[[SdkBindingData]]
    */
  def of(string: String): SdkBindingData[String] =
    SdkBindingData.literal(strings(), string)

  /** Creates a [[SdkBindingData]] for a flyte integer ([[Long]] for scala) with
    * the given value.
    *
    * @param long
    *   the simple value for this data
    * @return
    *   the new {[[SdkBindingData]]
    */
  def of(long: Long): SdkBindingData[Long] =
    SdkBindingData.literal(integers(), long)

  /** Creates a [[SdkBindingData]] for a flyte float ([[Double]] for scala) with
    * the given value.
    *
    * @param double
    *   the simple value for this data
    * @return
    *   the new {[[SdkBindingData]]
    */
  def of(double: Double): SdkBindingData[Double] =
    SdkBindingData.literal(floats(), double)

  /** Creates a [[SdkBindingData]] for a flyte boolean ([[Boolean]] for scala)
    * with the given value.
    *
    * @param boolean
    *   the simple value for this data
    * @return
    *   the new {[[SdkBindingData]]
    */
  def of(
      boolean: Boolean
  ): SdkBindingData[Boolean] =
    SdkBindingData.literal(booleans(), boolean)

  /** Creates a [[SdkBindingData]] for a flyte instant ([[Instant]] for scala)
    * with the given value.
    *
    * @param instant
    *   the simple value for this data
    * @return
    *   the new {[[SdkBindingData]]
    */
  def of(instant: Instant): SdkBindingData[Instant] =
    SdkBindingData.literal(datetimes(), instant)

  /** Creates a [[SdkBindingData]] for a flyte duration ([[Duration]] for scala)
    * with the given value.
    *
    * @param duration
    *   the simple value for this data
    * @return
    *   the new {[[SdkBindingData]]
    */
  def of(
      duration: Duration
  ): SdkBindingData[Duration] = SdkBindingData.literal(durations(), duration)

  /** Creates a [[SdkBindingData]] for a flyte collection given a scala
    * [[List]].
    *
    * @param collection
    *   collection to represent on this data.
    * @return
    *   the new [[SdkBindingData]]
    */
  def of[T](
      collection: List[T]
  ): SdkBindingData[List[T]] =
    SdkBindingData.literal(
      toSdkLiteralType(collection).asInstanceOf[SdkLiteralType[List[T]]],
      collection
    )

  /** Creates a [[SdkBindingData]] for a flyte collection given a scala
    * [[List]].
    *
    * @param elementLiteralType
    *   [[SdkLiteralType]] for elements of collection.
    * @param collection
    *   collection to represent on this data.
    * @return
    *   the new [[SdkBindingData]]
    */
  def of[T](
      elementLiteralType: SdkLiteralType[T],
      collection: List[T]
  ): SdkBindingData[List[T]] =
    SdkBindingData.literal(
      collections(elementLiteralType),
      collection
    )

  /** Creates a [[SdkBindingData]] for a flyte Blob with the given value.
    *
    * @param value
    *   the simple value for this data
    * @return
    *   the new [[SdkBindingData]]
    */
  def of(value: Blob): SdkBindingData[Blob] =
    SdkBindingData.literal(SdkLiteralTypes.blobs(value.metadata.`type`), value)

  /** Creates a [[SdkBindingData]] for a flyte type with the given value.
    *
    * @param type
    *   the flyte type
    * @param value
    *   the simple value for this data
    * @return
    *   the new [[SdkBindingData]]
    */
  def of[T](`type`: SdkLiteralType[T], value: T): SdkBindingData[T] =
    SdkBindingData.literal(`type`, value)

  /** Creates a [[SdkBindingDataFactory]] for a flyte string collection given a
    * scala [[List]].
    *
    * @param collection
    *   collection to represent on this data.
    * @return
    *   the new [[SdkBindingData]]
    */
  def ofStringCollection(
      collection: List[String]
  ): SdkBindingData[List[String]] =
    SdkBindingData.literal(collections(strings()), collection)

  /** Creates a [[SdkBindingData]] for a flyte integer collection given a scala
    * [[List]].
    *
    * @param collection
    *   collection to represent on this data.
    * @return
    *   the new [[SdkBindingDataFactory]]
    */
  def ofIntegerCollection(
      collection: List[Long]
  ): SdkBindingData[List[Long]] =
    SdkBindingData.literal(collections(integers()), collection)

  /** Creates a [[SdkBindingData]] for a flyte boolean collection given a scala
    * [[List]].
    *
    * @param collection
    *   collection to represent on this data.
    * @return
    *   the new [[SdkBindingData]]
    */
  def ofBooleanCollection(
      collection: List[Boolean]
  ): SdkBindingData[List[Boolean]] =
    SdkBindingData.literal(collections(booleans()), collection)

  /** Creates a [[SdkBindingData]] for a flyte float collection given a scala
    * [[List]].
    *
    * @param collection
    *   collection to represent on this data.
    * @return
    *   the new [[SdkBindingData]]
    */
  def ofFloatCollection(
      collection: List[Double]
  ): SdkBindingData[List[Double]] =
    SdkBindingData.literal(collections(floats()), collection)

  /** Creates a [[SdkBindingData]] for a flyte datetime collection given a scala
    * [[List]].
    *
    * @param collection
    *   collection to represent on this data.
    * @return
    *   the new [[SdkBindingData]]
    */
  def ofDatetimeCollection(
      collection: List[Instant]
  ): SdkBindingData[List[Instant]] =
    SdkBindingData.literal(collections(datetimes()), collection)

  /** Creates a [[SdkBindingData]] for a flyte duration collection given a scala
    * [[List]].
    *
    * @param collection
    *   collection to represent on this data.
    * @return
    *   the new [[SdkBindingData]]
    */
  def ofDurationCollection(
      collection: List[Duration]
  ): SdkBindingData[List[Duration]] =
    SdkBindingData.literal(collections(durations()), collection)

  /** Creates a [[SdkBindingData]] for a flyte map given a scala [[Map]].
    *
    * @param map
    *   collection to represent on this data.
    * @return
    *   the new [[SdkBindingData]]
    */
  def of[T](map: Map[String, T]): SdkBindingData[Map[String, T]] =
    SdkBindingData.literal(
      toSdkLiteralType(map).asInstanceOf[SdkLiteralType[Map[String, T]]],
      map
    )

  /** Creates a [[SdkBindingData]] for a flyte string map given a scala [[Map]].
    *
    * @param map
    *   collection to represent on this data.
    * @return
    *   the new [[SdkBindingDataFactory]]
    */
  def ofStringMap(
      map: Map[String, String]
  ): SdkBindingData[Map[String, String]] =
    SdkBindingData.literal(maps(strings()), map)

  /** Creates a [[SdkBindingData]] for a flyte long map given a scala [[Map]].
    *
    * @param map
    *   collection to represent on this data.
    * @return
    *   the new [[SdkBindingData]]
    */
  def ofIntegerMap(map: Map[String, Long]): SdkBindingData[Map[String, Long]] =
    SdkBindingData.literal(maps(integers()), map)

  /** Creates a [[SdkBindingData]] for a flyte boolean map given a scala
    * [[Map]].
    *
    * @param map
    *   collection to represent on this data.
    * @return
    *   the new [[SdkBindingData]]
    */
  def ofBooleanMap(
      map: Map[String, Boolean]
  ): SdkBindingData[Map[String, Boolean]] =
    SdkBindingData.literal(maps(booleans()), map)

  /** Creates a [[SdkBindingData]] for a flyte double map given a scala [[Map]].
    *
    * @param map
    *   collection to represent on this data.
    * @return
    *   the new [[SdkBindingData]]
    */
  def ofFloatMap(
      map: Map[String, Double]
  ): SdkBindingData[Map[String, Double]] =
    SdkBindingData.literal(maps(floats()), map)

  /** Creates a [[SdkBindingData]] for a flyte instant map given a scala
    * [[Map]].
    *
    * @param map
    *   collection to represent on this data.
    * @return
    *   the new [[SdkBindingData]]
    */
  def ofDatetimeMap(
      map: Map[String, Instant]
  ): SdkBindingData[Map[String, Instant]] =
    SdkBindingData.literal(maps(datetimes()), map)

  /** Creates a [[SdkBindingData]] for a flyte duration map given a scala
    * [[Map]].
    *
    * @param map
    *   collection to represent on this data.
    * @return
    *   the new [[SdkBindingData]]
    */
  def ofDurationMap(
      map: Map[String, Duration]
  ): SdkBindingData[Map[String, Duration]] =
    SdkBindingData.literal(maps(durations()), map)

  /** Creates a [[SdkBindingData]] for a flyte duration map given a scala
    * [[Map]].
    *
    * @param valuesLiteralType
    *   [[SdkLiteralType]] type for the values of the map.
    * @param map
    *   collection to represent on this data.
    * @return
    *   the new [[SdkBindingData]]
    */
  def of[T](
      valuesLiteralType: SdkLiteralType[T],
      map: Map[String, T]
  ): SdkBindingData[Map[String, T]] =
    SdkBindingData.literal(maps(valuesLiteralType), map)

  /** Creates a [[SdkBindingData]] for a flyte collection given a scala
    * {{{List[SdkBindingData[T]]}}} and [[SdkLiteralType]] for types for the
    * elements.
    *
    * @param elementType
    *   a [[SdkLiteralType]] expressing the types for the elements in the
    *   collection.
    * @param elements
    *   collection to represent on this data.
    * @return
    *   the new [[SdkBindingData]]
    */
  def ofBindingCollection[T](
      elementType: SdkLiteralType[T],
      elements: List[SdkBindingData[T]]
  ): SdkBindingData[List[T]] = {
    BindingCollection(elementType, elements)
  }

  /** Creates a [[SdkBindingData]] for a flyte map given a java
    * {{{SdkBindingData[Map[String, T]]}}} and a [[SdkLiteralType]] for the
    * values of the map.
    *
    * @param valuesType
    *   a [[SdkLiteralType]] expressing the types for the values of the map. The
    *   keys are always String.
    * @param valueMap
    *   map to represent on this data.
    * @return
    *   the new [[SdkBindingData]]
    */
  def ofBindingMap[T](
      valuesType: SdkLiteralType[T],
      valueMap: Map[String, SdkBindingData[T]]
  ): SdkBindingData[Map[String, T]] =
    BindingMap(valuesType, valueMap)

  private def toSdkLiteralType(
      value: Any,
      internalTypeOpt: Option[SdkLiteralType[_]] = Option.empty
  ): SdkLiteralType[_] = {
    value match {
      case string: String =>
        strings()
      case boolean: Boolean =>
        booleans()
      case long: Long =>
        integers()

      case double: Double =>
        floats()

      case instant: Instant =>
        datetimes()

      case duration: Duration =>
        durations()

      case list: Seq[_] =>
        val internalType = internalTypeOpt.getOrElse {
          toSdkLiteralType(
            list.headOption.getOrElse(
              throw new RuntimeException(
                // TODO: check the error comment once we have settle with the name
                "Can't create binding for an empty list without knowing the type, use SdkBindingData.of<type>Collection(...)"
              )
            )
          )

        }
        collections(internalType)

      case map: Map[_, _] =>
        val internalType = internalTypeOpt.getOrElse {
          val head = map.headOption.getOrElse(
            throw new RuntimeException(
              // TODO: check the error comment once we have settle with the name
              "Can't create binding for an empty map without knowing the type, use SdkBindingData.of<type>Map(...)"
            )
          )
          head._1 match {
            case _: String => toSdkLiteralType(head._2)
            case _ =>
              throw new RuntimeException(
                "Can't create binding for a map with key type other than String."
              )
          }
        }
        maps(internalType)

      case other =>
        throw new IllegalStateException(
          s"${other.getClass.getSimpleName} class is not supported as SdkBindingData inner class"
        )
    }
  }
}
