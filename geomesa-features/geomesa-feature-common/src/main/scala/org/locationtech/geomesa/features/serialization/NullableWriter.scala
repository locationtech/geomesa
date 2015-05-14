/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.features.serialization

import java.util.{Date, UUID}

/** A collection of [[DatumWriter]]s for writing objects which may be null.
  *
  */
trait NullableWriter[Writer] extends PrimitiveWriter[Writer] with NullableCheck {

  /**
   * Write any value that may be null.
   */
  def writeNullable[T](rawWrite: DatumWriter[Writer, T]): DatumWriter[Writer, T]

  val writeNullableInt: DatumWriter[Writer, Int] = writeNullable(writeInt)
  val writeNullableLong: DatumWriter[Writer, Long] = writeNullable(writeLong)
  val writeNullableFloat: DatumWriter[Writer, Float] = writeNullable(writeFloat)
  val writeNullableDouble: DatumWriter[Writer, Double] = writeNullable(writeDouble)
  val writeNullableBoolean: DatumWriter[Writer, Boolean] = writeNullable(writeBoolean)
  val writeNullableDate: DatumWriter[Writer, Date] = writeNullable(writeDate)
}

trait NullableCheck {

  type isNullableFn = (Class[_]) => Boolean

  val notNullable: isNullableFn = (_) => false

  /** Standard definition of nullable types.  Only applies when the type is known, for example, for attribute values.
    * Does not apply when the type is not know, for example, in a user data map, because when the type is unknown then
    * the class of the object has to be serialized which serves as a null value flag.
    */
  val standardNullable: isNullableFn = {
    case c if classOf[java.lang.Integer].isAssignableFrom(c) => true
    case c if classOf[java.lang.Long].isAssignableFrom(c) => true
    case c if classOf[java.lang.Float].isAssignableFrom(c) => true
    case c if classOf[java.lang.Double].isAssignableFrom(c) => true
    case c if classOf[java.lang.Boolean].isAssignableFrom(c) => true
    case c if classOf[Date].isAssignableFrom(c) => true
    case c if classOf[UUID].isAssignableFrom(c) => true
    case _ => false
  }
}
