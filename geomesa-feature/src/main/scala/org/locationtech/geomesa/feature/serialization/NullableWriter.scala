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

package org.locationtech.geomesa.feature.serialization

import java.util.Date

/** A collection of [[DatumWriter]]s for writing objects which may be null.
  *
  * Created by mmatz on 4/7/15.
  */
trait NullableWriter extends PrimitiveWriter {

  /**
   * Writs a null or not-null and if not-null then the value.
   */
  def writeOption[T](rawWrite: DatumWriter[T]): DatumWriter[Option[T]]

  /**
   * Write any value that may be null.
   */
  def writeNullable[T](rawWrite: DatumWriter[T]): DatumWriter[T] = (obj) => writeOption(rawWrite)(Option(obj))

  val writeNullableInt: DatumWriter[Int] = writeNullable(writeInt)
  val writeNullableLong: DatumWriter[Long] = writeNullable(writeLong)
  val writeNullableFloat: DatumWriter[Float] = writeNullable(writeFloat)
  val writeNullableDouble: DatumWriter[Double] = writeNullable(writeDouble)
  val writeNullableBoolean: DatumWriter[Boolean] = writeNullable(writeBoolean)
  val writeNullableDate: DatumWriter[Date] = writeNullable(writeDate)
}
