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

/** A collection of [[DatumReader]]s for reading objects which may be null.
  *
  */
trait NullableReader extends PrimitiveReader {

  /**
   * Read any value that may be null.
   */
  def readNullable[T](readRaw: DatumReader[T]): DatumReader[T]

  val readNullableInt: DatumReader[Int] = readNullable(readInt)
  val readNullableLong: DatumReader[Long] = readNullable(readLong)
  val readNullableFloat: DatumReader[Float] = readNullable(readFloat)
  val readNullableDouble: DatumReader[Double] = readNullable(readDouble)
  val readNullableBoolean: DatumReader[Boolean] = readNullable(readBoolean)
  val readNullableDate: DatumReader[Date] = readNullable(readDate)
}
