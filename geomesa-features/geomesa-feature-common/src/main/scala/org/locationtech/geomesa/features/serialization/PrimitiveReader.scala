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

import java.util.Date

/** A collection of [[DatumReader]]s for reading primitive-like datums.
  *
  */
trait PrimitiveReader[Reader] {

  def readString: DatumReader[Reader, String]
  def readInt: DatumReader[Reader, Int]
  def readLong: DatumReader[Reader, Long]
  def readFloat: DatumReader[Reader, Float]
  def readDouble: DatumReader[Reader, Double]
  def readBoolean: DatumReader[Reader, Boolean]
  def readDate: DatumReader[Reader, Date]
  def readBytes: DatumReader[Reader, Array[Byte]]

  /** A [[DatumReader]] for reading an [[Int]] written with positive optimization. */
  def readPositiveInt: DatumReader[Reader, Int]
}
