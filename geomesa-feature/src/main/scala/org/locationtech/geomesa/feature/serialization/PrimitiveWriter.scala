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

import java.nio.ByteBuffer
import java.util.Date

/** A collection of [[DatumWriter]]s for writing primitive-like datums.
  *
  * Created by mmatz on 4/7/15.
  */
trait PrimitiveWriter {

  def writeString: DatumWriter[String]
  def writeInt: DatumWriter[Int]
  def writeLong: DatumWriter[Long]
  def writeFloat: DatumWriter[Float]
  def writeDouble: DatumWriter[Double]
  def writeBoolean: DatumWriter[Boolean]
  def writeDate: DatumWriter[Date]
  def writeBytes: DatumWriter[ByteBuffer]
}
