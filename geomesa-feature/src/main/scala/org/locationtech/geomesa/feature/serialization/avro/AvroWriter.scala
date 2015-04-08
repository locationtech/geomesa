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

package org.locationtech.geomesa.feature.serialization.avro

import java.nio.ByteBuffer
import java.util.Date

import org.apache.avro.io.Encoder
import org.locationtech.geomesa.feature.serialization._

/** Implemenation of [[AbstractWriter]] for Avro. */
class AvroWriter(encoder: Encoder) extends AbstractWriter {
  import AvroWriter.{NOT_NULL_INDEX, NULL_INDEX}

  override val writeString: DatumWriter[String] =  encoder.writeString
  override val writeInt: DatumWriter[Int] = encoder.writeInt
  override val writeLong: DatumWriter[Long] = encoder.writeLong
  override val writeFloat: DatumWriter[Float] = encoder.writeFloat
  override val writeDouble: DatumWriter[Double] = encoder.writeDouble
  override val writeBoolean: DatumWriter[Boolean] = encoder.writeBoolean
  override val writeDate: DatumWriter[Date] = (date) => encoder.writeLong(date.getTime)
  override val writeBytes: DatumWriter[ByteBuffer] = encoder.writeBytes

  override def writeNullable[T](writeRaw: DatumWriter[T]): DatumWriter[T] = (raw) => {
    if (raw != null) {
      encoder.writeIndex(NOT_NULL_INDEX)
      writeRaw(raw)
    } else {
      encoder.writeIndex(NULL_INDEX)
      encoder.writeNull()
    }
  }

  override val writeArrayStart: DatumWriter[Long] = (arrayLen) => {
    encoder.writeArrayStart()
    encoder.setItemCount(arrayLen)
  }

  override def startItem() = encoder.startItem()
  override def endArray() = encoder.writeArrayEnd()
}

object AvroWriter {
  val NOT_NULL_INDEX = 0
  val NULL_INDEX = 1
}