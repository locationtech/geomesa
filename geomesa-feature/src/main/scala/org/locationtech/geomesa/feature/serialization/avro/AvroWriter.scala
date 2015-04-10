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

import java.util.Date

import org.apache.avro.io.Encoder
import org.locationtech.geomesa.feature.serialization._

/** Implemenation of [[AbstractWriter]] for Avro. */
class AvroWriter extends AbstractWriter[Encoder] {
  import AvroWriter.{NOT_NULL_INDEX, NULL_INDEX}

  override val writeString: DatumWriter[Encoder, String] = (encoder, str) => encoder.writeString(str)
  override val writeInt: DatumWriter[Encoder, Int] = (encoder, int) => encoder.writeInt(int)
  override val writePositiveInt: DatumWriter[Encoder, Int] = writeInt // no optimization
  override val writeLong: DatumWriter[Encoder, Long] = (encoder, long) => encoder.writeLong(long)
  override val writeFloat: DatumWriter[Encoder, Float] = (encoder, float) => encoder.writeFloat(float)
  override val writeDouble: DatumWriter[Encoder, Double] = (encoder, double) => encoder.writeDouble(double)
  override val writeBoolean: DatumWriter[Encoder, Boolean] = (encoder, boolean) => encoder.writeBoolean(boolean)
  override val writeDate: DatumWriter[Encoder, Date] = (encoder, date) => encoder.writeLong(date.getTime)
  override val writeBytes: DatumWriter[Encoder, Array[Byte]] = (encoder, bytes) => encoder.writeBytes(bytes)

  override def writeNullable[T](writeRaw: DatumWriter[Encoder, T]): DatumWriter[Encoder, T] = (encoder, raw) => {
    if (raw != null) {
      encoder.writeIndex(NOT_NULL_INDEX)
      writeRaw(encoder, raw)
    } else {
      encoder.writeIndex(NULL_INDEX)
      encoder.writeNull()
    }
  }

  override val writeArrayStart: DatumWriter[Encoder, Int] = (encoder, arrayLen) => {
    encoder.writeArrayStart()
    encoder.setItemCount(arrayLen)
  }

  override val startItem: (Encoder) => Unit = (encoder) => encoder.startItem()
  override def endArray: (Encoder) => Unit = (encoder) => encoder.writeArrayEnd()
}

object AvroWriter {
  val NOT_NULL_INDEX = 0
  val NULL_INDEX = 1
}