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

import com.vividsolutions.jts.geom.Geometry
import org.apache.avro.io.Decoder
import org.locationtech.geomesa.feature.serialization._

/** Implemenation of [[AbstractReader]] for Avro. */
class AvroReader extends AbstractReader[Decoder] {
  import AvroWriter.NOT_NULL_INDEX

  override val readString: DatumReader[Decoder, String] = (decoder) => decoder.readString
  override val readInt: DatumReader[Decoder, Int] = (decoder) => decoder.readInt
  override val readPositiveInt: DatumReader[Decoder, Int] = readInt // no optimization
  override val readLong: DatumReader[Decoder, Long] = (decoder) => decoder.readLong
  override val readFloat: DatumReader[Decoder, Float] = (decoder) => decoder.readFloat
  override val readDouble: DatumReader[Decoder, Double] = (decoder) => decoder.readDouble
  override val readBoolean: DatumReader[Decoder, Boolean] = (decoder) => decoder.readBoolean
  override val readDate: DatumReader[Decoder, Date] = (decoder) => new Date(decoder.readLong())

  override val readBytes: DatumReader[Decoder, Array[Byte]] = (decoder) => {
    val buffer: ByteBuffer = decoder.readBytes(null)

    val pos: Int = buffer.position
    val len: Int = buffer.limit - pos

    if (len == 0) {
      Array.emptyByteArray
    } else if (pos == 0 && buffer.hasArray) {
      // should always be able to acess directly
      buffer.array()
    } else {
      // just in case
      val array = Array.ofDim[Byte](len)
      buffer.get(array, pos, len)
      array
    }
  }

  override def readNullable[T](readRaw: DatumReader[Decoder, T]): DatumReader[Decoder, T] = (decoder) => {
    if (decoder.readIndex() == NOT_NULL_INDEX) {
      readRaw(decoder)
    } else {
      decoder.readNull()
      null.asInstanceOf[T]
    }
  }

  override val readArrayStart: (Decoder) => Int = (decoder) => {
    // always writing an Int so this cast should be safe
    decoder.readArrayStart.asInstanceOf[Int]
  }

  override val readGeometry: DatumReader[Decoder, Geometry] = readGeometryAsWKB
}
