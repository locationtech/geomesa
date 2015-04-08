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

import org.apache.avro.io.Decoder
import org.locationtech.geomesa.feature.serialization.{AbstractReader, DatumReader}

/** Implemenation of [[AbstractReader]] for Avro.
  *
  * Created by mmatz on 4/7/15.
  */
class AvroReader(decoder: Decoder) extends AbstractReader {
  import AvroWriter.NOT_NULL_INDEX

  override val readString: DatumReader[String] = decoder.readString
  override val readInt: DatumReader[Int] = decoder.readInt
  override val readLong: DatumReader[Long] = decoder.readLong
  override val readFloat: DatumReader[Float] = decoder.readFloat
  override val readDouble: DatumReader[Double] = decoder.readDouble
  override val readBoolean: DatumReader[Boolean] = decoder.readBoolean
  override val readDate: DatumReader[Date] = () => new Date(decoder.readLong())
  override val readBytes: DatumReader[ByteBuffer] = () => decoder.readBytes(null)

  override def readOption[T](readRaw: DatumReader[T]): DatumReader[Option[T]] = () => {
    if (decoder.readIndex() == NOT_NULL_INDEX) {
      Some(readRaw())
    } else {
      decoder.readNull()
      None
    }
  }

  override val readArrayStart: DatumReader[Long] = decoder.readArrayStart
}
