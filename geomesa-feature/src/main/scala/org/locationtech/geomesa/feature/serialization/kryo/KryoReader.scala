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

package org.locationtech.geomesa.feature.serialization.kryo

import java.nio.ByteBuffer
import java.util.Date

import com.esotericsoftware.kryo.io.Input
import org.locationtech.geomesa.feature.serialization._

/** Implemenation of [[AbstractReader]] for Kryo. */
class KryoReader(in: Input) extends AbstractReader {
  import KryoWriter.NON_NULL_MARKER_BYTE

  override val readString: DatumReader[String] = in.readString
  override val readInt: DatumReader[Int] = in.readInt
  override val readLong: DatumReader[Long] = in.readLong
  override val readFloat: DatumReader[Float] = in.readFloat
  override val readDouble: DatumReader[Double] = in.readDouble
  override val readBoolean: DatumReader[Boolean] = in.readBoolean
  override val readDate: DatumReader[Date] = () => new Date(in.readLong())
  override val readBytes: DatumReader[ByteBuffer] = () => {
    val len = in.readInt()
    val bytes = in.readBytes(len)
    ByteBuffer.wrap(bytes)
  }

  override def readNullable[T](readRaw: DatumReader[T]): DatumReader[T] = () => {
    if (in.readByte() == NON_NULL_MARKER_BYTE) {
      readRaw()
    } else {
      null.asInstanceOf
    }
  }

  override val readArrayStart: DatumReader[Long] = in.readLong
}
