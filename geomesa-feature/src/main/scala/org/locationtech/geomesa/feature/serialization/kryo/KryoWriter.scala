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

import com.esotericsoftware.kryo.io.{Input, Output}
import org.locationtech.geomesa.feature.serialization.{AbstractWriter, DatumWriter}

/** Implemenation of [[AbstractWriter]] for Kryo.
  *
  * Created by mmatz on 4/7/15.
  */
class KryoWriter(in: Input, out: Output) extends AbstractWriter {
  import KryoWriter.{NON_NULL_MARKER_BYTE, NULL_MARKER_BYTE}
  
  override val writeString: DatumWriter[String] = out.writeString
  override val writeInt: DatumWriter[Int] = out.writeInt
  override val writeLong: DatumWriter[Long] = out.writeLong
  override val writeFloat: DatumWriter[Float] = out.writeFloat
  override val writeDouble: DatumWriter[Double] = out.writeDouble
  override val writeBoolean: DatumWriter[Boolean] = out.writeBoolean
  override val writeDate: DatumWriter[Date] = (date) => out.writeLong(date.getTime)
  override val writeBytes: DatumWriter[ByteBuffer] = (bytes) => {
    // based on logic from Avro's BinaryEncoder
    val pos: Int = bytes.position
    val len: Int = bytes.limit - pos

    out.writeInt(len)

    if (len > 0) {
      if (bytes.hasArray) {
        out.writeBytes(bytes.array, bytes.arrayOffset + pos, len)
      } else {
        val b: Array[Byte] = new Array[Byte](len)
        bytes.get(b, 0, len)
        out.writeBytes(b, 0, len)
      }
    }
  }

  override val writeArrayStart: DatumWriter[Long] = out.writeLong

  override val startItem = ()
  override val endArray = ()

  // override to avoid Option creation
  override def writeNullable[T](writeRaw: DatumWriter[T]): DatumWriter[T] = (raw) => {
    if (raw != null) {
      out.writeByte(NON_NULL_MARKER_BYTE)
      writeRaw(raw)
    } else {
      out.writeByte(NULL_MARKER_BYTE)
    }
  }

  override def writeOption[T](writeRaw: DatumWriter[T]): DatumWriter[Option[T]] = {
    case Some(raw) =>
      out.writeByte(NON_NULL_MARKER_BYTE)
      writeRaw(raw)
    case _ =>
      out.writeByte(NULL_MARKER_BYTE)
  }
}

object KryoWriter {
  lazy val NULL_MARKER_BYTE     = 0.asInstanceOf[Byte]
  lazy val NON_NULL_MARKER_BYTE = 1.asInstanceOf[Byte]
}