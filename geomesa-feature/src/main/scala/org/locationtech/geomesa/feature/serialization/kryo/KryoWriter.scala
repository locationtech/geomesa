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

import java.util.Date

import com.esotericsoftware.kryo.io.Output
import org.locationtech.geomesa.feature.serialization.{AbstractWriter, DatumWriter}

/** Implemenation of [[AbstractWriter]] for Kryo. */
class KryoWriter extends AbstractWriter[Output] {
  import KryoWriter.{NON_NULL_MARKER_BYTE, NULL_MARKER_BYTE}
  
  override val writeString: DatumWriter[Output, String] = (out, str) => out.writeString(str)
  override val writeInt: DatumWriter[Output, Int] = (out, int) => out.writeInt(int)
  override val writePositiveInt: DatumWriter[Output, Int] = (out, int) => out.writeInt(int, true)
  override val writeLong: DatumWriter[Output, Long] = (out, long) => out.writeLong(long)
  override val writeFloat: DatumWriter[Output, Float] = (out, float) => out.writeFloat(float)
  override val writeDouble: DatumWriter[Output, Double] = (out, double) => out.writeDouble(double)
  override val writeBoolean: DatumWriter[Output, Boolean] = (out, boolean) => out.writeBoolean(boolean)
  override val writeDate: DatumWriter[Output, Date] = (out, date) => out.writeLong(date.getTime)
  override val writeBytes: DatumWriter[Output, Array[Byte]] = (out, bytes) => {
    out.writeInt(bytes.length, true)
    out.writeBytes(bytes)
  }

  override val writeArrayStart: DatumWriter[Output, Int] = (out, arrayLen) => out.writeInt(arrayLen)

  override def startItem(out: Output) = {}
  override def endArray(out: Output) = {}

  override def writeNullable[T](writeRaw: DatumWriter[Output, T]): DatumWriter[Output, T] = (out, raw) => {
    if (raw != null) {
      out.writeByte(NON_NULL_MARKER_BYTE)
      writeRaw(out, raw)
    } else {
      out.writeByte(NULL_MARKER_BYTE)
    }
  }

}

object KryoWriter {
  lazy val NULL_MARKER_BYTE     = 0.asInstanceOf[Byte]
  lazy val NON_NULL_MARKER_BYTE = 1.asInstanceOf[Byte]
}