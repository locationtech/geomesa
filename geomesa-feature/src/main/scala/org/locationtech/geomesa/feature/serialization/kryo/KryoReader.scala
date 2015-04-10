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

import com.esotericsoftware.kryo.io.Input
import com.vividsolutions.jts.geom.Geometry
import org.locationtech.geomesa.feature.serialization._

/** Implemenation of [[AbstractReader]] for Kryo. */
class KryoReader extends AbstractReader[Input] {
  import KryoWriter.NON_NULL_MARKER_BYTE

  override val readString: DatumReader[Input, String] = (in) => in.readString
  override val readInt: DatumReader[Input, Int] = (in) => in.readInt
  override val readPositiveInt: DatumReader[Input, Int] = (in) => in.readInt(true)
  override val readLong: DatumReader[Input, Long] = (in) => in.readLong
  override val readFloat: DatumReader[Input, Float] = (in) => in.readFloat
  override val readDouble: DatumReader[Input, Double] = (in) => in.readDouble
  override val readBoolean: DatumReader[Input, Boolean] = (in) => in.readBoolean
  override val readDate: DatumReader[Input, Date] = (in) => new Date(in.readLong())
  override val readBytes: DatumReader[Input, Array[Byte]] = (in) => {
    val len = in.readInt(true)
    in.readBytes(len)
  }

  override def readNullable[T](readRaw: DatumReader[Input, T]): DatumReader[Input, T] = (in) => {
    if (in.readByte() == NON_NULL_MARKER_BYTE) {
      readRaw(in)
    } else {
      null.asInstanceOf[T]
    }
  }

  override val readArrayStart: (Input) => Int = (in) => in.readInt

  override def readGeometry: DatumReader[Input, Geometry] = (in) => {
    if (version == 0) {
      readGeometryAsWKB(in)
    } else {
      readGeometryDirectly(in)
    }
  }
}
