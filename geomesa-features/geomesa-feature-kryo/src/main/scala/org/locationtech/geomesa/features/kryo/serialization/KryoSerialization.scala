/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/
package org.locationtech.geomesa.features.kryo.serialization

import java.util.Date

import com.esotericsoftware.kryo.io.{Input, Output}
import com.vividsolutions.jts.geom.Geometry
import org.locationtech.geomesa.features.serialization._


/** Kryo specific serialization logic.
  */
object KryoSerialization
  extends EncodingsCache[Output]
  with DecodingsCache[Input] {

  override protected val datumWritersFactory: () => AbstractWriter[Output] = () => new KryoWriter()
  override protected val datumReadersFactory: () => AbstractReader[Input] = () => new KryoReader()

  lazy val NULL_MARKER_BYTE     = 0.asInstanceOf[Byte]
  lazy val NON_NULL_MARKER_BYTE = 1.asInstanceOf[Byte]
}

/** Implemenation of [[AbstractWriter]] for Kryo. */
class KryoWriter extends AbstractWriter[Output] {

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

  override val startItem: (Output) => Unit = (_) => {}
  override def endArray: (Output) => Unit = (_) => {}

  override def writeNullable[T](writeRaw: DatumWriter[Output, T]): DatumWriter[Output, T] = (out, raw) => {
    if (raw != null) {
      out.writeByte(KryoSerialization.NON_NULL_MARKER_BYTE)
      writeRaw(out, raw)
    } else {
      out.writeByte(KryoSerialization.NULL_MARKER_BYTE)
    }
  }
}

/** Implemenation of [[AbstractReader]] for Kryo. */
class KryoReader extends AbstractReader[Input] {

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
    if (in.readByte() == KryoSerialization.NON_NULL_MARKER_BYTE) {
      readRaw(in)
    } else {
      null.asInstanceOf[T]
    }
  }

  override val readArrayStart: (Input) => Int = (in) => in.readInt

  override def selectGeometryReader(version: Version): DatumReader[Input, Geometry] = {
    if (version == 0) {
      readGeometryAsWKB
    } else {
      readGeometryDirectly
    }
  }
}
