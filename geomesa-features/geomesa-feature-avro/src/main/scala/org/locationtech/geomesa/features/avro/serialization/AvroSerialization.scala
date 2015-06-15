/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/
package org.locationtech.geomesa.features.avro.serialization

import java.nio.ByteBuffer
import java.util.Date

import com.vividsolutions.jts.geom.Geometry
import org.apache.avro.io.{Decoder, Encoder}
import org.locationtech.geomesa.features.serialization._

/** Avro specific serialization logic.
 */
object AvroSerialization
  extends EncodingsCache[Encoder]
  with DecodingsCache[Decoder] {

  override protected val datumWritersFactory: () => AbstractWriter[Encoder] = () => new AvroWriter()
  override protected val datumReadersFactory: () => AbstractReader[Decoder] = () => new AvroReader()

//  def getEncodings(sft: SimpleFeatureType)

  val NOT_NULL_INDEX = 0
  val NULL_INDEX = 1
}

/** Implemenation of [[AbstractWriter]] for Avro. */
class AvroWriter extends AbstractWriter[Encoder] {

  override val writeString: DatumWriter[Encoder, String] = (encoder, str) => encoder.writeString(str)
  override val writeInt: DatumWriter[Encoder, Int] = (encoder, int) => encoder.writeInt(int)
  override val writePositiveInt: DatumWriter[Encoder, Int] = writeInt // no optimization
  override val writeLong: DatumWriter[Encoder, Long] = (encoder, long) => encoder.writeLong(long)
  override val writeFloat: DatumWriter[Encoder, Float] = (encoder, float) => encoder.writeFloat(float)
  override val writeDouble: DatumWriter[Encoder, Double] = (encoder, double) => encoder.writeDouble(double)
  override val writeBoolean: DatumWriter[Encoder, Boolean] = (encoder, bool) => encoder.writeBoolean(bool)
  override val writeDate: DatumWriter[Encoder, Date] = (encoder, date) => encoder.writeLong(date.getTime)
  override val writeBytes: DatumWriter[Encoder, Array[Byte]] = (encoder, bytes) => encoder.writeBytes(bytes)

  override def writeNullable[T](writeRaw: DatumWriter[Encoder, T]): DatumWriter[Encoder, T] =
    (encoder, raw) => {
      if (raw != null) {
        encoder.writeIndex(AvroSerialization.NOT_NULL_INDEX)
        writeRaw(encoder, raw)
      } else {
        encoder.writeIndex(AvroSerialization.NULL_INDEX)
        encoder.writeNull()
      }
  }

  override val writeArrayStart: DatumWriter[Encoder, Int] = (encoder, arrayLen) => {
    encoder.writeArrayStart()
    encoder.setItemCount(arrayLen)
  }

  override val startItem: (Encoder) => Unit = (encoder) => encoder.startItem()
  override val endArray: (Encoder) => Unit = (encoder) => encoder.writeArrayEnd()
}

/** Implemenation of [[AbstractReader]] for Avro. */
class AvroReader extends AbstractReader[Decoder] {

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
    if (decoder.readIndex() == AvroSerialization.NOT_NULL_INDEX) {
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

  override def selectGeometryReader(version: Version): DatumReader[Decoder, Geometry] = readGeometryAsWKB
}
