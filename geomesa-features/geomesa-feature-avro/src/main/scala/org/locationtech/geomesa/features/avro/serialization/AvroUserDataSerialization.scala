/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.avro.serialization

import java.nio.ByteBuffer

import org.apache.avro.io.{Decoder, Encoder}
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.features.serialization.HintKeySerialization

object AvroUserDataSerialization {

  import scala.collection.JavaConverters._

  def serialize(out: Encoder, map: java.util.Map[_ <: AnyRef, _ <: AnyRef]): Unit = {
    out.writeArrayStart()
    out.setItemCount(map.size)

    def write(value: Any): Unit = {
      value match {
        case null                 => out.writeIndex(0); out.writeNull()
        case v: String            => out.writeIndex(1); out.writeString(v)
        case v: java.lang.Integer => out.writeIndex(2); out.writeInt(v)
        case v: java.lang.Long    => out.writeIndex(3); out.writeLong(v)
        case v: java.lang.Float   => out.writeIndex(4); out.writeFloat(v)
        case v: java.lang.Double  => out.writeIndex(5); out.writeDouble(v)
        case v: java.lang.Boolean => out.writeIndex(6); out.writeBoolean(v)
        case v: Array[Byte]       => out.writeIndex(7); out.writeBytes(v)
        case v: Hints.Key if HintKeySerialization.canSerialize(v) => out.writeIndex(8); out.writeEnum(HintKeySerialization.keyToEnum(v))
        case _ =>
          throw new IllegalArgumentException(s"Serialization not implemented for '$value' of type ${value.getClass}")
      }
    }

    map.asScala.foreach { case (key, value) =>
      out.startItem()
      write(key)
      write(value)
    }

    out.writeArrayEnd()
  }

  def deserialize(in: Decoder): java.util.Map[AnyRef, AnyRef] = {
    val size = in.readArrayStart().toInt
    val map = new java.util.HashMap[AnyRef, AnyRef](size)
    deserializeWithSize(in, size, map)
    map
  }

  def deserialize(in: Decoder, map: java.util.Map[AnyRef, AnyRef]): Unit =
    deserializeWithSize(in, in.readArrayStart().toInt, map)

  private def deserializeWithSize(in: Decoder, size: Int, map: java.util.Map[AnyRef, AnyRef]): Unit = {
    var bb: ByteBuffer = null

    def read(): AnyRef = {
      in.readIndex() match {
        case 0 => in.readNull(); null
        case 1 => in.readString()
        case 2 => Int.box(in.readInt())
        case 3 => Long.box(in.readLong())
        case 4 => Float.box(in.readFloat())
        case 5 => Double.box(in.readDouble())
        case 6 => Boolean.box(in.readBoolean())
        case 7 => bb = in.readBytes(bb); val array = Array.ofDim[Byte](bb.remaining()); bb.get(array); array
        case 8 => HintKeySerialization.enumToKey(in.readEnum())
        case i => throw new IllegalArgumentException(s"Unexpected union type: $i")
      }
    }

    var remaining = size
    while (remaining > 0) {
      val key = read()
      val value = read()
      map.put(key, value)
      remaining -= 1
      if (remaining == 0) {
        remaining = in.arrayNext().toInt
      }
    }
  }
}
