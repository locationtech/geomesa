/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.avro.serialization

import java.nio.ByteBuffer

import com.typesafe.scalalogging.LazyLogging
import org.apache.avro.io.{Decoder, Encoder}
import org.geotools.util.factory.Hints

object AvroUserDataSerialization extends LazyLogging {

  import scala.collection.JavaConverters._

  def serialize(out: Encoder, map: java.util.Map[_ <: AnyRef, _ <: AnyRef]): Unit = {
    def write(value: Any): Option[() => Unit] = {
      value match {
        case null                 => Some(() => { out.writeIndex(0); out.writeNull() })
        case v: String            => Some(() => { out.writeIndex(1); out.writeString(v) })
        case v: java.lang.Integer => Some(() => { out.writeIndex(2); out.writeInt(v) })
        case v: java.lang.Long    => Some(() => { out.writeIndex(3); out.writeLong(v) })
        case v: java.lang.Float   => Some(() => { out.writeIndex(4); out.writeFloat(v) })
        case v: java.lang.Double  => Some(() => { out.writeIndex(5); out.writeDouble(v) })
        case v: java.lang.Boolean => Some(() => { out.writeIndex(6); out.writeBoolean(v) })
        case v: Array[Byte]       => Some(() => { out.writeIndex(7); out.writeBytes(v) })
        case v: Hints.Key if v == Hints.USE_PROVIDED_FID => logger.warn("Dropping USE_PROVIDED_FID hint"); None
        case _ =>
          throw new IllegalArgumentException(s"Serialization not implemented for '$value' of type ${value.getClass}")
      }
    }

    val writes = map.asScala.flatMap { case (key, value) =>
      for (k <- write(key); v <- write(value)) yield { () => { out.startItem(); k(); v() }}
    }

    out.writeArrayStart()
    out.setItemCount(writes.size)
    writes.foreach(_.apply())
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
