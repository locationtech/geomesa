/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.avro.serialization

import org.apache.avro.io.{Decoder, Encoder}
import org.locationtech.geomesa.features.serialization.GenericMapSerialization

object AvroUserDataSerialization extends GenericMapSerialization[Encoder, Decoder] {

  import scala.collection.JavaConverters._

  val NullMarkerString = "<null>"

  override def serialize(out: Encoder, map: java.util.Map[AnyRef, AnyRef]): Unit = {
    // may not be able to write all entries - must pre-filter to know correct count
    val filtered = map.asScala.filter { case (key, value) =>
      if (canSerialize(key)) {
        true
      } else {
        logger.warn(s"Can't serialize Map entry ($key,$value) - it will be skipped.")
        false
      }
    }

    out.writeArrayStart()
    out.setItemCount(filtered.size)

    filtered.foreach { case (key, value) =>
      out.startItem()
      if (key == null) {
        out.writeString(NullMarkerString)
      } else {
        out.writeString(key.getClass.getName)
        write(out, key)
      }
      if (value == null) {
        out.writeString(NullMarkerString)
      } else {
        out.writeString(value.getClass.getName)
        write(out, value)
      }
    }

    out.writeArrayEnd()
  }

  override def deserialize(in: Decoder): java.util.Map[AnyRef, AnyRef] = {
    val size = in.readArrayStart().toInt
    val map = new java.util.HashMap[AnyRef, AnyRef](size)
    deserializeWithSize(in, size, map)
    map
  }

  override def deserialize(in: Decoder, map: java.util.Map[AnyRef, AnyRef]): Unit = {
    deserializeWithSize(in, in.readArrayStart().toInt, map)
  }

  private def deserializeWithSize(in: Decoder, size: Int, map: java.util.Map[AnyRef, AnyRef]): Unit = {
    var remaining = size
    while (remaining > 0) {
      val keyClass = in.readString()
      val key = if (keyClass == NullMarkerString) { null } else { read(in, Class.forName(keyClass)) }
      val valueClass = in.readString()
      val value = if (valueClass == NullMarkerString) { null } else { read(in, Class.forName(valueClass))}
      map.put(key, value)
      remaining -= 1
      if (remaining == 0) {
        remaining = in.arrayNext().toInt
      }
    }
  }

  override protected def writeBytes(out: Encoder, bytes: Array[Byte]): Unit = out.writeBytes(bytes)

  override protected def readBytes(in: Decoder): Array[Byte] = {
    val buffer = in.readBytes(null)
    val bytes = Array.ofDim[Byte](buffer.remaining())
    buffer.get(bytes)
    bytes
  }

  override protected def writeList(out: Encoder, list: java.util.List[AnyRef]): Unit = {
    out.writeArrayStart()
    out.setItemCount(list.size())
    list.asScala.foreach { value =>
      out.startItem()
      if (value == null) {
        out.writeString(NullMarkerString)
      } else {
        out.writeString(value.getClass.getName)
        write(out, value)
      }
    }
    out.writeArrayEnd()
  }

  override protected def readList(in: Decoder): java.util.List[AnyRef] = {
    val size = in.readArrayStart().toInt
    val list = new java.util.ArrayList[AnyRef](size)
    var remaining = size
    while (remaining > 0) {
      val clas = in.readString()
      val value = if (clas == NullMarkerString) { null } else { read(in, Class.forName(clas)) }
      list.add(value)
      remaining -= 1
      if (remaining == 0) {
        remaining = in.arrayNext().toInt
      }
    }
    list
  }
}
