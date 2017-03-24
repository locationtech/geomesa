/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.avro.serialization

import org.apache.avro.io.{Decoder, Encoder}
import org.locationtech.geomesa.features.serialization.GenericMapSerialization

object AvroUserDataSerialization extends GenericMapSerialization[Encoder, Decoder] {

  val NullMarkerString = "<null>"

  override def serialize(out: Encoder, map: java.util.Map[AnyRef, AnyRef]): Unit = {
    import scala.collection.JavaConversions._

    // may not be able to write all entries - must pre-filter to know correct count
    val filtered = map.filter {
      case (key, value) =>
        if (canSerialize(key)) {
          true
        } else {
          logger.warn(s"Can't serialize Map entry ($key,$value) - it will be skipped.")
          false
        }
    }

    out.writeArrayStart()
    out.setItemCount(filtered.size)

    filtered.foreach {
      case (key, value) =>
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
    var toRead = in.readArrayStart().toInt
    val map = new java.util.HashMap[AnyRef, AnyRef](toRead)

    while (toRead > 0) {
      val keyClass = in.readString()
      val key = if (keyClass == NullMarkerString) { null } else { read(in, Class.forName(keyClass)) }
      val valueClass = in.readString()
      val value = if (valueClass == NullMarkerString) { null } else { read(in, Class.forName(valueClass))}
      map.put(key, value)
      toRead -= 1
      if (toRead == 0) {
        toRead = in.arrayNext().toInt
      }
    }

    map
  }
}
