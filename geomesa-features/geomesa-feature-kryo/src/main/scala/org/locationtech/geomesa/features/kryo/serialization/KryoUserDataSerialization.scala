/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.features.kryo.serialization

import com.esotericsoftware.kryo.io.{Input, Output}
import org.locationtech.geomesa.features.serialization.GenericMapSerialization

object KryoUserDataSerialization extends GenericMapSerialization[Output, Input] {

  override def serialize(out: Output, map: java.util.Map[AnyRef, AnyRef]): Unit = {
    import scala.collection.JavaConversions._

    // may not be able to write all entries - must pre-filter to know correct count
    val (toWrite, toIgnore) = map.partition { case (key, value) => key != null && value != null && canSerialize(key) }

    if (toIgnore.nonEmpty) {
      logger.warn(s"Skipping serialization of entries: ${toIgnore.mkString("[", "],[", "]")}")
    }

    out.writeInt(toWrite.size) // don't use positive optimized version for back compatibility

    toWrite.foreach { case (key, value) =>
      out.writeString(key.getClass.getName)
      write(out, key)
      out.writeString(value.getClass.getName)
      write(out, value)
    }
  }

  override def deserialize(in: Input): java.util.Map[AnyRef, AnyRef] = {
    var toRead = in.readInt()
    val map = new java.util.HashMap[AnyRef, AnyRef](toRead)

    while (toRead > 0) {
      val key = read(in, Class.forName(in.readString()))
      val value = read(in, Class.forName(in.readString()))
      map.put(key, value)
      toRead -= 1
    }

    map
  }
}
