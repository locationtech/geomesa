/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.kryo.serialization

import com.esotericsoftware.kryo.io.{Input, Output}
import org.geotools.factory.Hints
import org.locationtech.geomesa.features.serialization.GenericMapSerialization

object KryoUserDataSerialization extends GenericMapSerialization[Output, Input] {

  private val nullMapping = "$_"

  private val baseClassMappings: Map[Class[_], String] = Map(
    classOf[String]            -> "$s",
    classOf[Int]               -> "$i",
    classOf[java.lang.Integer] -> "$i",
    classOf[Long]              -> "$l",
    classOf[java.lang.Long]    -> "$l",
    classOf[Float]             -> "$f",
    classOf[java.lang.Float]   -> "$f",
    classOf[Double]            -> "$d",
    classOf[java.lang.Double]  -> "$d",
    classOf[Boolean]           -> "$b",
    classOf[java.lang.Boolean] -> "$b",
    classOf[java.util.Date]    -> "$D",
    classOf[Hints.Key]         -> "$h"
  )

  private val baseClassLookups: Map[String, Class[_]] = baseClassMappings.filterNot(_._1.isPrimitive).map(_.swap)

  override def serialize(out: Output, javaMap: java.util.Map[AnyRef, AnyRef]): Unit = {
    import scala.collection.JavaConverters._

    val map = javaMap.asScala

    // may not be able to write all entries - must pre-filter to know correct count
    val skip = new java.util.HashSet[AnyRef]()
    map.foreach { case (k, _) => if (k == null || !canSerialize(k)) { skip.add(k) } }

    val toWrite = if (skip.isEmpty) { map } else {
      logger.warn(s"Skipping serialization of entries: " +
          map.collect { case (k, v) if skip.contains(k) => s"$k->$v" }.mkString("[", "],[", "]"))
      map.filterNot { case (k, v) => skip.contains(k) }
    }

    out.writeInt(toWrite.size) // don't use positive optimized version for back compatibility

    toWrite.foreach { case (key, value) =>
      out.writeString(baseClassMappings.getOrElse(key.getClass, key.getClass.getName))
      write(out, key)
      if (value == null) {
        out.writeString(nullMapping)
      } else {
        out.writeString(baseClassMappings.getOrElse(value.getClass, value.getClass.getName))
        write(out, value)
      }
    }
  }

  override def deserialize(in: Input): java.util.Map[AnyRef, AnyRef] = {
    val size = in.readInt()
    val map = new java.util.HashMap[AnyRef, AnyRef](size)
    deserializeWithSize(in, map, size)
    map
  }

  override def deserialize(in: Input, map: java.util.Map[AnyRef, AnyRef]): Unit =
    deserializeWithSize(in, map, in.readInt())

  private def deserializeWithSize(in: Input, map: java.util.Map[AnyRef, AnyRef], size: Int): Unit = {
    var i = 0
    while (i < size) {
      val keyClass = in.readString()
      val key = read(in, baseClassLookups.getOrElse(keyClass, Class.forName(keyClass)))
      val valueClass = in.readString()
      val value = if (valueClass == nullMapping) { null } else {
        read(in, baseClassLookups.getOrElse(valueClass, Class.forName(valueClass)))
      }
      map.put(key, value)
      i += 1
    }
  }
}
