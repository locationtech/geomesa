/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.kryo.serialization

import java.util.{Date, UUID}

import com.esotericsoftware.kryo.io.{Input, Output}
import com.typesafe.scalalogging.LazyLogging
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.features.serialization.HintKeySerialization
import org.locationtech.jts.geom.{Geometry, LineString, Point, Polygon}

import scala.util.control.NonFatal

object KryoUserDataSerialization extends LazyLogging {

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
    classOf[Array[Byte]]       -> "$B",
    classOf[UUID]              -> "$u",
    classOf[Point]             -> "$pt",
    classOf[LineString]        -> "$ls",
    classOf[Polygon]           -> "$pl",
    classOf[Hints.Key]         -> "$h"
  )

  private val baseClassLookups: Map[String, Class[_]] = {
    val m1 = baseClassMappings.filterNot(_._1.isPrimitive).map(_.swap)
    // support hints generated with geotools versions <= 20
    val m2 = m1 + ("org.geotools.factory.Hints$Key" -> classOf[Hints.Key])
    m2
  }

  private implicit val ordering: Ordering[(AnyRef, AnyRef)] = Ordering.by(_._1.toString)

  def serialize(out: Output, javaMap: java.util.Map[_ <: AnyRef, _ <: AnyRef]): Unit = {
    import scala.collection.JavaConverters._

    // write in sorted order to keep consistent output
    val toWrite = scala.collection.mutable.SortedSet.empty[(AnyRef, AnyRef)]

    javaMap.asScala.foreach { case (k, v) =>
      if (k != null && !k.isInstanceOf[Hints.Key]) { toWrite += k -> v } else {
        logger.warn(s"Skipping serialization of entry: $k -> $v")
      }
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

  def deserialize(in: Input): java.util.Map[AnyRef, AnyRef] = {
    try {
      val size = in.readInt()
      val map = new java.util.HashMap[AnyRef, AnyRef](size)
      deserializeWithSize(in, map, size)
      map
    } catch {
      case NonFatal(e) =>
        logger.error("Error reading serialized kryo user data:", e)
        new java.util.HashMap[AnyRef, AnyRef]()
    }
  }

  def deserialize(in: Input, map: java.util.Map[AnyRef, AnyRef]): Unit = {
    try {
      deserializeWithSize(in, map, in.readInt())
    } catch {
      case NonFatal(e) =>
        logger.error("Error reading serialized kryo user data:", e)
        new java.util.HashMap[AnyRef, AnyRef]()
    }
  }

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

  private def write(out: Output, value: AnyRef): Unit = value match {
    case v: String                 => out.writeString(v)
    case v: java.lang.Integer      => out.writeInt(v)
    case v: java.lang.Long         => out.writeLong(v)
    case v: java.lang.Float        => out.writeFloat(v)
    case v: java.lang.Double       => out.writeDouble(v)
    case v: java.lang.Boolean      => out.writeBoolean(v)
    case v: Date                   => out.writeLong(v.getTime)
    case v: Array[Byte]            => writeBytes(out, v)
    case v: Geometry               => KryoGeometrySerialization.serializeWkb(out, v)
    case v: UUID                   => out.writeLong(v.getMostSignificantBits); out.writeLong(v.getLeastSignificantBits)
    case v: java.util.List[AnyRef] => writeList(out, v)
    case _ => throw new IllegalArgumentException(s"Unsupported value: $value (${value.getClass})")
  }

  /**
   * Read a key or value. Strings will be interned, as we expect a lot of duplication in user data,
   * i.e keys but also visibilities, which is the only user data we generally store
   *
   * @param in input
   * @param clas class of the item to read
   * @return
   */
  private def read(in: Input, clas: Class[_]): AnyRef = clas match {
    case c if classOf[java.lang.String].isAssignableFrom(c)  => in.readString().intern()
    case c if classOf[java.lang.Integer].isAssignableFrom(c) => Int.box(in.readInt())
    case c if classOf[java.lang.Long].isAssignableFrom(c)    => Long.box(in.readLong())
    case c if classOf[java.lang.Float].isAssignableFrom(c)   => Float.box(in.readFloat())
    case c if classOf[java.lang.Double].isAssignableFrom(c)  => Double.box(in.readDouble())
    case c if classOf[java.lang.Boolean].isAssignableFrom(c) => Boolean.box(in.readBoolean())
    case c if classOf[java.util.Date].isAssignableFrom(c)    => new java.util.Date(in.readLong())
    case c if classOf[Array[Byte]] == c                      => readBytes(in)
    case c if classOf[Geometry].isAssignableFrom(c)          => KryoGeometrySerialization.deserializeWkb(in, checkNull = true)
    case c if classOf[UUID].isAssignableFrom(c)              => new UUID(in.readLong(), in.readLong())
    case c if classOf[java.util.List[_]].isAssignableFrom(c) => readList(in)
    case c if classOf[Hints.Key].isAssignableFrom(c)         => HintKeySerialization.idToKey(in.readString())
    case _ => throw new IllegalArgumentException(s"Unsupported value class: $clas")
  }

  private def writeBytes(out: Output, bytes: Array[Byte]): Unit = {
    out.writeInt(bytes.length)
    out.writeBytes(bytes)
  }

  private def readBytes(in: Input): Array[Byte] = {
    val bytes = Array.ofDim[Byte](in.readInt)
    in.readBytes(bytes)
    bytes
  }

  private def writeList(out: Output, list: java.util.List[AnyRef]): Unit = {
    out.writeInt(list.size)
    val iterator = list.iterator()
    while (iterator.hasNext) {
      val value = iterator.next()
      if (value == null) {
        out.writeString(nullMapping)
      } else {
        out.writeString(baseClassMappings.getOrElse(value.getClass, value.getClass.getName))
        write(out, value)
      }
    }
  }

  private def readList(in: Input): java.util.List[AnyRef] = {
    val size = in.readInt()
    val list = new java.util.ArrayList[AnyRef](size)
    var i = 0
    while (i < size) {
      val clas = in.readString()
      if (clas == nullMapping) { list.add(null) } else {
        list.add(read(in, baseClassLookups.getOrElse(clas, Class.forName(clas))))
      }
      i += 1
    }
    list
  }
}
