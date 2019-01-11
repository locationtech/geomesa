/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.serialization

import java.util.{Date, UUID}

import com.typesafe.scalalogging.LazyLogging
import org.geotools.factory.Hints
import org.locationtech.geomesa.utils.text.WKBUtils
import org.locationtech.jts.geom.Geometry

// noinspection LanguageFeature
trait GenericMapSerialization[T <: PrimitiveWriter, V <: PrimitiveReader] extends LazyLogging {

  def serialize(out: T, map: java.util.Map[AnyRef, AnyRef]): Unit

  def deserialize(in: V): java.util.Map[AnyRef, AnyRef]

  def deserialize(in: V, map: java.util.Map[AnyRef, AnyRef]): Unit

  protected def write(out: T, value: AnyRef): Unit = value match {
    case v: String                 => out.writeString(v)
    case v: java.lang.Integer      => out.writeInt(v)
    case v: java.lang.Long         => out.writeLong(v)
    case v: java.lang.Float        => out.writeFloat(v)
    case v: java.lang.Double       => out.writeDouble(v)
    case v: java.lang.Boolean      => out.writeBoolean(v)
    case v: Date                   => out.writeLong(v.getTime)
    case v: Array[Byte]            => writeBytes(out, v)
    case v: Geometry               => writeGeometry(out, v)
    case v: UUID                   => out.writeLong(v.getMostSignificantBits); out.writeLong(v.getLeastSignificantBits)
    case v: java.util.List[AnyRef] => writeList(out, v)
    case v: Hints.Key              => out.writeString(HintKeySerialization.keyToId(v))
    case _ => throw new IllegalArgumentException(s"Unsupported value: $value (${value.getClass})")
  }

  protected def read(in: V, clas: Class[_]): AnyRef = clas match {
    case c if classOf[java.lang.String].isAssignableFrom(c)  => in.readString()
    case c if classOf[java.lang.Integer].isAssignableFrom(c) => Int.box(in.readInt())
    case c if classOf[java.lang.Long].isAssignableFrom(c)    => Long.box(in.readLong())
    case c if classOf[java.lang.Float].isAssignableFrom(c)   => Float.box(in.readFloat())
    case c if classOf[java.lang.Double].isAssignableFrom(c)  => Double.box(in.readDouble())
    case c if classOf[java.lang.Boolean].isAssignableFrom(c) => Boolean.box(in.readBoolean())
    case c if classOf[java.util.Date].isAssignableFrom(c)    => new java.util.Date(in.readLong())
    case c if classOf[Array[Byte]] == c                      => readBytes(in)
    case c if classOf[Geometry].isAssignableFrom(c)          => readGeometry(in)
    case c if classOf[UUID].isAssignableFrom(c)              => new UUID(in.readLong(), in.readLong())
    case c if classOf[java.util.List[_]].isAssignableFrom(c) => readList(in)
    case c if classOf[Hints.Key].isAssignableFrom(c)         => HintKeySerialization.idToKey(in.readString())
    case _ => throw new IllegalArgumentException(s"Unsupported value class: $clas")
  }

  /**
    * Write a geometry
    *
    * @param out out
    * @param geom geometry
    */
  protected def writeGeometry(out: T, geom: Geometry): Unit = writeBytes(out, WKBUtils.write(geom))

  /**
    * Read a geometry
    *
    * @param in in
    * @return geometry
    */
  protected def readGeometry(in: V): Geometry = WKBUtils.read(readBytes(in))

  /**
    * Write bytes
    *
    * @param out out
    * @param bytes bytes
    */
  protected def writeBytes(out: T, bytes: Array[Byte]): Unit

  /**
    * Read bytes
    *
    * @param in in
    * @return bytes
    */
  protected def readBytes(in: V): Array[Byte]

  /**
    * Write a list
    *
    * @param out out
    * @param list list
    */
  protected def writeList(out: T, list: java.util.List[AnyRef]): Unit

  /**
    * Read a list
    *
    * @param in in
    * @return list
    */
  protected def readList(in: V): java.util.List[AnyRef]

  protected def canSerialize(obj: AnyRef): Boolean = obj match {
    case key: Hints.Key => HintKeySerialization.canSerialize(key)
    case _ => true
  }
}
