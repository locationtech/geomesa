/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.avro.serialization

import java.util.{Date, UUID}

import com.typesafe.scalalogging.LazyLogging
import org.apache.avro.io.{Decoder, Encoder}
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.features.serialization.HintKeySerialization
import org.locationtech.geomesa.utils.text.WKBUtils
import org.locationtech.jts.geom.Geometry

@deprecated("does not match declared schema")
object AvroUserDataSerializationV4 extends LazyLogging {

  import scala.collection.JavaConverters._

  val NullMarkerString = "<null>"

  def serialize(out: Encoder, map: java.util.Map[_ <: AnyRef, _ <: AnyRef]): Unit = {
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

  def deserialize(in: Decoder): java.util.Map[AnyRef, AnyRef] = {
    val size = in.readArrayStart().toInt
    val map = new java.util.HashMap[AnyRef, AnyRef](size)
    deserializeWithSize(in, size, map)
    map
  }

  def deserialize(in: Decoder, map: java.util.Map[AnyRef, AnyRef]): Unit = {
    deserializeWithSize(in, in.readArrayStart().toInt, map)
  }

  private def deserializeWithSize(in: Decoder, size: Int, map: java.util.Map[AnyRef, AnyRef]): Unit = {
    var remaining = size
    while (remaining > 0) {
      val keyClass = in.readString()
      val key =
        if (keyClass == NullMarkerString) { null }
        else if (keyClass == "org.geotools.factory.Hints$Key") { HintKeySerialization.idToKey(in.readString()) }
        else { read(in, Class.forName(keyClass)) }
      val valueClass = in.readString()
      val value = if (valueClass == NullMarkerString) { null } else { read(in, Class.forName(valueClass))}
      map.put(key, value)
      remaining -= 1
      if (remaining == 0) {
        remaining = in.arrayNext().toInt
      }
    }
  }

  private def write(out: Encoder, value: AnyRef): Unit = value match {
    case v: String                 => out.writeString(v)
    case v: java.lang.Integer      => out.writeInt(v)
    case v: java.lang.Long         => out.writeLong(v)
    case v: java.lang.Float        => out.writeFloat(v)
    case v: java.lang.Double       => out.writeDouble(v)
    case v: java.lang.Boolean      => out.writeBoolean(v)
    case v: Date                   => out.writeLong(v.getTime)
    case v: Array[Byte]            => out.writeBytes(v)
    case v: Geometry               => out.writeBytes(WKBUtils.write(v))
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
  private def read(in: Decoder, clas: Class[_]): AnyRef = clas match {
    case c if classOf[java.lang.String].isAssignableFrom(c)  => in.readString().intern()
    case c if classOf[java.lang.Integer].isAssignableFrom(c) => Int.box(in.readInt())
    case c if classOf[java.lang.Long].isAssignableFrom(c)    => Long.box(in.readLong())
    case c if classOf[java.lang.Float].isAssignableFrom(c)   => Float.box(in.readFloat())
    case c if classOf[java.lang.Double].isAssignableFrom(c)  => Double.box(in.readDouble())
    case c if classOf[java.lang.Boolean].isAssignableFrom(c) => Boolean.box(in.readBoolean())
    case c if classOf[java.util.Date].isAssignableFrom(c)    => new java.util.Date(in.readLong())
    case c if classOf[Array[Byte]] == c                      => readBytes(in)
    case c if classOf[Geometry].isAssignableFrom(c)          => WKBUtils.read(readBytes(in))
    case c if classOf[UUID].isAssignableFrom(c)              => new UUID(in.readLong(), in.readLong())
    case c if classOf[java.util.List[_]].isAssignableFrom(c) => readList(in)
    case c if classOf[Hints.Key].isAssignableFrom(c)         => HintKeySerialization.idToKey(in.readString())
    case _ => throw new IllegalArgumentException(s"Unsupported value class: $clas")
  }

  private def readBytes(in: Decoder): Array[Byte] = {
    val buffer = in.readBytes(null)
    val bytes = Array.ofDim[Byte](buffer.remaining())
    buffer.get(bytes)
    bytes
  }

  private def writeList(out: Encoder, list: java.util.List[AnyRef]): Unit = {
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

  private def readList(in: Decoder): java.util.List[AnyRef] = {
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

  private def canSerialize(obj: AnyRef): Boolean = obj match {
    case key: Hints.Key => HintKeySerialization.canSerialize(key)
    case _ => true
  }
}
