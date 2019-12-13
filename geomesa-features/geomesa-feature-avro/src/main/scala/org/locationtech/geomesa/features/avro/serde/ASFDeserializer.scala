/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.avro.serde

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.{Date, UUID}

import org.locationtech.jts.geom.Geometry
import org.apache.avro.io.Decoder
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.avro.{AvroSimpleFeature, AvroSimpleFeatureUtils}
import org.locationtech.geomesa.utils.cache.SoftThreadLocal
import org.locationtech.geomesa.utils.text.WKBUtils

/**
 * Trait that encapsulates the methods needed to deserialize
 * an AvroSimpleFeature
 */
trait ASFDeserializer {

  protected val buffers = new SoftThreadLocal[(ByteBuffer, Array[Byte])]

  @deprecated("Replaced with set(ScalaSimpleFeature, Int, Decoder)")
  def setString(sf: AvroSimpleFeature, field: String, in: Decoder): Unit = {
    var (bb, bytes) = buffers.getOrElseUpdate((ByteBuffer.allocate(16), Array.empty))
    bb = in.readBytes(bb)
    val length = bb.remaining
    if (bytes.length < length) {
      bytes = Array.ofDim(length)
    }
    buffers.put((bb, bytes))
    bb.get(bytes, 0, length)
    sf.setAttributeNoConvert(field, new String(bytes, 0, length, StandardCharsets.UTF_8))
  }

  @deprecated("Replaced with set(ScalaSimpleFeature, Int, Decoder)")
  def setInt(sf: AvroSimpleFeature, field: String, in: Decoder): Unit =
    sf.setAttributeNoConvert(field, in.readInt().asInstanceOf[Object])

  @deprecated("Replaced with set(ScalaSimpleFeature, Int, Decoder)")
  def setDouble(sf: AvroSimpleFeature, field: String, in: Decoder): Unit =
    sf.setAttributeNoConvert(field, in.readDouble().asInstanceOf[Object])

  @deprecated("Replaced with set(ScalaSimpleFeature, Int, Decoder)")
  def setLong(sf: AvroSimpleFeature, field: String, in: Decoder): Unit =
    sf.setAttributeNoConvert(field, in.readLong().asInstanceOf[Object])

  @deprecated("Replaced with set(ScalaSimpleFeature, Int, Decoder)")
  def setFloat(sf: AvroSimpleFeature, field: String, in: Decoder): Unit =
    sf.setAttributeNoConvert(field, in.readFloat().asInstanceOf[Object])

  @deprecated("Replaced with set(ScalaSimpleFeature, Int, Decoder)")
  def setBool(sf: AvroSimpleFeature, field: String, in: Decoder): Unit =
    sf.setAttributeNoConvert(field, in.readBoolean().asInstanceOf[Object])

  @deprecated("Replaced with set(ScalaSimpleFeature, Int, Decoder)")
  def setUUID(sf: AvroSimpleFeature, field: String, in: Decoder): Unit = {
    var (bb, bytes) = buffers.getOrElseUpdate((ByteBuffer.allocate(16), Array.empty))
    bb = in.readBytes(bb)
    buffers.put((bb, bytes))
    sf.setAttributeNoConvert(field, AvroSimpleFeatureUtils.decodeUUID(bb))
  }

  @deprecated("Replaced with set(ScalaSimpleFeature, Int, Decoder)")
  def setDate(sf: AvroSimpleFeature, field: String, in: Decoder): Unit =
    sf.setAttributeNoConvert(field, new Date(in.readLong()))

  @deprecated("Replaced with set(ScalaSimpleFeature, Int, Decoder)")
  def setList(sf: AvroSimpleFeature, field: String, in: Decoder): Unit = {
    var (bb, bytes) = buffers.getOrElseUpdate((ByteBuffer.allocate(16), Array.empty))
    bb = in.readBytes(bb)
    buffers.put((bb, bytes))
    sf.setAttributeNoConvert(field, AvroSimpleFeatureUtils.decodeList(bb))
  }

  @deprecated("Replaced with set(ScalaSimpleFeature, Int, Decoder)")
  def setMap(sf: AvroSimpleFeature, field: String, in: Decoder): Unit = {
    var (bb, bytes) = buffers.getOrElseUpdate((ByteBuffer.allocate(16), Array.empty))
    bb = in.readBytes(bb)
    buffers.put((bb, bytes))
    sf.setAttributeNoConvert(field, AvroSimpleFeatureUtils.decodeMap(bb))
  }

  @deprecated("Replaced with set(ScalaSimpleFeature, Int, Decoder)")
  def setBytes(sf: AvroSimpleFeature, field: String, in: Decoder): Unit = {
    var (bb, bytes) = buffers.getOrElseUpdate((ByteBuffer.allocate(16), Array.empty))
    bb = in.readBytes(bb)
    buffers.put((bb, bytes))
    val value = Array.ofDim[Byte](bb.remaining)
    bb.get(value)
    sf.setAttributeNoConvert(field, value)
  }

  @deprecated("Replaced with set(ScalaSimpleFeature, Int, Decoder)")
  def setGeometry(sf: AvroSimpleFeature, field: String, in: Decoder): Unit = {
    var (bb, bytes) = buffers.getOrElseUpdate((ByteBuffer.allocate(16), Array.empty))
    bb = in.readBytes(bb)
    val length = bb.remaining
    if (bytes.length < length) {
      bytes = Array.ofDim(length)
    }
    buffers.put((bb, bytes))
    bb.get(bytes, 0, length)
    // note: WKBReader ignores any bytes after the geom
    sf.setAttributeNoConvert(field, WKBUtils.read(bytes))
  }

  // note: we don't use the string reading methods, as internal avro state can get corrupted and cause
  // exceptions in BinaryDecoder.scratchUtf8
  def setString(sf: ScalaSimpleFeature, field: Int, in: Decoder): Unit = {
    var (bb, bytes) = buffers.getOrElseUpdate((ByteBuffer.allocate(16), Array.empty))
    bb = in.readBytes(bb)
    val length = bb.remaining
    if (bytes.length < length) {
      bytes = Array.ofDim(length)
    }
    buffers.put((bb, bytes))
    bb.get(bytes, 0, length)
    sf.setAttributeNoConvert(field, new String(bytes, 0, length, StandardCharsets.UTF_8))
  }

  // note: we don't use the string reading methods, as internal avro state can get corrupted and cause
  // exceptions in BinaryDecoder.scratchUtf8
  def consumeString(in: Decoder): Unit = in.skipBytes()

  def setInt(sf: ScalaSimpleFeature, field: Int, in: Decoder): Unit =
    sf.setAttributeNoConvert(field, Int.box(in.readInt()))

  def consumeInt(in: Decoder): Unit = in.readInt()

  def setDouble(sf: ScalaSimpleFeature, field: Int, in: Decoder): Unit =
    sf.setAttributeNoConvert(field, Double.box(in.readDouble()))

  def consumeDouble(in: Decoder): Unit = in.readDouble()

  def setLong(sf: ScalaSimpleFeature, field: Int, in: Decoder): Unit =
    sf.setAttributeNoConvert(field, Long.box(in.readLong()))

  def consumeLong(in: Decoder): Unit = in.readLong()

  def setFloat(sf: ScalaSimpleFeature, field: Int, in: Decoder): Unit =
    sf.setAttributeNoConvert(field, Float.box(in.readFloat()))

  def consumeFloat(in: Decoder): Unit = in.readFloat()

  def setBool(sf: ScalaSimpleFeature, field: Int, in: Decoder): Unit =
    sf.setAttributeNoConvert(field, Boolean.box(in.readBoolean()))

  def consumeBool(in: Decoder): Unit = in.readBoolean()

  def setUUID(sf: ScalaSimpleFeature, field: Int, in: Decoder): Unit = {
    var (bb, bytes) = buffers.getOrElseUpdate((ByteBuffer.allocate(16), Array.empty))
    bb = in.readBytes(bb)
    buffers.put((bb, bytes))
    sf.setAttributeNoConvert(field, AvroSimpleFeatureUtils.decodeUUID(bb))
  }

  def consumeUUID(in: Decoder): Unit = in.skipBytes()

  def setDate(sf: ScalaSimpleFeature, field: Int, in: Decoder): Unit =
    sf.setAttributeNoConvert(field, new Date(in.readLong()))

  def consumeDate(in: Decoder): Unit = in.readLong()

  def setList(sf: ScalaSimpleFeature, field: Int, in: Decoder): Unit = {
    var (bb, bytes) = buffers.getOrElseUpdate((ByteBuffer.allocate(16), Array.empty))
    bb = in.readBytes(bb)
    buffers.put((bb, bytes))
    sf.setAttributeNoConvert(field, AvroSimpleFeatureUtils.decodeList(bb))
  }

  def consumeList(in: Decoder): Unit = in.skipBytes()

  def setMap(sf: ScalaSimpleFeature, field: Int, in: Decoder): Unit = {
    var (bb, bytes) = buffers.getOrElseUpdate((ByteBuffer.allocate(16), Array.empty))
    bb = in.readBytes(bb)
    buffers.put((bb, bytes))
    sf.setAttributeNoConvert(field, AvroSimpleFeatureUtils.decodeMap(bb))
  }

  def consumeMap(in: Decoder): Unit = in.skipBytes()

  def setBytes(sf: ScalaSimpleFeature, field: Int, in: Decoder): Unit = {
    var (bb, bytes) = buffers.getOrElseUpdate((ByteBuffer.allocate(16), Array.empty))
    bb = in.readBytes(bb)
    buffers.put((bb, bytes))
    val value = Array.ofDim[Byte](bb.remaining)
    bb.get(value)
    sf.setAttributeNoConvert(field, value)
  }

  def consumeBytes(in: Decoder): Unit = in.skipBytes()

  def setGeometry(sf: ScalaSimpleFeature, field: Int, in: Decoder): Unit = {
    var (bb, bytes) = buffers.getOrElseUpdate((ByteBuffer.allocate(16), Array.empty))
    bb = in.readBytes(bb)
    val length = bb.remaining
    if (bytes.length < length) {
      bytes = Array.ofDim(length)
    }
    buffers.put((bb, bytes))
    bb.get(bytes, 0, length)
    // note: WKBReader ignores any bytes after the geom
    sf.setAttributeNoConvert(field, WKBUtils.read(bytes))
  }

  def consumeGeometry(in: Decoder): Unit = in.skipBytes()

  @deprecated("Replaced with consume(Decoder)")
  def buildConsumeFunction(cls: Class[_]): Decoder => AnyVal = cls match {
    case c if classOf[java.lang.String].isAssignableFrom(cls)    => (in: Decoder) => in.skipString()
    case c if classOf[java.lang.Integer].isAssignableFrom(cls)   => (in: Decoder) => in.readInt()
    case c if classOf[java.lang.Long].isAssignableFrom(cls)      => (in: Decoder) => in.readLong()
    case c if classOf[java.lang.Double].isAssignableFrom(cls)    => (in: Decoder) => in.readDouble()
    case c if classOf[java.lang.Float].isAssignableFrom(cls)     => (in: Decoder) => in.readFloat()
    case c if classOf[java.lang.Boolean].isAssignableFrom(cls)   => (in: Decoder) => in.readBoolean()
    case c if classOf[UUID].isAssignableFrom(cls)                => (in: Decoder) => in.skipBytes()
    case c if classOf[Date].isAssignableFrom(cls)                => (in: Decoder) => in.readLong()
    case c if classOf[Geometry].isAssignableFrom(cls)            => (in: Decoder) => consumeGeometry(in)
    case c if classOf[java.util.List[_]].isAssignableFrom(cls)   => (in: Decoder) => in.skipBytes()
    case c if classOf[java.util.Map[_, _]].isAssignableFrom(cls) => (in: Decoder) => in.skipBytes()
    case c if classOf[Array[Byte]].isAssignableFrom(cls)         => (in: Decoder) => in.skipBytes()
  }

}
