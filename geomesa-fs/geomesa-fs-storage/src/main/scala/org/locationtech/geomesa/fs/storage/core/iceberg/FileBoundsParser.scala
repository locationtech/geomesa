/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.iceberg

import org.apache.iceberg.types.{Conversions, Type, Types}
import org.geotools.api.feature.`type`.AttributeDescriptor
import org.locationtech.geomesa.fs.storage.core.parquet.schema.GeometrySchema.GeometryEncoding
import org.locationtech.geomesa.utils.geotools.ObjectType
import org.locationtech.geomesa.utils.text.WKBUtils
import org.locationtech.jts.geom.Geometry

import java.nio.ByteBuffer
import java.time.Instant
import java.util.Date

/**
 * Parse a value out of an iceberg column stat
 */
trait FileBoundsParser[T] {

  def apply(buffer: ByteBuffer): T

  protected def parse[U](fieldType: Type, buffer: ByteBuffer): U = Conversions.fromByteBuffer[U](fieldType, buffer)
}

object FileBoundsParser {

  import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

  def apply[T](descriptor: AttributeDescriptor, fieldType: Type): Option[FileBoundsParser[T]] = {
    val types = ObjectType.selectType(descriptor)
    val parser = types.head match {
      case ObjectType.STRING if descriptor.getJsonSchema().isEmpty => Some(StringParser)

      case ObjectType.DATE  => Some(DateParser)
      case ObjectType.BYTES => Some(BytesParser)

      case ObjectType.GEOMETRY =>
        val encoding = descriptor.getUserData.get(SimpleFeatureIcebergSchema.GeometryEncodingKey) match {
          case e: String => GeometryEncoding(e)
          case _ => GeometryEncoding.GeoParquetWkb
        }
        if (encoding != GeometryEncoding.GeoParquetWkb) {
          throw new UnsupportedOperationException(encoding.toString)
        }
        Some(WkbParser)

      case ObjectType.LIST | ObjectType.MAP | ObjectType.STRING => None

      case _ => Some(new GenericParser(fieldType))
    }

    parser.asInstanceOf[Option[FileBoundsParser[T]]]
  }

  private object WkbParser extends FileBoundsParser[Geometry] {
    override def apply(buffer: ByteBuffer): Geometry = {
      val parsed = parse[ByteBuffer](Types.BinaryType.get(), buffer)
      if (parsed == null) { null } else {
        val buf = Array.ofDim[Byte](parsed.remaining())
        parsed.get(buf, 0, buf.length)
        WKBUtils.read(buf)
      }
    }
  }

  private object StringParser extends FileBoundsParser[String] {
    override def apply(buffer: ByteBuffer): String = {
      val charseq = parse[CharSequence](Types.StringType.get(), buffer)
      if (charseq == null) { null } else { charseq.toString }
    }
  }

  private object DateParser extends FileBoundsParser[Date] {
    override def apply(buffer: ByteBuffer): Date = {
      val micros = parse[java.lang.Long](Types.LongType.get(), buffer)
      if (micros == null) { null } else { Date.from(Instant.ofEpochMilli(micros / 1000L)) }
    }
  }

  private object BytesParser extends FileBoundsParser[Array[Byte]] {
    override def apply(buffer: ByteBuffer): Array[Byte] = {
      val parsed = parse[ByteBuffer](Types.BinaryType.get(), buffer)
      val buf = Array.ofDim[Byte](parsed.remaining())
      parsed.get(buf, 0, buf.length)
      buf
    }
  }

  private class GenericParser(fieldType: Type) extends FileBoundsParser[AnyRef] {
    override def apply(buffer: ByteBuffer): AnyRef = parse(fieldType, buffer)
  }
}
