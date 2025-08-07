/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.parquet.io

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.conf.ParquetConfiguration
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.hadoop.api.WriteSupport.{FinalizedWriteContext, WriteContext}
import org.apache.parquet.io.api.{Binary, RecordConsumer}
import org.geotools.api.feature.`type`.AttributeDescriptor
import org.geotools.api.feature.simple.SimpleFeature
import org.locationtech.geomesa.fs.storage.parquet.io.GeoParquetMetadata.GeoParquetObserver
import org.locationtech.geomesa.fs.storage.parquet.io.GeometrySchema.{GeometryColumnX, GeometryColumnY, GeometryEncoding}
import org.locationtech.geomesa.security.SecurityUtils
import org.locationtech.geomesa.utils.geotools.ObjectType
import org.locationtech.geomesa.utils.geotools.ObjectType.ObjectType
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.locationtech.geomesa.utils.text.{StringSerialization, WKBUtils}
import org.locationtech.jts.geom._

import java.nio.ByteBuffer
import java.util.{Date, UUID}

class SimpleFeatureWriteSupport extends WriteSupport[SimpleFeature] {

  private var writer: SimpleFeatureWriteSupport.SimpleFeatureWriter = _
  private var consumer: RecordConsumer = _
  private var geoParquetObserver: GeoParquetObserver = _
  private var baseMetadata: java.util.Map[String, String] = _

  override val getName: String = "SimpleFeatureWriteSupport"

  // called once
  override def init(conf: Configuration): WriteContext = {
    val schema = SimpleFeatureParquetSchema.write(conf).getOrElse {
      throw new IllegalArgumentException("Could not extract SimpleFeatureType from write context")
    }
    init(schema)
  }

  override def init(conf: ParquetConfiguration): WriteContext = {
    val schema = SimpleFeatureParquetSchema.write(conf).getOrElse {
      throw new IllegalArgumentException("Could not extract SimpleFeatureType from write context")
    }
    init(schema)
  }

  private def init(schema: SimpleFeatureParquetSchema): WriteContext = {
    this.writer = new SimpleFeatureWriteSupport.SimpleFeatureWriter(schema)
    this.geoParquetObserver = new GeoParquetObserver(schema)
    this.baseMetadata = schema.metadata
    new WriteContext(schema.schema, schema.metadata)
  }

  // called per block
  override def prepareForWrite(recordConsumer: RecordConsumer): Unit = consumer = recordConsumer

  // called per row
  override def write(record: SimpleFeature): Unit = {
    writer.write(consumer, record)
    geoParquetObserver.write(record)
  }

  // called once at the end
  override def finalizeWrite(): FinalizedWriteContext = {
    try {
      val metadata = new java.util.HashMap[String, String]()
      metadata.putAll(baseMetadata)
      metadata.putAll(geoParquetObserver.metadata())
      new FinalizedWriteContext(metadata)
    } finally {
      CloseWithLogging(geoParquetObserver)
    }
  }
}

object SimpleFeatureWriteSupport {

  import StringSerialization.alphaNumericSafeString

  private class SimpleFeatureWriter(schema: SimpleFeatureParquetSchema) {

    private val attributes =
      Array.tabulate(schema.sft.getAttributeCount)(i => attribute(schema.sft.getDescriptor(i), i).asInstanceOf[AttributeWriter[AnyRef]])
    private val fids = new FidWriter(attributes.length) // put the ID at the end of the record
    private val vis = if (schema.hasVisibilities) { new VisibilityWriter(attributes.length + 1) } else { null }
    private val bboxes = schema.boundingBoxes.zipWithIndex.map { case (field, bi) =>
      val i = schema.sft.indexOf(StringSerialization.decodeAlphaNumericSafeString(field.geometry))
      i -> new BBoxWriter(field.bbox, attributes.length + 1 + Option(vis).size + bi)
    }

    def write(consumer: RecordConsumer, value: SimpleFeature): Unit = {
      consumer.startMessage()
      var i = 0
      while (i < attributes.length) {
        attributes(i).apply(consumer, value.getAttribute(i))
        i += 1
      }
      fids.apply(consumer, value.getID)
      if (vis != null) {
        vis.apply(consumer, SecurityUtils.getVisibility(value))
      }
      bboxes.foreach { case (i, writer) =>
        writer.apply(consumer, value.getAttribute(i).asInstanceOf[Geometry])
      }
      consumer.endMessage()
    }

    private def attribute(descriptor: AttributeDescriptor, index: Int): AttributeWriter[_] =
      attribute(alphaNumericSafeString(descriptor.getLocalName), index, ObjectType.selectType(descriptor))

    private def attribute(name: String, index: Int, bindings: Seq[ObjectType]): AttributeWriter[_] = {
      bindings.head match {
        case ObjectType.GEOMETRY => geometry(name, index, bindings.last)
        case ObjectType.DATE     => date(name, index)
        case ObjectType.STRING   => new StringWriter(name, index)
        case ObjectType.INT      => new IntegerWriter(name, index)
        case ObjectType.LONG     => new LongWriter(name, index)
        case ObjectType.FLOAT    => new FloatWriter(name, index)
        case ObjectType.DOUBLE   => new DoubleWriter(name, index)
        case ObjectType.BYTES    => new BytesWriter(name, index)
        case ObjectType.LIST     => list(name, index, attribute("element", 0, bindings.drop(1)))
        case ObjectType.MAP      => new MapWriter(name, index, attribute("key", 0, bindings.slice(1, 2)), attribute("value", 1, bindings.slice(2, 3)))
        case ObjectType.BOOLEAN  => new BooleanWriter(name, index)
        case ObjectType.UUID     => new UuidWriter(name, index)
        case _ => throw new IllegalArgumentException(s"Can't serialize field '$name' of type ${bindings.head}")
      }
    }

    // TODO support z/m
    private def geometry(name: String, index: Int, binding: ObjectType): AttributeWriter[_] = {
      if (schema.encodings.geometry == GeometryEncoding.GeoParquetWkb) {
        new WkbWriter(name, index)
      } else {
        val native = schema.encodings.geometry == GeometryEncoding.GeoParquetNative
        binding match {
          case ObjectType.POINT                     => new PointWriter(name, index)
          case ObjectType.LINESTRING      if native => new GeoParquetNativeLineStringWriter(name, index)
          case ObjectType.LINESTRING                => new LineStringWriter(name, index)
          case ObjectType.POLYGON         if native => new GeoParquetNativePolygonWriter(name, index)
          case ObjectType.POLYGON                   => new PolygonWriter(name, index)
          case ObjectType.MULTIPOINT      if native => new GeoParquetNativeMultiPointWriter(name, index)
          case ObjectType.MULTIPOINT                => new MultiPointWriter(name, index)
          case ObjectType.MULTILINESTRING if native => new GeoParquetNativeMultiLineStringWriter(name, index)
          case ObjectType.MULTILINESTRING           => new MultiLineStringWriter(name, index)
          case ObjectType.MULTIPOLYGON    if native => new GeoParquetNativeMultiPolygonWriter(name, index)
          case ObjectType.MULTIPOLYGON              => new MultiPolygonWriter(name, index)
          case ObjectType.GEOMETRY_COLLECTION       => new WkbWriter(name, index)
          case ObjectType.GEOMETRY                  => new WkbWriter(name, index)
          case _ => throw new IllegalArgumentException(s"Can't serialize field '$name' of type $binding")
        }
      }
    }

    private def list(name: String, index: Int, elements: AttributeWriter[_]): AttributeWriter[_] = {
      schema.encodings.list match {
        case ListEncoding.ThreeLevel => new ListWriter(name, index, elements)
        case ListEncoding.TwoLevel   => new TwoLevelListWriter(name, index, elements)
        case encoding => throw new UnsupportedOperationException(encoding.toString)
      }
    }

    private def date(name: String, index: Int): AttributeWriter[_] = {
      schema.encodings.date match {
        case DateEncoding.Millis => new DateMillisWriter(name, index)
        case DateEncoding.Micros => new DateMicrosWriter(name, index)
        case encoding => throw new UnsupportedOperationException(encoding.toString)
      }
    }
  }

  /**
    * Writes a simple feature attribute to a Parquet file
    */
  private abstract class AttributeWriter[T <: Any](name: String, index: Int) {

    /**
      * Writes a value to the current record
      *
      * @param consumer the Parquet record consumer
      * @param value value to write
      */
    def apply(consumer: RecordConsumer, value: T): Unit = {
      if (value != null) {
        consumer.startField(name, index)
        writeFields(consumer, value)
        consumer.endField(name, index)
      }
    }

    def writeFields(consumer: RecordConsumer, value: T): Unit
  }

  private class FidWriter(index: Int) extends AttributeWriter[String](SimpleFeatureParquetSchema.FeatureIdField, index) {
    override def writeFields(consumer: RecordConsumer, value: String): Unit =
      consumer.addBinary(Binary.fromString(value))
  }

  private class VisibilityWriter(index: Int) extends AttributeWriter[String](SimpleFeatureParquetSchema.VisibilitiesField, index) {
    override def writeFields(consumer: RecordConsumer, value: String): Unit =
      consumer.addBinary(Binary.fromString(value))
  }

  private class DateMillisWriter(name: String, index: Int) extends AttributeWriter[Date](name, index) {
    override def writeFields(consumer: RecordConsumer, value: Date): Unit =
      consumer.addLong(value.getTime)
  }

  private class DateMicrosWriter(name: String, index: Int) extends AttributeWriter[Date](name, index) {
    override def writeFields(consumer: RecordConsumer, value: Date): Unit =
      consumer.addLong(value.getTime * 1000L)
  }

  private class DoubleWriter(name: String, index: Int) extends AttributeWriter[java.lang.Double](name, index) {
    override def writeFields(consumer: RecordConsumer, value: java.lang.Double): Unit =
      consumer.addDouble(value)
  }

  private class FloatWriter(name: String, index: Int) extends AttributeWriter[java.lang.Float](name, index) {
    override def writeFields(consumer: RecordConsumer, value: java.lang.Float): Unit =
      consumer.addFloat(value)
  }

  private class IntegerWriter(name: String, index: Int) extends AttributeWriter[java.lang.Integer](name, index) {
    override def writeFields(consumer: RecordConsumer, value: java.lang.Integer): Unit =
      consumer.addInteger(value)
  }

  private class LongWriter(name: String, index: Int) extends AttributeWriter[java.lang.Long](name, index) {
    override def writeFields(consumer: RecordConsumer, value: java.lang.Long): Unit =
      consumer.addLong(value)
  }

  private class StringWriter(name: String, index: Int) extends AttributeWriter[String](name, index) {
    override def writeFields(consumer: RecordConsumer, value: String): Unit =
      consumer.addBinary(Binary.fromString(value))
  }

  private class BytesWriter(name: String, index: Int) extends AttributeWriter[Array[Byte]](name, index) {
    override def writeFields(consumer: RecordConsumer, value: Array[Byte]): Unit =
      consumer.addBinary(Binary.fromConstantByteArray(value))
  }

  private class BooleanWriter(name: String, index: Int) extends AttributeWriter[java.lang.Boolean](name, index) {
    override def writeFields(consumer: RecordConsumer, value: java.lang.Boolean): Unit =
      consumer.addBoolean(value)
  }

  private class ListWriter[T <: Any](name: String, index: Int, elements: AttributeWriter[T])
      extends AttributeWriter[java.util.List[T]](name, index) {

    override def writeFields(consumer: RecordConsumer, value: java.util.List[T]): Unit = {
      consumer.startGroup()
      if (!value.isEmpty) {
        consumer.startField("list", 0)
        consumer.startGroup()
        consumer.startField("element", 0)
        val iter = value.iterator
        while (iter.hasNext) {
          consumer.startGroup()
          val item = iter.next
          if (item != null) {
            elements(consumer, item)
          }
          consumer.endGroup()
        }
        consumer.endField("element", 0)
        consumer.endGroup()
        consumer.endField("list", 0)
      }
      consumer.endGroup()
    }
  }

  private class TwoLevelListWriter[T <: Any](name: String, index: Int, elements: AttributeWriter[T])
    extends AttributeWriter[java.util.List[T]](name, index) {

    override def writeFields(consumer: RecordConsumer, value: java.util.List[T]): Unit = {
      consumer.startGroup()
      if (!value.isEmpty) {
        consumer.startField("list", 0)
        val iter = value.iterator
        while (iter.hasNext) {
          consumer.startGroup()
          val item = iter.next
          if (item != null) {
            elements(consumer, item)
          }
          consumer.endGroup()
        }
        consumer.endField("list", 0)
      }
      consumer.endGroup()
    }
  }

  private class MapWriter[U <: Any, V <: Any](name: String, index: Int, keys: AttributeWriter[U], values: AttributeWriter[V])
      extends AttributeWriter[java.util.Map[U, V]](name, index) {
    override def writeFields(consumer: RecordConsumer, value: java.util.Map[U, V]): Unit = {
      consumer.startGroup()
      if (!value.isEmpty) {
        consumer.startField("key_value", 0)
        val iter = value.entrySet().iterator
        while (iter.hasNext) {
          val entry = iter.next()
          consumer.startGroup()
          keys(consumer, entry.getKey)
          val v = entry.getValue
          if (v != null) {
            values(consumer, v)
          }
          consumer.endGroup()
        }
        consumer.endField("key_value", 0)
      }
      consumer.endGroup()
    }
  }

  private class UuidWriter(name: String, index: Int) extends AttributeWriter[UUID](name, index) {
    override def writeFields(consumer: RecordConsumer, value: UUID): Unit = {
      val bb = ByteBuffer.wrap(new Array[Byte](16))
      bb.putLong(value.getMostSignificantBits)
      bb.putLong(value.getLeastSignificantBits)
      bb.rewind()
      consumer.addBinary(Binary.fromConstantByteBuffer(bb))
    }
  }

  private class PointWriter(name: String, index: Int) extends AttributeWriter[Point](name, index) {
    override def writeFields(consumer: RecordConsumer, value: Point): Unit = writeFields(consumer, value.getCoordinate)
    def writeFields(consumer: RecordConsumer, value: Coordinate): Unit = {
      consumer.startGroup()
      consumer.startField(GeometryColumnX, 0)
      consumer.addDouble(value.getX)
      consumer.endField(GeometryColumnX, 0)
      consumer.startField(GeometryColumnY, 1)
      consumer.addDouble(value.getY)
      consumer.endField(GeometryColumnY, 1)
      consumer.endGroup()
    }
  }

  private class LineStringWriter(name: String, index: Int) extends AttributeWriter[LineString](name, index) {
    override def writeFields(consumer: RecordConsumer, value: LineString): Unit = {
      consumer.startGroup()
      consumer.startField(GeometryColumnX, 0)
      var i = 0
      while (i < value.getNumPoints) {
        consumer.addDouble(value.getCoordinateN(i).x)
        i += 1
      }
      consumer.endField(GeometryColumnX, 0)
      consumer.startField(GeometryColumnY, 1)
      i = 0
      while (i < value.getNumPoints) {
        consumer.addDouble(value.getCoordinateN(i).y)
        i += 1
      }
      consumer.endField(GeometryColumnY, 1)
      consumer.endGroup()
    }
  }

  private class GeoParquetNativeLineStringWriter(name: String, index: Int) extends AttributeWriter[LineString](name, index) {
    private val pointWriter = new PointWriter("", -1)
    override def writeFields(consumer: RecordConsumer, value: LineString): Unit = {
      consumer.startGroup()
      consumer.startField("list", 0)
      consumer.startGroup()
      consumer.startField("element", 0)
      var i = 0
      while (i < value.getNumPoints) {
        val pt = value.getCoordinateN(i)
        pointWriter.writeFields(consumer, pt)
        i += 1
      }
      consumer.endField("element", 0)
      consumer.endGroup()
      consumer.endField("list", 0)
      consumer.endGroup()
    }
  }

  private class MultiPointWriter(name: String, index: Int) extends AttributeWriter[MultiPoint](name, index) {
    override def writeFields(consumer: RecordConsumer, value: MultiPoint): Unit = {
      consumer.startGroup()
      consumer.startField(GeometryColumnX, 0)
      var i = 0
      while (i < value.getNumPoints) {
        consumer.addDouble(value.getGeometryN(i).asInstanceOf[Point].getX)
        i += 1
      }
      consumer.endField(GeometryColumnX, 0)
      consumer.startField(GeometryColumnY, 1)
      i = 0
      while (i < value.getNumPoints) {
        consumer.addDouble(value.getGeometryN(i).asInstanceOf[Point].getY)
        i += 1
      }
      consumer.endField(GeometryColumnY, 1)
      consumer.endGroup()
    }
  }

  private class GeoParquetNativeMultiPointWriter(name: String, index: Int) extends AttributeWriter[MultiPoint](name, index) {
    private val pointWriter = new PointWriter("", -1)
    override def writeFields(consumer: RecordConsumer, value: MultiPoint): Unit = {
      consumer.startGroup()
      consumer.startField("list", 0)
      consumer.startGroup()
      consumer.startField("element", 0)
      var i = 0
      while (i < value.getNumGeometries) {
        val pt = value.getGeometryN(i).asInstanceOf[Point]
        pointWriter.writeFields(consumer, pt)
        i += 1
      }
      consumer.endField("element", 0)
      consumer.endGroup()
      consumer.endField("list", 0)
      consumer.endGroup()
    }
  }

  private trait HasLines[T <: Geometry] {
    protected def lines(value: T): Seq[LineString]
  }

  private trait PolygonHasLines extends HasLines[Polygon] {
    override protected def lines(value: Polygon): Seq[LineString] = {
      Seq.tabulate(value.getNumInteriorRing + 1) { i =>
        if (i == 0) { value.getExteriorRing } else { value.getInteriorRingN(i - 1) }
      }
    }
  }

  private trait MultiLineStringHasLines extends HasLines[MultiLineString] {
    override protected def lines(value: MultiLineString): Seq[LineString] =
      Seq.tabulate(value.getNumGeometries)(i => value.getGeometryN(i).asInstanceOf[LineString])
  }

  private abstract class AbstractLinesWriter[T <: Geometry](name: String, index: Int)
      extends AttributeWriter[T](name, index) with HasLines[T] {
    override def writeFields(consumer: RecordConsumer, value: T): Unit = {
      val lines = this.lines(value)
      consumer.startGroup()

      consumer.startField(GeometryColumnX, 0)
      consumer.startGroup()
      consumer.startField("list", 0)
      lines.foreach { line =>
        consumer.startGroup()
        writeLineStringX(consumer, line)
        consumer.endGroup()
      }
      consumer.endField("list", 0)
      consumer.endGroup()
      consumer.endField(GeometryColumnX, 0)

      consumer.startField(GeometryColumnY, 1)
      consumer.startGroup()
      consumer.startField("list", 0)
      lines.foreach { line =>
        consumer.startGroup()
        writeLineStringY(consumer, line)
        consumer.endGroup()
      }
      consumer.endField("list", 0)
      consumer.endGroup()
      consumer.endField(GeometryColumnY, 1)

      consumer.endGroup()
    }
  }

  private abstract class GeoParquetNativeLinesWriter[T <: Geometry](name: String, index: Int)
      extends AttributeWriter[T](name, index) with HasLines[T] {
    private val lineWriter = new GeoParquetNativeLineStringWriter(null, -1)
    override def writeFields(consumer: RecordConsumer, value: T): Unit = {
      consumer.startGroup()
      consumer.startField("list", 0)
      consumer.startGroup()
      consumer.startField("element", 0)
      lines(value).foreach { line =>
        lineWriter.writeFields(consumer, line)
      }
      consumer.endField("element", 0)
      consumer.endGroup()
      consumer.endField("list", 0)
      consumer.endGroup()
    }
  }

  private class PolygonWriter(name: String, index: Int) extends AbstractLinesWriter[Polygon](name, index) with PolygonHasLines

  private class GeoParquetNativePolygonWriter(name: String, index: Int)
    extends GeoParquetNativeLinesWriter[Polygon](name, index) with PolygonHasLines

  private class MultiLineStringWriter(name: String, index: Int)
    extends AbstractLinesWriter[MultiLineString](name, index) with MultiLineStringHasLines

  private class GeoParquetNativeMultiLineStringWriter(name: String, index: Int)
      extends GeoParquetNativeLinesWriter[MultiLineString](name, index) with MultiLineStringHasLines

  private class MultiPolygonWriter(name: String, index: Int) extends AttributeWriter[MultiPolygon](name, index) {
    override def writeFields(consumer: RecordConsumer, value: MultiPolygon): Unit = {
      val polys = Seq.tabulate(value.getNumGeometries) { i =>
        val poly = value.getGeometryN(i).asInstanceOf[Polygon]
        Seq.tabulate(poly.getNumInteriorRing + 1) { i =>
          if (i == 0) { poly.getExteriorRing } else { poly.getInteriorRingN(i - 1) }
        }
      }
      consumer.startGroup()

      consumer.startField(GeometryColumnX, 0)
      consumer.startGroup()
      consumer.startField("list", 0)
      polys.foreach { lines =>
        consumer.startGroup()
        consumer.startField("element", 0)
        consumer.startGroup()
        consumer.startField("list", 0)
        lines.foreach { line =>
          consumer.startGroup()
          writeLineStringX(consumer, line)
          consumer.endGroup()
        }
        consumer.endField("list", 0)
        consumer.endGroup()
        consumer.endField("element", 0)
        consumer.endGroup()
      }
      consumer.endField("list", 0)
      consumer.endGroup()
      consumer.endField(GeometryColumnX, 0)

      consumer.startField(GeometryColumnY, 1)
      consumer.startGroup()
      consumer.startField("list", 0)
      polys.foreach { lines =>
        consumer.startGroup()
        consumer.startField("element", 0)
        consumer.startGroup()
        consumer.startField("list", 0)
        lines.foreach { line =>
          consumer.startGroup()
          writeLineStringY(consumer, line)
          consumer.endGroup()
        }
        consumer.endField("list", 0)
        consumer.endGroup()
        consumer.endField("element", 0)
        consumer.endGroup()
      }
      consumer.endField("list", 0)
      consumer.endGroup()
      consumer.endField(GeometryColumnY, 1)

      consumer.endGroup()
    }
  }

  private class GeoParquetNativeMultiPolygonWriter(name: String, index: Int)
      extends AttributeWriter[MultiPolygon](name, index) with PolygonHasLines {
    private val polygonWriter = new GeoParquetNativePolygonWriter("", -1)
    override def writeFields(consumer: RecordConsumer, value: MultiPolygon): Unit = {
      val polys = Seq.tabulate(value.getNumGeometries)(value.getGeometryN(_).asInstanceOf[Polygon])
      consumer.startGroup()
      consumer.startField("list", 0)
      consumer.startGroup()
      consumer.startField("element", 0)
      polys.foreach { poly =>
        polygonWriter.writeFields(consumer, poly)
      }
      consumer.endField("element", 0)
      consumer.endGroup()
      consumer.endField("list", 0)
      consumer.endGroup()
    }
  }

  private def writeLineStringX(consumer: RecordConsumer, ring: LineString): Unit = {
    consumer.startField("element", 0)
    var i = 0
    while (i < ring.getNumPoints) {
      consumer.addDouble(ring.getCoordinateN(i).x)
      i += 1
    }
    consumer.endField("element", 0)
  }

  private def writeLineStringY(consumer: RecordConsumer, ring: LineString): Unit = {
    consumer.startField("element", 0)
    var i = 0
    while (i < ring.getNumPoints) {
      consumer.addDouble(ring.getCoordinateN(i).y)
      i += 1
    }
    consumer.endField("element", 0)
  }

  private class WkbWriter(name: String, index: Int) extends AttributeWriter[Geometry](name, index) {
    override def writeFields(consumer: RecordConsumer, value: Geometry): Unit =
      consumer.addBinary(Binary.fromConstantByteArray(WKBUtils.write(value)))
  }

  private class BBoxWriter(name: String, index: Int) extends AttributeWriter[Geometry](name, index) {
    override def writeFields(consumer: RecordConsumer, value: Geometry): Unit = {
      val bbox = value.getEnvelopeInternal
      consumer.startGroup()
      consumer.startField(GeometrySchema.BoundingBoxField.XMin, 0)
      consumer.addFloat(bbox.getMinX.toFloat)
      consumer.endField(GeometrySchema.BoundingBoxField.XMin, 0)
      consumer.startField(GeometrySchema.BoundingBoxField.YMin, 1)
      consumer.addFloat(bbox.getMinY.toFloat)
      consumer.endField(GeometrySchema.BoundingBoxField.YMin, 1)
      consumer.startField(GeometrySchema.BoundingBoxField.XMax, 2)
      consumer.addFloat(bbox.getMaxX.toFloat)
      consumer.endField(GeometrySchema.BoundingBoxField.XMax, 2)
      consumer.startField(GeometrySchema.BoundingBoxField.YMax, 3)
      consumer.addFloat(bbox.getMaxY.toFloat)
      consumer.endField(GeometrySchema.BoundingBoxField.YMax, 3)
      consumer.endGroup()
    }
  }
}
