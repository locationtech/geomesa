/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.parquet.io.rw

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.conf.{HadoopParquetConfiguration, ParquetConfiguration}
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.hadoop.api.WriteSupport.{FinalizedWriteContext, WriteContext}
import org.apache.parquet.io.api.{Binary, RecordConsumer}
import org.geotools.api.feature.`type`.AttributeDescriptor
import org.geotools.api.feature.simple.SimpleFeature
import org.locationtech.geomesa.curve.{XZ2SFC, XZSFC, Z2SFC}
import org.locationtech.geomesa.fs.storage.parquet.io.GeoParquetMetadata.GeoParquetObserver
import org.locationtech.geomesa.fs.storage.parquet.io.SimpleFeatureParquetSchema
import org.locationtech.geomesa.fs.storage.parquet.io.geometry.BoundingBoxes.BoundingBoxField
import org.locationtech.geomesa.fs.storage.parquet.io.geometry.GeometrySchema.{GeometryColumnX, GeometryColumnY, GeometryEncoding}
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
  override def init(conf: Configuration): WriteContext = init(new HadoopParquetConfiguration(conf))

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
    geoParquetObserver(record)
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

  private val xz = XZ2SFC(XZSFC.DefaultPrecision)

  private class SimpleFeatureWriter(schema: SimpleFeatureParquetSchema) {

    private val fids = new FidWriter(0) // ID is the 1st field
    private val vis = if (schema.hasVisibilities) { new VisibilityWriter(1) } else { null } // vis is 2nd field

    private val attributes = {
      var index = if (vis == null) { 1 } else { 2 }
      Array.tabulate(schema.sft.getAttributeCount) { i =>
        val writer = attribute(schema.sft.getDescriptor(i), index).asInstanceOf[AttributeWriter[AnyRef]]
        index += writer.numFields
        writer
      }
    }

    def write(consumer: RecordConsumer, value: SimpleFeature): Unit = {
      consumer.startMessage()
      fids.apply(consumer, value.getID)
      if (vis != null) {
        vis.apply(consumer, value.getUserData.get("geomesa.feature.visibility").asInstanceOf[String])
      }
      var i = 0
      while (i < attributes.length) {
        attributes(i).apply(consumer, value.getAttribute(i))
        i += 1
      }
      consumer.endMessage()
    }

    private def attribute(descriptor: AttributeDescriptor, index: Int): AttributeWriter[_] =
      attribute(descriptor.getLocalName, index, ObjectType.selectType(descriptor))

    private def attribute(name: String, index: Int, bindings: Seq[ObjectType]): AttributeWriter[_] = {
      lazy val safeName = alphaNumericSafeString(name)
      bindings.head match {
        case ObjectType.GEOMETRY => geometry(name, index, bindings.last)
        case ObjectType.DATE     => new DateMicrosWriter(safeName, index)
        case ObjectType.STRING   => new StringWriter(safeName, index)
        case ObjectType.INT      => new IntegerWriter(safeName, index)
        case ObjectType.LONG     => new LongWriter(safeName, index)
        case ObjectType.FLOAT    => new FloatWriter(safeName, index)
        case ObjectType.DOUBLE   => new DoubleWriter(safeName, index)
        case ObjectType.BYTES    => new BytesWriter(safeName, index)
        case ObjectType.LIST     => new ListWriter(safeName, index, attribute("element", 0, bindings.drop(1)))
        case ObjectType.MAP      => new MapWriter(safeName, index, attribute("key", 0, bindings.slice(1, 2)), attribute("value", 1, bindings.slice(2, 3)))
        case ObjectType.BOOLEAN  => new BooleanWriter(safeName, index)
        case ObjectType.UUID     => new UuidWriter(safeName, index)
        case _ => throw new IllegalArgumentException(s"Can't serialize field '$name' of type ${bindings.head}")
      }
    }

    // TODO support z/m
    private def geometry(name: String, index: Int, binding: ObjectType): AttributeWriter[_] = {
      val bbox = schema.bboxes.get(name)
      val zValue = schema.zValues.get(name)
      val safeName = alphaNumericSafeString(name)
      if (schema.encodings.geometry == GeometryEncoding.GeoParquetWkb) {
        if (binding == ObjectType.POINT) {
          new WkbPointWriter(safeName, index, bbox, zValue)
        } else {
          new WkbWriter(safeName, index, bbox, zValue)
        }
      } else {
        binding match {
          case ObjectType.POINT               => new PointWriter(safeName, index, bbox, zValue)
          case ObjectType.LINESTRING          => new NativeLineStringWriter(safeName, index, bbox, zValue)
          case ObjectType.POLYGON             => new NativePolygonWriter(safeName, index, bbox, zValue)
          case ObjectType.MULTIPOINT          => new NativeMultiPointWriter(safeName, index, bbox, zValue)
          case ObjectType.MULTILINESTRING     => new NativeMultiLineStringWriter(safeName, index, bbox, zValue)
          case ObjectType.MULTIPOLYGON        => new NativeMultiPolygonWriter(safeName, index, bbox, zValue)
          case ObjectType.GEOMETRY_COLLECTION => new WkbWriter(safeName, index, bbox, zValue)
          case ObjectType.GEOMETRY            => new WkbWriter(safeName, index, bbox, zValue)
          case _ => throw new IllegalArgumentException(s"Can't serialize field '$name' of type $binding")
        }
      }
    }
  }

  /**
    * Writes a simple feature attribute to a Parquet file
    */
  private abstract class AttributeWriter[T <: Any](name: String, index: Int, val numFields: Int = 1) {

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
          val item = iter.next
          if (item != null) {
            elements.writeFields(consumer, item)
          }
        }
        consumer.endField("element", 0)
        consumer.endGroup()
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

  /**
   * Writes a simple feature attribute to a Parquet file
   */
  private abstract class GeometryWriter[T <: Geometry](name: String, index: Int, bbox: Option[String], zValue: Option[String])
      extends AttributeWriter[T](name, index, 1 + bbox.size + zValue.size) {

    protected def z(geom: T): Long

    /**
     * Writes a value to the current record
     *
     * @param consumer the Parquet record consumer
     * @param value value to write
     */
    override def apply(consumer: RecordConsumer, value: T): Unit = {
      if (value != null) {
        consumer.startField(name, index)
        writeFields(consumer, value)
        consumer.endField(name, index)
        bbox.foreach { name =>
          val bbox = value.getEnvelopeInternal
          consumer.startField(name, index + 1)
          consumer.startGroup()
          consumer.startField(BoundingBoxField.XMin, 0)
          consumer.addFloat(bbox.getMinX.toFloat)
          consumer.endField(BoundingBoxField.XMin, 0)
          consumer.startField(BoundingBoxField.YMin, 1)
          consumer.addFloat(bbox.getMinY.toFloat)
          consumer.endField(BoundingBoxField.YMin, 1)
          consumer.startField(BoundingBoxField.XMax, 2)
          consumer.addFloat(bbox.getMaxX.toFloat)
          consumer.endField(BoundingBoxField.XMax, 2)
          consumer.startField(BoundingBoxField.YMax, 3)
          consumer.addFloat(bbox.getMaxY.toFloat)
          consumer.endField(BoundingBoxField.YMax, 3)
          consumer.endGroup()
          consumer.endField(name, index + 1)
        }
        zValue.foreach { name =>
          consumer.startField(name, index + 1 + bbox.size)
          consumer.addLong(z(value))
          consumer.endField(name, index + 1 + bbox.size)
        }
      }
    }
  }

  private abstract class GeometryZWriter(name: String, index: Int, bbox: Option[String], zValue: Option[String])
      extends GeometryWriter[Point](name, index, bbox, zValue) {
    override protected def z(geom: Point): Long = Z2SFC.index(geom.getX, geom.getY)
  }

  private abstract class GeometryXZWriter[T <: Geometry](name: String, index: Int, bbox: Option[String], zValue: Option[String])
    extends GeometryWriter[T](name, index, bbox, zValue) {
    override protected def z(geom: T): Long = {
      val env = geom.getEnvelopeInternal
      xz.index(env.getMinX, env.getMinY, env.getMaxX, env.getMaxY)
    }
  }

  private class PointWriter(name: String, index: Int, bbox: Option[String], zValue: Option[String])
      extends GeometryZWriter(name, index, bbox, zValue) {
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

  private class NativeLineStringWriter(name: String, index: Int, bbox: Option[String], zValue: Option[String])
      extends GeometryXZWriter[LineString](name, index, bbox, zValue) {

    private val pointWriter = new PointWriter("", -1, None, None)

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

  private class NativeMultiPointWriter(name: String, index: Int, bbox: Option[String], zValue: Option[String])
      extends GeometryXZWriter[MultiPoint](name, index, bbox, zValue) {

    private val pointWriter = new PointWriter("", -1, None, None)

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

  private abstract class NativeLinesWriter[T <: Geometry](name: String, index: Int, bbox: Option[String], zValue: Option[String])
      extends GeometryXZWriter[T](name, index, bbox, zValue) with HasLines[T] {

    private val lineWriter = new NativeLineStringWriter(null, -1, None, None)

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

  private class NativePolygonWriter(name: String, index: Int, bbox: Option[String], zValue: Option[String])
    extends NativeLinesWriter[Polygon](name, index, bbox, zValue) with PolygonHasLines

  private class NativeMultiLineStringWriter(name: String, index: Int, bbox: Option[String], zValue: Option[String])
      extends NativeLinesWriter[MultiLineString](name, index, bbox, zValue) with MultiLineStringHasLines

  private class NativeMultiPolygonWriter(name: String, index: Int, bbox: Option[String], zValue: Option[String])
      extends GeometryXZWriter[MultiPolygon](name, index, bbox, zValue) with PolygonHasLines {

    private val polygonWriter = new NativePolygonWriter("", -1, None, None)

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

  private class WkbPointWriter(name: String, index: Int, bbox: Option[String], zValue: Option[String])
      extends GeometryZWriter(name, index, bbox, zValue) {
    override def writeFields(consumer: RecordConsumer, value: Point): Unit =
      consumer.addBinary(Binary.fromConstantByteArray(WKBUtils.write(value)))
  }

  private class WkbWriter(name: String, index: Int, bbox: Option[String], zValue: Option[String])
      extends GeometryXZWriter[Geometry](name, index, bbox, zValue) {
    override def writeFields(consumer: RecordConsumer, value: Geometry): Unit =
      consumer.addBinary(Binary.fromConstantByteArray(WKBUtils.write(value)))
  }
}
