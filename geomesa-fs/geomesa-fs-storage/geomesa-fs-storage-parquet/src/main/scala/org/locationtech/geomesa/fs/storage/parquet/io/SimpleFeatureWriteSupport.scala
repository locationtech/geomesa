/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.parquet.io

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.hadoop.api.WriteSupport.{FinalizedWriteContext, WriteContext}
import org.apache.parquet.io.api.{Binary, RecordConsumer}
import org.geotools.api.feature.`type`.{AttributeDescriptor, GeometryDescriptor}
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.locationtech.geomesa.fs.storage.common.AbstractFileSystemStorage.MetadataObserver
import org.locationtech.geomesa.fs.storage.common.jobs.StorageConfiguration
import org.locationtech.geomesa.fs.storage.parquet.io.SimpleFeatureParquetSchema.{GeoParquetSchemaKey, SchemaVersionKey}
import org.locationtech.geomesa.utils.geotools.ObjectType
import org.locationtech.geomesa.utils.geotools.ObjectType.ObjectType
import org.locationtech.geomesa.utils.text.WKBUtils
import org.locationtech.jts.geom._

import java.nio.ByteBuffer
import java.util.{Date, UUID}
import scala.collection.JavaConverters._

class SimpleFeatureWriteSupport(callback: (Envelope, Long) => Unit = ((_, _) => {})) extends WriteSupport[SimpleFeature] {

  private class MultipleGeometriesObserver extends MetadataObserver {
    private var count: Long = 0L
    private var numGeoms: Int = 0 // number of geometries in the file
    private var bounds: Array[Envelope] = new Array[Envelope](0)

    override def write(feature: SimpleFeature): Unit = {
      // Update internal count/bounds/etc
      count += 1L

      // Initialize a bounding box for each geometry if we haven't already done so
      if (bounds.isEmpty) {
        val sft = feature.getFeatureType
        val geometryDescriptors = sft.getAttributeDescriptors.toArray.collect {case gd: GeometryDescriptor => gd}
        numGeoms = geometryDescriptors.length
        bounds = geometryDescriptors.map(_ => new Envelope)
      }

      val envelopes = feature.getAttributes.toArray.collect {
        case geom: Geometry => geom.getEnvelopeInternal
      }

      // Expand the bounding box for each geometry
      (0 until numGeoms).foreach(i => bounds(i).expandToInclude(envelopes(i)))
    }

    def getBoundingBoxes: Array[Envelope] = bounds

    override def close(): Unit = {
      // Merge all the envelopes into one
      val mergedBounds = new Envelope()
      for (b <- bounds) {
        mergedBounds.expandToInclude(b)
      }

      onClose(mergedBounds, count)
    }

    // Invokes the callback function that adds metadata to the storage partition
    override protected def onClose(bounds: Envelope, count: Long): Unit = callback(bounds, count)
  }

  private val observer = new MultipleGeometriesObserver
  private var writer: SimpleFeatureWriteSupport.SimpleFeatureWriter = _
  private var consumer: RecordConsumer = _
  private var schema: SimpleFeatureParquetSchema = _

  override val getName: String = "SimpleFeatureWriteSupport"

  // Need a no-arg constructor because Apache Parquet can't instantiate the callback arg for the MapReduce compaction job
  // Also, the compaction job doesn't write or calculate bounds anyway
  def this() = this( (_, _) => {} )

  // called once
  override def init(conf: Configuration): WriteContext = {
    schema = SimpleFeatureParquetSchema.write(conf).getOrElse {
      throw new IllegalArgumentException("Could not extract SimpleFeatureType from write context")
    }
    this.writer = SimpleFeatureWriteSupport.SimpleFeatureWriter(schema.sft)

    new WriteContext(schema.schema, schema.metadata)
  }

  // called once at the end after all SimpleFeatures are written
  override def finalizeWrite(): FinalizedWriteContext = {
    // Get the bounding boxes that span each geometry type
    val bboxes = observer.getBoundingBoxes
    observer.close()

    // If the SFT has no geometries, then there's no need to create GeoParquet metadata
    if (bboxes.isEmpty) {
      return new FinalizedWriteContext(schema.metadata)
    }

    // TODO: not an elegant way to do it
    //  somehow trying to mutate the map, e.g. by calling metadata.put(GeoParquetSchemaKey, result), causes empty parquet files to be written
    val newMetadata: java.util.Map[String, String] = Map(
      StorageConfiguration.SftNameKey -> schema.metadata.get(StorageConfiguration.SftNameKey),
      StorageConfiguration.SftSpecKey -> schema.metadata.get(StorageConfiguration.SftSpecKey),
      SchemaVersionKey -> schema.metadata.get(SchemaVersionKey),
      GeoParquetSchemaKey -> SimpleFeatureParquetSchema.geoParquetMetadata(schema.sft, bboxes)
    ).asJava

    new FinalizedWriteContext(newMetadata)
  }

  // called per block
  override def prepareForWrite(recordConsumer: RecordConsumer): Unit = consumer = recordConsumer

  // called per row
  override def write(record: SimpleFeature): Unit = {
    writer.write(consumer, record)
    observer.write(record)
  }
}

object SimpleFeatureWriteSupport {

  class SimpleFeatureWriter(attributes: Array[AttributeWriter[AnyRef]]) {

    private val fids = new FidWriter(attributes.length) // put the ID at the end of the record

    def write(consumer: RecordConsumer, value: SimpleFeature): Unit = {
      consumer.startMessage()
      var i = 0
      while (i < attributes.length) {
        attributes(i).apply(consumer, value.getAttribute(i))
        i += 1
      }
      fids.apply(consumer, value.getID)
      consumer.endMessage()
    }
  }

  object SimpleFeatureWriter {
    def apply(sft: SimpleFeatureType): SimpleFeatureWriter = {
      val attributes = Array.tabulate(sft.getAttributeCount)(i => attribute(sft.getDescriptor(i), i))
      new SimpleFeatureWriter(attributes.asInstanceOf[Array[AttributeWriter[AnyRef]]])
    }
  }

  def attribute(descriptor: AttributeDescriptor, index: Int): AttributeWriter[_] = {
    val bindings = ObjectType.selectType(descriptor.getType.getBinding, descriptor.getUserData)
    attribute(descriptor.getLocalName, index, bindings)
  }

  def attribute(name: String, index: Int, bindings: Seq[ObjectType]): AttributeWriter[_] = {
    bindings.head match {
      case ObjectType.GEOMETRY => new GeometryWkbAttributeWriter(name, index) // TODO support z/m
      case ObjectType.DATE     => new DateWriter(name, index)
      case ObjectType.STRING   => new StringWriter(name, index)
      case ObjectType.INT      => new IntegerWriter(name, index)
      case ObjectType.LONG     => new LongWriter(name, index)
      case ObjectType.FLOAT    => new FloatWriter(name, index)
      case ObjectType.DOUBLE   => new DoubleWriter(name, index)
      case ObjectType.BYTES    => new BytesWriter(name, index)
      case ObjectType.LIST     => new ListWriter(name, index, bindings(1))
      case ObjectType.MAP      => new MapWriter(name, index, bindings(1), bindings(2))
      case ObjectType.BOOLEAN  => new BooleanWriter(name, index)
      case ObjectType.UUID     => new UuidWriter(name, index)
      case _ => throw new IllegalArgumentException(s"Can't serialize field '$name' of type ${bindings.head}")
    }
  }

  /**
    * Writes a simple feature attribute to a Parquet file
    */
  abstract class AttributeWriter[T <: AnyRef](name: String, index: Int) {

    /**
      * Writes a value to the current record
      *
      * @param consumer the Parquet record consumer
      * @param value value to write
      */
    def apply(consumer: RecordConsumer, value: T): Unit = {
      if (value != null) {
        consumer.startField(name, index)
        write(consumer, value)
        consumer.endField(name, index)
      }
    }

    protected def write(consumer: RecordConsumer, value: T): Unit
  }

  class FidWriter(index: Int) extends AttributeWriter[String](SimpleFeatureParquetSchema.FeatureIdField, index) {
    override protected def write(consumer: RecordConsumer, value: String): Unit =
      consumer.addBinary(Binary.fromString(value))
  }

  class DateWriter(name: String, index: Int) extends AttributeWriter[Date](name, index) {
    override protected def write(consumer: RecordConsumer, value: Date): Unit =
      consumer.addLong(value.getTime)
  }

  class DoubleWriter(name: String, index: Int) extends AttributeWriter[java.lang.Double](name, index) {
    override protected def write(consumer: RecordConsumer, value: java.lang.Double): Unit =
      consumer.addDouble(value)
  }

  class FloatWriter(name: String, index: Int) extends AttributeWriter[java.lang.Float](name, index) {
    override protected def write(consumer: RecordConsumer, value: java.lang.Float): Unit =
      consumer.addFloat(value)
  }

  class IntegerWriter(name: String, index: Int) extends AttributeWriter[java.lang.Integer](name, index) {
    override protected def write(consumer: RecordConsumer, value: java.lang.Integer): Unit =
      consumer.addInteger(value)
  }

  class LongWriter(name: String, index: Int) extends AttributeWriter[java.lang.Long](name, index) {
    override protected def write(consumer: RecordConsumer, value: java.lang.Long): Unit =
      consumer.addLong(value)
  }

  class StringWriter(name: String, index: Int) extends AttributeWriter[String](name, index) {
    override protected def write(consumer: RecordConsumer, value: String): Unit =
      consumer.addBinary(Binary.fromString(value))
  }

  class BytesWriter(name: String, index: Int) extends AttributeWriter[Array[Byte]](name, index) {
    override protected def write(consumer: RecordConsumer, value: Array[Byte]): Unit =
      consumer.addBinary(Binary.fromConstantByteArray(value))
  }

  class BooleanWriter(name: String, index: Int) extends AttributeWriter[java.lang.Boolean](name, index) {
    override protected def write(consumer: RecordConsumer, value: java.lang.Boolean): Unit =
      consumer.addBoolean(value)
  }

  class ListWriter(name: String, index: Int, valueType: ObjectType)
      extends AttributeWriter[java.util.List[AnyRef]](name, index) {

    private val elementWriter = attribute("element", 0, Seq(valueType)).asInstanceOf[AttributeWriter[AnyRef]]

    override protected def write(consumer: RecordConsumer, value: java.util.List[AnyRef]): Unit = {
      consumer.startGroup()
      if (!value.isEmpty) {
        consumer.startField("list", 0)
        val iter = value.iterator
        while (iter.hasNext) {
          consumer.startGroup()
          val item = iter.next
          if (item != null) {
            elementWriter(consumer, item)
          }
          consumer.endGroup()
        }
        consumer.endField("list", 0)
      }
      consumer.endGroup()
    }
  }

  class MapWriter(name: String, index: Int, keyType: ObjectType, valueType: ObjectType)
      extends AttributeWriter[java.util.Map[AnyRef, AnyRef]](name, index) {

    private val keyWriter = attribute("key", 0, Seq(keyType)).asInstanceOf[AttributeWriter[AnyRef]]
    private val valueWriter = attribute("value", 1, Seq(valueType)).asInstanceOf[AttributeWriter[AnyRef]]

    override protected def write(consumer: RecordConsumer, value: java.util.Map[AnyRef, AnyRef]): Unit = {
      consumer.startGroup()
      if (!value.isEmpty) {
        consumer.startField("map", 0)
        val iter = value.entrySet().iterator
        while (iter.hasNext) {
          val entry = iter.next()
          consumer.startGroup()
          keyWriter(consumer, entry.getKey)
          val v = entry.getValue
          if (v != null) {
            valueWriter(consumer, v)
          }
          consumer.endGroup()
        }
        consumer.endField("map", 0)
      }
      consumer.endGroup()
    }
  }

  class UuidWriter(name: String, index: Int) extends AttributeWriter[UUID](name, index) {
    override protected def write(consumer: RecordConsumer, value: UUID): Unit = {
      val bb = ByteBuffer.wrap(new Array[Byte](16))
      bb.putLong(value.getMostSignificantBits)
      bb.putLong(value.getLeastSignificantBits)
      consumer.addBinary(Binary.fromConstantByteArray(bb.array()))
    }
  }

  class GeometryWkbAttributeWriter(name: String, index: Int) extends AttributeWriter[Geometry](name, index) {
    override protected def write(consumer: RecordConsumer, value: Geometry): Unit =
      consumer.addBinary(Binary.fromConstantByteArray(WKBUtils.write(value)))
  }
}
