/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow
package vector

import java.nio.charset.StandardCharsets
import java.util.Date

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.{FixedSizeListVector, ListVector, StructVector}
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.pojo.{ArrowType, FieldType}
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding.Encoding
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding.Encoding.Encoding
import org.locationtech.geomesa.features.serialization.ObjectType
import org.locationtech.geomesa.features.serialization.ObjectType.ObjectType
import org.locationtech.geomesa.filter.function.ProxyIdFunction
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.jts.geom._
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
  * Writes a simple feature attribute to an arrow vector
  */
trait ArrowAttributeWriter {

  /**
    * Writes an attribute for the ith feature
    * @param i index of the feature to write
    * @param value attribute value to write
    */
  def apply(i: Int, value: AnyRef): Unit

  /**
    * Sets the underlying value count, after writing is finished. @see FieldVector.Mutator.setValueCount
    *
    * @param count number of features written (or null)
    */
  def setValueCount(count: Int): Unit = vector.setValueCount(count)

  /**
    * Handle to the underlying field vector being written to
    *
    * @return
    */
  def vector: FieldVector
}

object ArrowAttributeWriter {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  import scala.collection.JavaConverters._

  /**
    * Writer for feature ID. The return FeatureWriter expects to be passed the entire SimpleFeature, not
    * just the feature ID string (this is to support cached UUIDs).
    *
    * @param vector simple feature vector
    * @param encoding actually write the feature ids, or omit them, in which case the writer is a no-op
    * @return feature ID writer
    */
  def id(sft: SimpleFeatureType, encoding: SimpleFeatureEncoding, vector: StructVector): ArrowAttributeWriter = {
    val name = SimpleFeatureVector.FeatureIdField
    encoding.fids match {
      case None => ArrowNoopWriter
      case Some(Encoding.Min) if sft.isUuid => new ArrowFeatureIdMinimalUuidWriter(name, VectorFactory(vector))
      case Some(Encoding.Max) if sft.isUuid => new ArrowFeatureIdUuidWriter(name, VectorFactory(vector))
      case Some(Encoding.Min) => new ArrowFeatureIdMinimalStringWriter(name, VectorFactory(vector))
      case Some(Encoding.Max) => new ArrowFeatureIdStringWriter(name, VectorFactory(vector))
    }
  }

  /**
    * Creates a sequence of attribute writers for a simple feature type. Each attribute in the feature type
    * will map to a writer in the returned sequence.
    *
    * @param sft simple feature type
    * @param vector child vectors will be created in this container
    * @param dictionaries dictionaries, if any
    * @param encoding encoding options
    * @return attribute writers
    */
  def apply(
      sft: SimpleFeatureType,
      vector: StructVector,
      dictionaries: Map[String, ArrowDictionary] = Map.empty,
      encoding: SimpleFeatureEncoding = SimpleFeatureEncoding.Min): Seq[ArrowAttributeWriter] = {
    sft.getAttributeDescriptors.asScala.map { descriptor =>
      apply(sft, descriptor, dictionaries.get(descriptor.getLocalName), encoding, vector)
    }
  }

  /**
   * Creates a single attribute writer
   *
   * @param sft simple feature type
   * @param descriptor attribute descriptor
   * @param dictionary the dictionary for the attribute, if any
   * @param encoding encoding options
   * @param vector child vectors will be created in the container
   * @return
   */
  def apply(
      sft: SimpleFeatureType,
      descriptor: AttributeDescriptor,
      dictionary: Option[ArrowDictionary],
      encoding: SimpleFeatureEncoding,
      vector: StructVector): ArrowAttributeWriter = {
    apply(sft, descriptor, dictionary, encoding, VectorFactory(vector))
  }

  /**
   * Creates a single attribute writer
   *
   * @param sft simple feature type
   * @param descriptor attribute descriptor
   * @param dictionary the dictionary for the attribute, if any
   * @param encoding encoding options
   * @param allocator buffer allocator used to create underlying vectors
   * @return
   */
  def apply(
      sft: SimpleFeatureType,
      descriptor: AttributeDescriptor,
      dictionary: Option[ArrowDictionary],
      encoding: SimpleFeatureEncoding,
      allocator: BufferAllocator): ArrowAttributeWriter = {
    apply(sft, descriptor, dictionary, encoding, VectorFactory(allocator))
  }

  /**
   * Creates a single attribute writer
   *
   * @param sft simple feature type
   * @param descriptor attribute descriptor
   * @param dictionary the dictionary for the attribute, if any
   * @param encoding encoding options
   * @param factory factory used to create underlying vectors
   * @return
   */
  def apply(
      sft: SimpleFeatureType,
      descriptor: AttributeDescriptor,
      dictionary: Option[ArrowDictionary],
      encoding: SimpleFeatureEncoding,
      factory: VectorFactory): ArrowAttributeWriter = {
    val name = descriptor.getLocalName
    val bindings = ObjectType.selectType(descriptor)
    val metadata = Map(SimpleFeatureVector.DescriptorKey -> SimpleFeatureTypes.encodeDescriptor(sft, descriptor))
    apply(name, bindings, dictionary, metadata, encoding, factory)
  }

  /**
   * Low-level method to create a single attribute writer
   *
   * @param name attribute name
   * @param bindings object bindings, the attribute type plus any subtypes (e.g. for lists or maps)
   * @param dictionary the dictionary for the attribute, if any
   * @param metadata vector metadata encoded in the field - generally the encoded attribute descriptor
   * @param encoding encoding options
   * @param factory parent vector or allocator
   * @return
   */
  def apply(
      name: String,
      bindings: Seq[ObjectType],
      dictionary: Option[ArrowDictionary],
      metadata: Map[String, String],
      encoding: SimpleFeatureEncoding,
      factory: VectorFactory): ArrowAttributeWriter = {
    dictionary match {
      case Some(dict) =>
        ArrowAttributeWriter.dictionary(name, dict, metadata, factory)

      case None =>
        bindings.head match {
          case ObjectType.STRING   => new ArrowStringWriter(name, metadata, factory)
          case ObjectType.DATE     => date(name, encoding.date, metadata, factory)
          case ObjectType.INT      => new ArrowIntWriter(name, metadata, factory)
          case ObjectType.LONG     => new ArrowLongWriter(name, metadata, factory)
          case ObjectType.FLOAT    => new ArrowFloatWriter(name, metadata, factory)
          case ObjectType.DOUBLE   => new ArrowDoubleWriter(name, metadata, factory)
          case ObjectType.GEOMETRY => geometry(name, bindings(1), encoding.geometry, metadata, factory)
          case ObjectType.BOOLEAN  => new ArrowBooleanWriter(name, metadata, factory)
          case ObjectType.LIST     => new ArrowListWriter(name, bindings(1), encoding, metadata, factory)
          case ObjectType.MAP      => new ArrowMapWriter(name, bindings(1), bindings(2), encoding, metadata, factory)
          case ObjectType.BYTES    => new ArrowBytesWriter(name, metadata, factory)
          case ObjectType.UUID     => new ArrowStringWriter(name, metadata, factory)
          case _ => throw new IllegalArgumentException(s"Unexpected object type ${bindings.head}")
        }
    }
  }

  private def dictionary(
      name: String,
      dictionary: ArrowDictionary,
      metadata: Map[String, String],
      factory: VectorFactory): ArrowAttributeWriter = {
    dictionary.encoding.getIndexType.getBitWidth match {
      case 8  => new ArrowDictionaryByteWriter(name, dictionary, metadata, factory)
      case 16 => new ArrowDictionaryShortWriter(name, dictionary, metadata, factory)
      case 32 => new ArrowDictionaryIntWriter(name, dictionary, metadata, factory)
      case w  => throw new IllegalArgumentException(s"Unsupported dictionary encoding width: $w")
    }
  }

  private def date(
      name: String,
      encoding: Encoding,
      metadata: Map[String, String],
      factory: VectorFactory): ArrowAttributeWriter = {
    encoding match {
      case Encoding.Min => new ArrowDateSecondsWriter(name, metadata, factory)
      case Encoding.Max => new ArrowDateMillisWriter(name, metadata, factory)
    }
  }

  private def geometry(
      name: String,
      binding: ObjectType,
      encoding: Encoding,
      metadata: Map[String, String],
      factory: VectorFactory): ArrowGeometryWriter = {
    val m = metadata.asJava
    val vector = (binding, encoding, factory) match {
      case (ObjectType.POINT, Encoding.Min, FromStruct(c))              => new PointFloatVector(name, c, m)
      case (ObjectType.POINT, Encoding.Min, FromAllocator(c))           => new PointFloatVector(name, c, m)
      case (ObjectType.POINT, Encoding.Max, FromStruct(c))              => new PointVector(name, c, m)
      case (ObjectType.POINT, Encoding.Max, FromAllocator(c))           => new PointVector(name, c, m)
      case (ObjectType.LINESTRING, Encoding.Min, FromStruct(c))         => new LineStringFloatVector(name, c, m)
      case (ObjectType.LINESTRING, Encoding.Min, FromAllocator(c))      => new LineStringFloatVector(name, c, m)
      case (ObjectType.LINESTRING, Encoding.Max, FromStruct(c))         => new LineStringVector(name, c, m)
      case (ObjectType.LINESTRING, Encoding.Max, FromAllocator(c))      => new LineStringVector(name, c, m)
      case (ObjectType.POLYGON, Encoding.Min, FromStruct(c))            => new PolygonFloatVector(name, c, m)
      case (ObjectType.POLYGON, Encoding.Min, FromAllocator(c))         => new PolygonFloatVector(name, c, m)
      case (ObjectType.POLYGON, Encoding.Max, FromStruct(c))            => new PolygonVector(name, c, m)
      case (ObjectType.POLYGON, Encoding.Max, FromAllocator(c))         => new PolygonVector(name, c, m)
      case (ObjectType.MULTILINESTRING, Encoding.Min, FromStruct(c))    => new MultiLineStringFloatVector(name, c, m)
      case (ObjectType.MULTILINESTRING, Encoding.Min, FromAllocator(c)) => new MultiLineStringFloatVector(name, c, m)
      case (ObjectType.MULTILINESTRING, Encoding.Max, FromStruct(c))    => new MultiLineStringVector(name, c, m)
      case (ObjectType.MULTILINESTRING, Encoding.Max, FromAllocator(c)) => new MultiLineStringVector(name, c, m)
      case (ObjectType.MULTIPOLYGON, Encoding.Min, FromStruct(c))       => new MultiPolygonFloatVector(name, c, m)
      case (ObjectType.MULTIPOLYGON, Encoding.Min, FromAllocator(c))    => new MultiPolygonFloatVector(name, c, m)
      case (ObjectType.MULTIPOLYGON, Encoding.Max, FromStruct(c))       => new MultiPolygonVector(name, c, m)
      case (ObjectType.MULTIPOLYGON, Encoding.Max, FromAllocator(c))    => new MultiPolygonVector(name, c, m)
      case (ObjectType.MULTIPOINT, Encoding.Min, FromStruct(c))         => new MultiPointFloatVector(name, c, m)
      case (ObjectType.MULTIPOINT, Encoding.Min, FromAllocator(c))      => new MultiPointFloatVector(name, c, m)
      case (ObjectType.MULTIPOINT, Encoding.Max, FromStruct(c))         => new MultiPointVector(name, c, m)
      case (ObjectType.MULTIPOINT, Encoding.Max, FromAllocator(c))      => new MultiPointVector(name, c, m)
      case (ObjectType.GEOMETRY, _, FromStruct(c))                      => new WKBGeometryVector(name, c, m)
      case (ObjectType.GEOMETRY, _, FromAllocator(c))                   => new WKBGeometryVector(name, c, m)
      case (ObjectType.GEOMETRY_COLLECTION, _, _) => throw new NotImplementedError(s"Geometry type $binding is not supported")
      case (_, _, FromList(_)) => throw new NotImplementedError("Geometry lists are not supported")
      case _ => throw new IllegalArgumentException(s"Unexpected geometry type $binding")
    }
    new ArrowGeometryWriter(vector.asInstanceOf[GeometryVector[Geometry, FieldVector]])
  }

  trait ArrowDictionaryWriter extends ArrowAttributeWriter {
    def dictionary: ArrowDictionary
  }

  /**
    * Converts a value into a dictionary byte and writes it
    */
  class ArrowDictionaryByteWriter(
      name: String,
      val dictionary: ArrowDictionary,
      metadata: Map[String, String],
      factory: VectorFactory
    ) extends ArrowDictionaryWriter {

    override val vector: TinyIntVector =
      factory.apply(name, new FieldType(true, MinorType.TINYINT.getType, dictionary.encoding, metadata.asJava))

    // note: nulls get encoded in the dictionary
    override def apply(i: Int, value: AnyRef): Unit = vector.setSafe(i, dictionary.index(value).toByte)
  }

  /**
    * Converts a value into a dictionary short and writes it
    */
  class ArrowDictionaryShortWriter(
      name: String,
      val dictionary: ArrowDictionary,
      metadata: Map[String, String],
      factory: VectorFactory
    ) extends ArrowDictionaryWriter {

    override val vector: SmallIntVector =
      factory.apply(name, new FieldType(true, MinorType.SMALLINT.getType, dictionary.encoding, metadata.asJava))

    // note: nulls get encoded in the dictionary
    override def apply(i: Int, value: AnyRef): Unit = vector.setSafe(i, dictionary.index(value).toShort)
  }

  /**
    * Converts a value into a dictionary int and writes it
    */
  class ArrowDictionaryIntWriter(
      name: String,
      val dictionary: ArrowDictionary,
      metadata: Map[String, String],
      factory: VectorFactory
    ) extends ArrowDictionaryWriter {

    override val vector: IntVector =
      factory.apply(name, new FieldType(true, MinorType.INT.getType, dictionary.encoding, metadata.asJava))

    // note: nulls get encoded in the dictionary
    override def apply(i: Int, value: AnyRef): Unit = vector.setSafe(i, dictionary.index(value))
  }

  /**
    * Writes geometries - delegates to our JTS geometry vectors
    */
  class ArrowGeometryWriter(delegate: GeometryVector[Geometry, FieldVector]) extends ArrowAttributeWriter {

    override def vector: FieldVector = delegate.getVector

    // note: delegate handles nulls
    override def apply(i: Int, value: AnyRef): Unit = delegate.set(i, value.asInstanceOf[Geometry])

    override def setValueCount(count: Int): Unit = delegate.setValueCount(count)
  }

  /**
    * Doesn't actually write anything
    */
  object ArrowNoopWriter extends ArrowAttributeWriter {
    override def vector: FieldVector = null
    override def apply(i: Int, value: AnyRef): Unit = {}
  }

  class ArrowFeatureIdMinimalUuidWriter(name: String, factory: VectorFactory)
      extends ArrowAttributeWriter {

    override val vector: IntVector = factory.apply(name, MinorType.INT, Map.empty)

    override def apply(i: Int, value: AnyRef): Unit = {
      import org.locationtech.geomesa.utils.geotools.Conversions.RichSimpleFeature
      val (msb, lsb) = value.asInstanceOf[SimpleFeature].getUuid
      vector.setSafe(i, ProxyIdFunction.proxyId(msb, lsb))
    }
  }

  class ArrowFeatureIdMinimalStringWriter(name: String, factory: VectorFactory)
      extends ArrowAttributeWriter {

    override val vector: IntVector = factory.apply(name, MinorType.INT, Map.empty)

    override def apply(i: Int, value: AnyRef): Unit =
      vector.setSafe(i, ProxyIdFunction.proxyId(value.asInstanceOf[SimpleFeature].getID))
  }

  class ArrowFeatureIdUuidWriter(name: String, factory: VectorFactory)
      extends ArrowAttributeWriter {

    override val vector: FixedSizeListVector = factory.apply(name, FieldType.nullable(new ArrowType.FixedSizeList(2)))

    private val bits = {
      val result = vector.addOrGetVector[BigIntVector](FieldType.nullable(MinorType.BIGINT.getType))
      if (result.isCreated) {
        result.getVector.allocateNew()
      }
      result.getVector
    }

    override def apply(i: Int, value: AnyRef): Unit = {
      import org.locationtech.geomesa.utils.geotools.Conversions.RichSimpleFeature
      val (msb, lsb) = value.asInstanceOf[SimpleFeature].getUuid
      vector.setNotNull(i)
      bits.setSafe(i * 2, msb)
      bits.setSafe(i * 2 + 1, lsb)
    }
  }

  class ArrowFeatureIdStringWriter(name: String, factory: VectorFactory)
      extends ArrowAttributeWriter {

    override val vector: VarCharVector = factory.apply(name, MinorType.VARCHAR, Map.empty)

    override def apply(i: Int, value: AnyRef): Unit = {
      val bytes = value.asInstanceOf[SimpleFeature].getID.getBytes(StandardCharsets.UTF_8)
      vector.setSafe(i, bytes, 0, bytes.length)
    }
  }

  class ArrowStringWriter(name: String, metadata: Map[String, String], factory: VectorFactory)
      extends ArrowAttributeWriter {

    override val vector: VarCharVector = factory.apply(name, MinorType.VARCHAR, metadata)

    override def apply(i: Int, value: AnyRef): Unit = {
      if (value == null) {
        vector.setNull(i) // note: calls .setSafe internally
      } else {
        val bytes = value.toString.getBytes(StandardCharsets.UTF_8)
        vector.setSafe(i, bytes, 0, bytes.length)
      }
    }
  }

  class ArrowIntWriter(name: String, metadata: Map[String, String], factory: VectorFactory)
      extends ArrowAttributeWriter {

    override val vector: IntVector = factory.apply(name, MinorType.INT, metadata)

    override def apply(i: Int, value: AnyRef): Unit = {
      if (value == null) {
        vector.setNull(i) // note: calls .setSafe internally
      } else {
        vector.setSafe(i, value.asInstanceOf[Int])
      }
    }
  }

  class ArrowLongWriter(name: String, metadata: Map[String, String], factory: VectorFactory)
      extends ArrowAttributeWriter {

    override val vector: BigIntVector = factory.apply(name, MinorType.BIGINT, metadata)

    override def apply(i: Int, value: AnyRef): Unit = {
      if (value == null) {
        vector.setNull(i) // note: calls .setSafe internally
      } else {
        vector.setSafe(i, value.asInstanceOf[Long])
      }
    }
  }

  class ArrowFloatWriter(name: String, metadata: Map[String, String], factory: VectorFactory)
      extends ArrowAttributeWriter {

    override val vector: Float4Vector = factory.apply(name, MinorType.FLOAT4, metadata)

    override def apply(i: Int, value: AnyRef): Unit = {
      if (value == null) {
        vector.setNull(i) // note: calls .setSafe internally
      } else {
        vector.setSafe(i, value.asInstanceOf[Float])
      }
    }
  }

  class ArrowDoubleWriter(name: String, metadata: Map[String, String], factory: VectorFactory)
      extends ArrowAttributeWriter {

    override val vector: Float8Vector = factory.apply(name, MinorType.FLOAT8, metadata)

    override def apply(i: Int, value: AnyRef): Unit = {
      if (value == null) {
        vector.setNull(i) // note: calls .setSafe internally
      } else {
        vector.setSafe(i, value.asInstanceOf[Double])
      }
    }
  }

  class ArrowBooleanWriter(name: String, metadata: Map[String, String], factory: VectorFactory)
      extends ArrowAttributeWriter {

    override val vector: BitVector = factory.apply(name, MinorType.BIT, metadata)

    override def apply(i: Int, value: AnyRef): Unit = {
      if (value == null) {
        vector.setNull(i) // note: calls .setSafe internally
      } else {
        vector.setSafe(i, if (value.asInstanceOf[Boolean]) { 1 } else { 0 })
      }
    }
  }

  class ArrowDateMillisWriter(name: String, metadata: Map[String, String], factory: VectorFactory)
      extends ArrowAttributeWriter {

    override val vector: BigIntVector = factory.apply(name, MinorType.BIGINT, metadata)

    override def apply(i: Int, value: AnyRef): Unit = {
      if (value == null) {
        vector.setNull(i) // note: calls .setSafe internally
      } else {
        vector.setSafe(i, value.asInstanceOf[Date].getTime)
      }
    }
  }

  class ArrowDateSecondsWriter(name: String, metadata: Map[String, String], factory: VectorFactory)
      extends ArrowAttributeWriter {

    override val vector: IntVector = factory.apply(name, MinorType.INT, metadata)

    override def apply(i: Int, value: AnyRef): Unit = {
      if (value == null) {
        vector.setNull(i) // note: calls .setSafe internally
      } else {
        vector.setSafe(i, (value.asInstanceOf[Date].getTime / 1000L).toInt)
      }
    }
  }

  class ArrowBytesWriter(name: String, metadata: Map[String, String], factory: VectorFactory)
      extends ArrowAttributeWriter {

    override val vector: VarBinaryVector = factory.apply(name, MinorType.VARBINARY, metadata)

    override def apply(i: Int, value: AnyRef): Unit = {
      if (value == null) {
        vector.setNull(i) // note: calls .setSafe internally
      } else {
        val bytes = value.asInstanceOf[Array[Byte]]
        vector.setSafe(i, bytes, 0, bytes.length)
      }
    }
  }

  class ArrowListWriter(
      name: String,
      binding: ObjectType,
      encoding: SimpleFeatureEncoding,
      metadata: Map[String, String],
      factory: VectorFactory
    ) extends ArrowAttributeWriter {

    override val vector: ListVector = factory.apply(name, MinorType.LIST, metadata)

    private val subWriter =
      ArrowAttributeWriter(null, Seq(binding), None, Map.empty[String, String], encoding, FromList(vector))

    override def apply(i: Int, value: AnyRef): Unit = {
      val start = vector.startNewValue(i)
      // note: null gets converted to empty list
      if (value == null) {
        vector.endValue(i, 0)
      } else {
        val list = value.asInstanceOf[java.util.List[AnyRef]]
        var offset = 0
        while (offset < list.size()) {
          subWriter.apply(start + offset, list.get(offset))
          offset += 1
        }
        vector.endValue(i, offset)
      }
    }
  }

  class ArrowMapWriter(
      name: String,
      keyBinding: ObjectType,
      valueBinding: ObjectType,
      encoding: SimpleFeatureEncoding,
      metadata: Map[String, String],
      factory: VectorFactory
    ) extends ArrowAttributeWriter {

    override val vector: StructVector = factory.apply(name, MinorType.STRUCT, metadata)

    private val keyWriter =
      ArrowAttributeWriter("k", Seq(ObjectType.LIST, keyBinding), None, Map.empty, encoding, FromStruct(vector))
    private val valueWriter =
      ArrowAttributeWriter("v", Seq(ObjectType.LIST, valueBinding), None, Map.empty, encoding, FromStruct(vector))

    override def apply(i: Int, value: AnyRef): Unit = {
      if (value == null) {
        vector.setNull(i)
      } else {
        vector.setIndexDefined(i)
        val map = value.asInstanceOf[java.util.Map[AnyRef, AnyRef]]
        val keys = new java.util.ArrayList[AnyRef](map.size())
        val values = new java.util.ArrayList[AnyRef](map.size())
        map.asScala.foreach { case (k, v) =>
          keys.add(k)
          values.add(v)
        }
        keyWriter.apply(i, keys)
        valueWriter.apply(i, values)
      }
    }
  }
}
