/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow
package vector

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 0591c93159 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d1928d9633 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
import java.nio.charset.StandardCharsets
import java.util.{Date, UUID}
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> bcd39d6dd9 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> a5af53dbe1 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
>>>>>>> 728c6f1ae3 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> 728c6f1ae (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
>>>>>>> 022e7e92fe (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> 728c6f1ae (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
>>>>>>> 35d3871b34 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 728c6f1ae (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
>>>>>>> b51dd8f02f (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 75fa4fbac8 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 822d0242c (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
>>>>>>> 0baa2c07aa (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 9e2da6708 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
import java.nio.charset.StandardCharsets
import java.util.{Date, UUID}
>>>>>>> 728c6f1ae (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 822d0242c6 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
=======
import java.nio.charset.StandardCharsets
import java.util.{Date, UUID}
>>>>>>> 728c6f1ae (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
>>>>>>> 9e2da6708a (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 75fa4fbac8 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 822d0242c (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
<<<<<<< HEAD
>>>>>>> 0baa2c07aa (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 9e2da6708 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
<<<<<<< HEAD
>>>>>>> 75fa4fbac8 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
=======
import java.nio.charset.StandardCharsets
import java.util.{Date, UUID}
>>>>>>> 728c6f1ae (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
>>>>>>> a362209f73 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
=======
import java.nio.charset.StandardCharsets
import java.util.{Date, UUID}
<<<<<<< HEAD
>>>>>>> 728c6f1ae3 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
<<<<<<< HEAD
>>>>>>> bb9821e80f (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
=======
>>>>>>> 728c6f1ae (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
>>>>>>> 022e7e92fe (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
>>>>>>> 2dc433b627 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> bcd39d6dd9 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
=======
>>>>>>> 728c6f1ae (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
>>>>>>> b51dd8f02f (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
>>>>>>> a5af53dbe1 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
import java.nio.charset.StandardCharsets
import java.util.{Date, UUID}
>>>>>>> 728c6f1ae (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
<<<<<<< HEAD
>>>>>>> 822d0242c6 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
<<<<<<< HEAD
>>>>>>> 0591c93159 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
import java.nio.charset.StandardCharsets
import java.util.{Date, UUID}
>>>>>>> 728c6f1ae (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
>>>>>>> 9e2da6708a (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
>>>>>>> d1928d9633 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.{FixedSizeListVector, ListVector, StructVector}
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.pojo.{ArrowType, FieldType}
import org.geotools.api.feature.`type`.AttributeDescriptor
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.locationtech.geomesa.arrow.jts._
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding.Encoding
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding.Encoding.Encoding
import org.locationtech.geomesa.filter.function.ProxyIdFunction
import org.locationtech.geomesa.utils.geotools.ObjectType.ObjectType
import org.locationtech.geomesa.utils.geotools.{ObjectType, SimpleFeatureTypes}
import org.locationtech.jts.geom._

import java.nio.charset.StandardCharsets
import java.util.{Date, UUID}

/**
  * Writes a simple feature attribute to an arrow vector
  */
trait ArrowAttributeWriter {

  def name: String

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
      case None => ArrowNoFidWriter
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
    }.toSeq
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
        ArrowAttributeWriter.dictionary(name, dict, bindings.head == ObjectType.LIST, metadata, factory)

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
          case ObjectType.UUID     => new ArrowUuidWriter(name, metadata, factory)
          case _ => throw new IllegalArgumentException(s"Unexpected object type ${bindings.head}")
        }
    }
  }

  private def dictionary(
      name: String,
      dictionary: ArrowDictionary,
      isList: Boolean,
      metadata: Map[String, String],
      factory: VectorFactory): ArrowAttributeWriter = {
    (dictionary.encoding.getIndexType.getBitWidth, isList) match {
      case (8,  false) => new ArrowDictionaryByteWriter(name, dictionary, metadata, factory)
      case (16, false) => new ArrowDictionaryShortWriter(name, dictionary, metadata, factory)
      case (32, false) => new ArrowDictionaryIntWriter(name, dictionary, metadata, factory)
      case (8,  true)  => new ArrowListDictionaryByteWriter(name, dictionary, metadata, factory)
      case (16, true)  => new ArrowListDictionaryShortWriter(name, dictionary, metadata, factory)
      case (32, true)  => new ArrowListDictionaryIntWriter(name, dictionary, metadata, factory)
      case (w, _)      => throw new IllegalArgumentException(s"Unsupported dictionary encoding width: $w")
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
    new ArrowGeometryWriter(name, vector.asInstanceOf[GeometryVector[Geometry, FieldVector]])
  }

  trait ArrowDictionaryWriter extends ArrowAttributeWriter {
    def dictionary: ArrowDictionary
  }

  /**
   * Converts a value into a dictionary encoded byte and writes it
   *
   * @param name attribute/field name
   * @param dictionary dictionary values
   * @param metadata field metadata
   * @param factory vector factory
   */
  class ArrowDictionaryByteWriter(
      val name: String,
      val dictionary: ArrowDictionary,
      metadata: Map[String, String],
      factory: VectorFactory
    ) extends ArrowDictionaryWriter {

    override val vector: TinyIntVector = factory.apply(name, MinorType.TINYINT, dictionary.encoding, metadata)

    // note: nulls get encoded in the dictionary
    override def apply(i: Int, value: AnyRef): Unit = vector.setSafe(i, dictionary.index(value).toByte)
  }

  /**
   * Converts a list value into a list of dictionary bytes and writes it
   *
   * @param name attribute/field name
   * @param dictionary dictionary values
   * @param metadata field metadata
   * @param factory vector factory
   */
  class ArrowListDictionaryByteWriter(
      val name: String,
      val dictionary: ArrowDictionary,
      metadata: Map[String, String],
      factory: VectorFactory
    ) extends ArrowDictionaryWriter {

    override val vector: ListVector = factory.apply(name, MinorType.LIST, metadata)

    private val inner: TinyIntVector =
      FromList(vector).apply(null, MinorType.TINYINT, dictionary.encoding, Map.empty)

    override def apply(i: Int, value: AnyRef): Unit = {
      if (vector.getLastSet >= i) {
        vector.setLastSet(i - 1)
      }
      val start = vector.startNewValue(i)
      // note: null gets converted to empty list
      if (value == null) {
        vector.endValue(i, 0)
      } else {
        val list = value.asInstanceOf[java.util.List[AnyRef]]
        var offset = 0
        while (offset < list.size()) {
          // note: nulls get encoded in the dictionary
          inner.setSafe(start + offset, dictionary.index(list.get(offset)).toByte)
          offset += 1
        }
        vector.endValue(i, offset)
      }
    }
  }

  /**
   * Converts a value into a dictionary encoded short and writes it
   *
   * @param name attribute/field name
   * @param dictionary dictionary values
   * @param metadata field metadata
   * @param factory vector factory
   */
  class ArrowDictionaryShortWriter(
      val name: String,
      val dictionary: ArrowDictionary,
      metadata: Map[String, String],
      factory: VectorFactory
    ) extends ArrowDictionaryWriter {

    override val vector: SmallIntVector = factory.apply(name, MinorType.SMALLINT, dictionary.encoding, metadata)

    // note: nulls get encoded in the dictionary
    override def apply(i: Int, value: AnyRef): Unit = vector.setSafe(i, dictionary.index(value).toShort)
  }

  /**
   * Converts a list value into a list of dictionary shorts and writes it
   *
   * @param name attribute/field name
   * @param dictionary dictionary values
   * @param metadata field metadata
   * @param factory vector factory
   */
  class ArrowListDictionaryShortWriter(
      val name: String,
      val dictionary: ArrowDictionary,
      metadata: Map[String, String],
      factory: VectorFactory
    ) extends ArrowDictionaryWriter {

    override val vector: ListVector = factory.apply(name, MinorType.LIST, metadata)

    private val inner: SmallIntVector =
      FromList(vector).apply(null, MinorType.SMALLINT, dictionary.encoding, Map.empty)

    override def apply(i: Int, value: AnyRef): Unit = {
      if (vector.getLastSet >= i) {
        vector.setLastSet(i - 1)
      }
      val start = vector.startNewValue(i)
      // note: null gets converted to empty list
      if (value == null) {
        vector.endValue(i, 0)
      } else {
        val list = value.asInstanceOf[java.util.List[AnyRef]]
        var offset = 0
        while (offset < list.size()) {
          // note: nulls get encoded in the dictionary
          inner.setSafe(start + offset, dictionary.index(list.get(offset)).toShort)
          offset += 1
        }
        vector.endValue(i, offset)
      }
    }
  }

  /**
   * Converts a value into a dictionary encoded int and writes it
   *
   * @param name attribute/field name
   * @param dictionary dictionary values
   * @param metadata field metadata
   * @param factory vector factory
   */
  class ArrowDictionaryIntWriter(
      val name: String,
      val dictionary: ArrowDictionary,
      metadata: Map[String, String],
      factory: VectorFactory
    ) extends ArrowDictionaryWriter {

    override val vector: IntVector = factory.apply(name, MinorType.INT, dictionary.encoding, metadata)

    // note: nulls get encoded in the dictionary
    override def apply(i: Int, value: AnyRef): Unit = vector.setSafe(i, dictionary.index(value))
  }

  /**
   * Converts a list value into a list of dictionary ints and writes it
   *
   * @param name attribute/field name
   * @param dictionary dictionary values
   * @param metadata field metadata
   * @param factory vector factory
   */
  class ArrowListDictionaryIntWriter(
      val name: String,
      val dictionary: ArrowDictionary,
      metadata: Map[String, String],
      factory: VectorFactory
    ) extends ArrowDictionaryWriter {

    override val vector: ListVector = factory.apply(name, MinorType.LIST, metadata)

    private val inner: IntVector = FromList(vector).apply(null, MinorType.INT, dictionary.encoding, Map.empty)

    override def apply(i: Int, value: AnyRef): Unit = {
      if (vector.getLastSet >= i) {
        vector.setLastSet(i - 1)
      }
      val start = vector.startNewValue(i)
      // note: null gets converted to empty list
      if (value == null) {
        vector.endValue(i, 0)
      } else {
        val list = value.asInstanceOf[java.util.List[AnyRef]]
        var offset = 0
        while (offset < list.size()) {
          // note: nulls get encoded in the dictionary
          inner.setSafe(start + offset, dictionary.index(list.get(offset)))
          offset += 1
        }
        vector.endValue(i, offset)
      }
    }
  }

  /**
    * Writes geometries - delegates to our JTS geometry vectors
    */
  class ArrowGeometryWriter(val name: String, delegate: GeometryVector[Geometry, FieldVector])
      extends ArrowAttributeWriter {

    override def vector: FieldVector = delegate.getVector

    // note: delegate handles nulls
    override def apply(i: Int, value: AnyRef): Unit = delegate.set(i, value.asInstanceOf[Geometry])

    override def setValueCount(count: Int): Unit = delegate.setValueCount(count)
  }

  /**
    * Doesn't actually write anything
    */
  object ArrowNoFidWriter extends ArrowAttributeWriter {
    override def name: String = SimpleFeatureVector.FeatureIdField
    override def vector: FieldVector = null
    override def apply(i: Int, value: AnyRef): Unit = {}
    override def setValueCount(count: Int): Unit = {}
  }

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> bb9821e80f (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> 2dc433b627 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> bcd39d6dd9 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> a5af53dbe1 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 0591c93159 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> c3b8fded7f (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> ef39717849 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> 2bc567026a (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d1928d9633 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> f4f002819f (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> 4ae6269900 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
  class ArrowUuidWriter(val name: String, metadata: Map[String, String], factory: VectorFactory)
=======
  class ArrowUuidWriter(name: String, metadata: Map[String, String], factory: VectorFactory)
>>>>>>> 728c6f1ae3 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 35d3871b34 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b51dd8f02f (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
=======
=======
>>>>>>> 2dc433b627 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> bcd39d6dd9 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
=======
>>>>>>> b51dd8f02f (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
>>>>>>> a5af53dbe1 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
  @deprecated("replaced with ArrowNoFidWriter")
  object ArrowNoopWriter extends ArrowAttributeWriter {
    override def name: String = SimpleFeatureVector.FeatureIdField
    override def vector: FieldVector = null
    override def apply(i: Int, value: AnyRef): Unit = {}
    override def setValueCount(count: Int): Unit = {}
  }

    class ArrowUuidWriter(val name: String, metadata: Map[String, String], factory: VectorFactory)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
  class ArrowUuidWriter(name: String, metadata: Map[String, String], factory: VectorFactory)
>>>>>>> 728c6f1ae (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 022e7e92fe (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> 35d3871b34 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> b51dd8f02f (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 64f0137ab3 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> 9d075f0a11 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> ac5c5646a6 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 0baa2c07aa (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> 5dca98162e (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> fd69508ae9 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> 80e4f4cff5 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 75fa4fbac8 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> d9505f01f0 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> e541703a9c (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> ebda10316a (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
  class ArrowUuidWriter(val name: String, metadata: Map[String, String], factory: VectorFactory)
=======
=======
>>>>>>> 35d3871b3 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> b51dd8f02 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
  @deprecated("replaced with ArrowNoFidWriter")
  object ArrowNoopWriter extends ArrowAttributeWriter {
    override def name: String = SimpleFeatureVector.FeatureIdField
    override def vector: FieldVector = null
    override def apply(i: Int, value: AnyRef): Unit = {}
    override def setValueCount(count: Int): Unit = {}
  }

    class ArrowUuidWriter(val name: String, metadata: Map[String, String], factory: VectorFactory)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 022e7e92f (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> 35d3871b3 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> b51dd8f02 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
  class ArrowUuidWriter(name: String, metadata: Map[String, String], factory: VectorFactory)
>>>>>>> 728c6f1ae (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
<<<<<<< HEAD
>>>>>>> 822d0242c6 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
  class ArrowUuidWriter(val name: String, metadata: Map[String, String], factory: VectorFactory)
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
  class ArrowUuidWriter(val name: String, metadata: Map[String, String], factory: VectorFactory)
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 016aa01381 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> d8a0c13d06 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> 92c426af90 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
  class ArrowUuidWriter(val name: String, metadata: Map[String, String], factory: VectorFactory)
=======
=======
>>>>>>> 35d3871b3 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> b51dd8f02 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
  @deprecated("replaced with ArrowNoFidWriter")
  object ArrowNoopWriter extends ArrowAttributeWriter {
    override def name: String = SimpleFeatureVector.FeatureIdField
    override def vector: FieldVector = null
    override def apply(i: Int, value: AnyRef): Unit = {}
    override def setValueCount(count: Int): Unit = {}
  }

    class ArrowUuidWriter(val name: String, metadata: Map[String, String], factory: VectorFactory)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 022e7e92f (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> 35d3871b3 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> b51dd8f02 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
  class ArrowUuidWriter(name: String, metadata: Map[String, String], factory: VectorFactory)
>>>>>>> 728c6f1ae (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
<<<<<<< HEAD
>>>>>>> 9e2da6708a (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> 64f0137ab (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> 9d075f0a1 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> ac5c5646a (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
  class ArrowUuidWriter(val name: String, metadata: Map[String, String], factory: VectorFactory)
=======
=======
>>>>>>> 35d3871b3 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> b51dd8f02 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
  @deprecated("replaced with ArrowNoFidWriter")
  object ArrowNoopWriter extends ArrowAttributeWriter {
    override def name: String = SimpleFeatureVector.FeatureIdField
    override def vector: FieldVector = null
    override def apply(i: Int, value: AnyRef): Unit = {}
    override def setValueCount(count: Int): Unit = {}
  }

    class ArrowUuidWriter(val name: String, metadata: Map[String, String], factory: VectorFactory)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 022e7e92f (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> 35d3871b3 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> b51dd8f02 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
  class ArrowUuidWriter(name: String, metadata: Map[String, String], factory: VectorFactory)
>>>>>>> 728c6f1ae (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
<<<<<<< HEAD
>>>>>>> 822d0242c (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
<<<<<<< HEAD
>>>>>>> 0baa2c07aa (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
=======
  class ArrowUuidWriter(val name: String, metadata: Map[String, String], factory: VectorFactory)
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
  class ArrowUuidWriter(val name: String, metadata: Map[String, String], factory: VectorFactory)
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> 016aa0138 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> d8a0c13d0 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> 92c426af9 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
  class ArrowUuidWriter(val name: String, metadata: Map[String, String], factory: VectorFactory)
=======
=======
>>>>>>> 35d3871b3 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> b51dd8f02 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
  @deprecated("replaced with ArrowNoFidWriter")
  object ArrowNoopWriter extends ArrowAttributeWriter {
    override def name: String = SimpleFeatureVector.FeatureIdField
    override def vector: FieldVector = null
    override def apply(i: Int, value: AnyRef): Unit = {}
    override def setValueCount(count: Int): Unit = {}
  }

    class ArrowUuidWriter(val name: String, metadata: Map[String, String], factory: VectorFactory)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 022e7e92f (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> 35d3871b3 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> b51dd8f02 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
  class ArrowUuidWriter(name: String, metadata: Map[String, String], factory: VectorFactory)
>>>>>>> 728c6f1ae (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
<<<<<<< HEAD
>>>>>>> 9e2da6708 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
<<<<<<< HEAD
>>>>>>> 75fa4fbac8 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 0a8feaa57c (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> 922f693ef9 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> 4d7e13c598 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
  class ArrowUuidWriter(val name: String, metadata: Map[String, String], factory: VectorFactory)
=======
=======
>>>>>>> 35d3871b3 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> b51dd8f02 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
  @deprecated("replaced with ArrowNoFidWriter")
  object ArrowNoopWriter extends ArrowAttributeWriter {
    override def name: String = SimpleFeatureVector.FeatureIdField
    override def vector: FieldVector = null
    override def apply(i: Int, value: AnyRef): Unit = {}
    override def setValueCount(count: Int): Unit = {}
  }

    class ArrowUuidWriter(val name: String, metadata: Map[String, String], factory: VectorFactory)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 022e7e92f (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> 35d3871b3 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> b51dd8f02 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
  class ArrowUuidWriter(name: String, metadata: Map[String, String], factory: VectorFactory)
>>>>>>> 728c6f1ae (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
<<<<<<< HEAD
>>>>>>> a362209f73 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> bb9821e80f (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
=======
  class ArrowUuidWriter(name: String, metadata: Map[String, String], factory: VectorFactory)
>>>>>>> 728c6f1ae (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 022e7e92fe (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
<<<<<<< HEAD
>>>>>>> 2dc433b627 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
=======
>>>>>>> 35d3871b34 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
<<<<<<< HEAD
>>>>>>> bcd39d6dd9 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
=======
>>>>>>> b51dd8f02f (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
>>>>>>> a5af53dbe1 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> 64f0137ab3 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> 9d075f0a11 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> ac5c5646a6 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
  class ArrowUuidWriter(val name: String, metadata: Map[String, String], factory: VectorFactory)
=======
=======
>>>>>>> 35d3871b3 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> b51dd8f02 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
  @deprecated("replaced with ArrowNoFidWriter")
  object ArrowNoopWriter extends ArrowAttributeWriter {
    override def name: String = SimpleFeatureVector.FeatureIdField
    override def vector: FieldVector = null
    override def apply(i: Int, value: AnyRef): Unit = {}
    override def setValueCount(count: Int): Unit = {}
  }

    class ArrowUuidWriter(val name: String, metadata: Map[String, String], factory: VectorFactory)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 022e7e92f (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> 35d3871b3 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> b51dd8f02 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
  class ArrowUuidWriter(name: String, metadata: Map[String, String], factory: VectorFactory)
>>>>>>> 728c6f1ae (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
<<<<<<< HEAD
>>>>>>> 822d0242c6 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
<<<<<<< HEAD
>>>>>>> 0591c93159 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
=======
  class ArrowUuidWriter(val name: String, metadata: Map[String, String], factory: VectorFactory)
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
=======
  class ArrowUuidWriter(val name: String, metadata: Map[String, String], factory: VectorFactory)
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> 016aa01381 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> d8a0c13d06 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
  class ArrowUuidWriter(val name: String, metadata: Map[String, String], factory: VectorFactory)
=======
=======
>>>>>>> 35d3871b3 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
  @deprecated("replaced with ArrowNoFidWriter")
  object ArrowNoopWriter extends ArrowAttributeWriter {
    override def name: String = SimpleFeatureVector.FeatureIdField
    override def vector: FieldVector = null
    override def apply(i: Int, value: AnyRef): Unit = {}
    override def setValueCount(count: Int): Unit = {}
  }

    class ArrowUuidWriter(val name: String, metadata: Map[String, String], factory: VectorFactory)
<<<<<<< HEAD
>>>>>>> 022e7e92f (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> 35d3871b3 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
  class ArrowUuidWriter(name: String, metadata: Map[String, String], factory: VectorFactory)
>>>>>>> 728c6f1ae (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
>>>>>>> 9e2da6708a (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
>>>>>>> d1928d9633 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
    extends ArrowAttributeWriter {
    val fieldType: FieldType = new FieldType(true, new ArrowType.FixedSizeList(2), null, metadata.asJava)
    override val vector: FixedSizeListVector = factory.apply(name, fieldType)

    private val bits = {
      val result = vector.addOrGetVector[BigIntVector](FieldType.nullable(MinorType.BIGINT.getType))
      if (result.isCreated) {
        result.getVector.allocateNew()
      }
      result.getVector
    }

    override def apply(i: Int, value: AnyRef): Unit = {
      if (value == null) {
        vector.setNull(i) // note: calls .setSafe internally
      } else {
        val uuid = value.asInstanceOf[UUID]
        val (msb, lsb) = (uuid.getMostSignificantBits, uuid.getLeastSignificantBits)
        vector.setNotNull(i)
        bits.setSafe(i * 2, msb)
        bits.setSafe(i * 2 + 1, lsb)
      }
    }
  }

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 0591c93159 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d1928d9633 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
  class ArrowFeatureIdMinimalUuidWriter(val name: String, factory: VectorFactory)
=======
  class ArrowFeatureIdMinimalUuidWriter(name: String, factory: VectorFactory)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> bcd39d6dd9 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> a5af53dbe1 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
>>>>>>> 728c6f1ae3 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> 728c6f1ae (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
>>>>>>> 022e7e92fe (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> 728c6f1ae (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
>>>>>>> 35d3871b34 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 728c6f1ae (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
>>>>>>> b51dd8f02f (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
  class ArrowFeatureIdMinimalUuidWriter(val name: String, factory: VectorFactory)
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 822d0242c (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
>>>>>>> 0baa2c07aa (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 75fa4fbac8 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 822d0242c (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 9e2da6708 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
<<<<<<< HEAD
>>>>>>> 75fa4fbac8 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 0591c93159 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
  class ArrowFeatureIdMinimalUuidWriter(val name: String, factory: VectorFactory)
=======
  class ArrowFeatureIdMinimalUuidWriter(name: String, factory: VectorFactory)
>>>>>>> 728c6f1ae (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 822d0242c6 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
=======
  class ArrowFeatureIdMinimalUuidWriter(val name: String, factory: VectorFactory)
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
  class ArrowFeatureIdMinimalUuidWriter(val name: String, factory: VectorFactory)
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d1928d9633 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
  class ArrowFeatureIdMinimalUuidWriter(val name: String, factory: VectorFactory)
=======
  class ArrowFeatureIdMinimalUuidWriter(name: String, factory: VectorFactory)
>>>>>>> 728c6f1ae (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9e2da6708a (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
=======
  class ArrowFeatureIdMinimalUuidWriter(val name: String, factory: VectorFactory)
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 75fa4fbac8 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
  class ArrowFeatureIdMinimalUuidWriter(val name: String, factory: VectorFactory)
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 822d0242c (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
<<<<<<< HEAD
>>>>>>> 0baa2c07aa (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
=======
=======
  class ArrowFeatureIdMinimalUuidWriter(val name: String, factory: VectorFactory)
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
=======
  class ArrowFeatureIdMinimalUuidWriter(val name: String, factory: VectorFactory)
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 9e2da6708 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
<<<<<<< HEAD
>>>>>>> 75fa4fbac8 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
=======
=======
  class ArrowFeatureIdMinimalUuidWriter(val name: String, factory: VectorFactory)
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
  class ArrowFeatureIdMinimalUuidWriter(val name: String, factory: VectorFactory)
=======
  class ArrowFeatureIdMinimalUuidWriter(name: String, factory: VectorFactory)
>>>>>>> 728c6f1ae (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
>>>>>>> a362209f73 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
=======
  class ArrowFeatureIdMinimalUuidWriter(val name: String, factory: VectorFactory)
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
  class ArrowFeatureIdMinimalUuidWriter(val name: String, factory: VectorFactory)
=======
  class ArrowFeatureIdMinimalUuidWriter(name: String, factory: VectorFactory)
<<<<<<< HEAD
>>>>>>> 728c6f1ae3 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
<<<<<<< HEAD
>>>>>>> bb9821e80f (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
=======
>>>>>>> 728c6f1ae (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
>>>>>>> 022e7e92fe (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
>>>>>>> 2dc433b627 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> bcd39d6dd9 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
=======
>>>>>>> 728c6f1ae (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
>>>>>>> b51dd8f02f (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
>>>>>>> a5af53dbe1 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 822d0242c6 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
>>>>>>> 0591c93159 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
<<<<<<< HEAD
>>>>>>> 822d0242c6 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
=======
=======
  class ArrowFeatureIdMinimalUuidWriter(val name: String, factory: VectorFactory)
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
=======
  class ArrowFeatureIdMinimalUuidWriter(val name: String, factory: VectorFactory)
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e2da6708a (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
>>>>>>> d1928d9633 (GEOMESA-3137 Arrow - UUID attribute types cause queries to fail (#2809))
      extends ArrowAttributeWriter {

    override val vector: IntVector = factory.apply(name, MinorType.INT, Map.empty)

    override def apply(i: Int, value: AnyRef): Unit = {
      import org.locationtech.geomesa.utils.geotools.Conversions.RichSimpleFeature
      val (msb, lsb) = value.asInstanceOf[SimpleFeature].getUuid
      vector.setSafe(i, ProxyIdFunction.proxyId(msb, lsb))
    }
  }

  class ArrowFeatureIdMinimalStringWriter(val name: String, factory: VectorFactory)
      extends ArrowAttributeWriter {

    override val vector: IntVector = factory.apply(name, MinorType.INT, Map.empty)

    override def apply(i: Int, value: AnyRef): Unit =
      vector.setSafe(i, ProxyIdFunction.proxyId(value.asInstanceOf[SimpleFeature].getID))
  }

  class ArrowFeatureIdUuidWriter(val name: String, factory: VectorFactory)
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

  class ArrowFeatureIdStringWriter(val name: String, factory: VectorFactory)
      extends ArrowAttributeWriter {

    override val vector: VarCharVector = factory.apply(name, MinorType.VARCHAR, Map.empty)

    override def apply(i: Int, value: AnyRef): Unit = {
      val bytes = value.asInstanceOf[SimpleFeature].getID.getBytes(StandardCharsets.UTF_8)
      vector.setSafe(i, bytes, 0, bytes.length)
    }
  }

  class ArrowStringWriter(val name: String, metadata: Map[String, String], factory: VectorFactory)
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

  class ArrowIntWriter(val name: String, metadata: Map[String, String], factory: VectorFactory)
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

  class ArrowLongWriter(val name: String, metadata: Map[String, String], factory: VectorFactory)
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

  class ArrowFloatWriter(val name: String, metadata: Map[String, String], factory: VectorFactory)
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

  class ArrowDoubleWriter(val name: String, metadata: Map[String, String], factory: VectorFactory)
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

  class ArrowBooleanWriter(val name: String, metadata: Map[String, String], factory: VectorFactory)
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

  class ArrowDateMillisWriter(val name: String, metadata: Map[String, String], factory: VectorFactory)
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

  class ArrowDateSecondsWriter(val name: String, metadata: Map[String, String], factory: VectorFactory)
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

  class ArrowBytesWriter(val name: String, metadata: Map[String, String], factory: VectorFactory)
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
      val name: String,
      binding: ObjectType,
      encoding: SimpleFeatureEncoding,
      metadata: Map[String, String],
      factory: VectorFactory
    ) extends ArrowAttributeWriter {

    override val vector: ListVector = factory.apply(name, MinorType.LIST, metadata)

    private val subWriter =
      ArrowAttributeWriter(null, Seq(binding), None, Map.empty[String, String], encoding, FromList(vector))

    override def apply(i: Int, value: AnyRef): Unit = {
      if (vector.getLastSet >= i) {
        vector.setLastSet(i - 1)
      }
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
      val name: String,
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
