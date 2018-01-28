/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.vector

import java.nio.charset.StandardCharsets
import java.util.Date

import com.vividsolutions.jts.geom._
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.{FixedSizeListVector, ListVector, NullableMapVector}
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.pojo.{ArrowType, DictionaryEncoding, FieldType}
import org.locationtech.geomesa.arrow.TypeBindings
import org.locationtech.geomesa.arrow.vector.GeometryVector.GeometryWriter
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.EncodingPrecision.EncodingPrecision
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.{EncodingPrecision, SimpleFeatureEncoding}
import org.locationtech.geomesa.arrow.vector.impl._
import org.locationtech.geomesa.features.serialization.ObjectType
import org.locationtech.geomesa.features.serialization.ObjectType.ObjectType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.SimpleFeatureType

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
  def setValueCount(count: Int): Unit = vector.getMutator.setValueCount(count)

  /**
    * Handle to the underlying field vector being written to
    *
    * @return
    */
  def vector: FieldVector
}

object ArrowAttributeWriter {

  import scala.collection.JavaConversions._

  /**
    * Writer for feature ID
    *
    * @param vector simple feature vector
    * @param encoding actually write the feature ids, or omit them, in which case the writer is a no-op
    * @param allocator buffer allocator
    * @return feature ID writer
    */
  def id(vector: Option[NullableMapVector],
         encoding: SimpleFeatureEncoding)
        (implicit allocator: BufferAllocator): ArrowAttributeWriter = {
    if (encoding.fids) {
      val name = SimpleFeatureVector.FeatureIdField
      ArrowAttributeWriter(name, Seq(ObjectType.STRING), vector, None, Map.empty, null)
    } else {
      ArrowAttributeWriter.ArrowNoopWriter
    }
  }

  /**
    * Creates a sequence of attribute writers for a simple feature type. Each attribute in the feature type
    * will map to a writer in the returned sequence.
    *
    * @param sft simple feature type
    * @param vector optional container arrow vector - child vector will be created in the container, if provided
    * @param dictionaries dictionaries, if any
    * @param encoding encoding options
    * @param allocator buffer allocator
    * @return attribute writers
    */
  def apply(sft: SimpleFeatureType,
            vector: Option[NullableMapVector],
            dictionaries: Map[String, ArrowDictionary],
            encoding: SimpleFeatureEncoding = SimpleFeatureEncoding.min(false))
           (implicit allocator: BufferAllocator): Seq[ArrowAttributeWriter] = {
    sft.getAttributeDescriptors.map { descriptor =>
      val dictionary = dictionaries.get(descriptor.getLocalName)
      apply(sft, descriptor, vector, dictionary, encoding)
    }
  }

  /**
    * Creates a single attribute writer
    *
    * @param sft simple feature type
    * @param descriptor attribute descriptor
    * @param vector optional container arrow vector - child vector will be created in the container, if provided
    * @param dictionary the dictionary for the attribute, if any
    * @param encoding encoding options
    * @param allocator buffer allocator
    * @return
    */
  def apply(sft: SimpleFeatureType,
            descriptor: AttributeDescriptor,
            vector: Option[NullableMapVector],
            dictionary: Option[ArrowDictionary],
            encoding: SimpleFeatureEncoding)
           (implicit allocator: BufferAllocator): ArrowAttributeWriter = {
    val name = descriptor.getLocalName
    val metadata = Map(SimpleFeatureVector.DescriptorKey -> SimpleFeatureTypes.encodeDescriptor(sft, descriptor))
    val bindings = ObjectType.selectType(descriptor)
    apply(name, bindings, vector, dictionary, metadata, encoding)
  }

  /**
    * Low-level method to create a single attribute writer
    *
    * @param name attribute name
    * @param bindings object bindings, the attribute type plus any subtypes (e.g. for lists or maps)
    * @param vector optional container arrow vector - child vector will be created in the container, if provided
    * @param dictionary the dictionary for the attribute, if any
    * @param metadata vector metadata encoded in the field - generally the encoded attribute descriptor
    * @param encoding encoding options
    * @param allocator buffer allocator
    * @return
    */
  def apply(name: String,
            bindings: Seq[ObjectType],
            vector: Option[NullableMapVector],
            dictionary: Option[ArrowDictionary],
            metadata: Map[String, String],
            encoding: SimpleFeatureEncoding)
           (implicit allocator: BufferAllocator): ArrowAttributeWriter = {
    val toVector = vector.map(new ToMapChildVector(name, _)).getOrElse(new ToNewVector(name))
    apply(bindings, toVector, dictionary, metadata, encoding)
  }

  /**
    * Creates a writer for a single attribute
    *
    * @param bindings object bindings, the attribute type plus any subtypes (e.g. for lists or maps)
    * @param toVector the simple feature vector to write to
    * @param dictionary the dictionary for the attribute, if any
    * @param metadata vector metadata encoded in the field - generally the encoded attribute descriptor
    * @param encoding encoding options
    * @param allocator buffer allocator
    * @return
    */
  private def apply(bindings: Seq[ObjectType],
                    toVector: ToVector,
                    dictionary: Option[ArrowDictionary],
                    metadata: Map[String, String],
                    encoding: SimpleFeatureEncoding)
                   (implicit allocator: BufferAllocator): ArrowAttributeWriter = {
    dictionary match {
      case None =>
        bindings.head match {
          case ObjectType.GEOMETRY =>
            new ArrowGeometryWriter(toVector, bindings(1), metadata, encoding.geometry)

          case ObjectType.DATE =>
            if (encoding.date == EncodingPrecision.Min) {
              new ArrowDateSecondsWriter(toVector[NullableIntVector](MinorType.INT.getType, null, metadata))
            } else {
              new ArrowDateMillisWriter(toVector[NullableBigIntVector](MinorType.BIGINT.getType, null, metadata))
            }

          case ObjectType.STRING =>
            new ArrowStringWriter(toVector[NullableVarCharVector](MinorType.VARCHAR.getType, null, metadata))

          case ObjectType.INT =>
            new ArrowIntWriter(toVector[NullableIntVector](MinorType.INT.getType, null, metadata))

          case ObjectType.LONG =>
            new ArrowLongWriter(toVector[NullableBigIntVector](MinorType.BIGINT.getType, null, metadata))

          case ObjectType.FLOAT =>
            new ArrowFloatWriter(toVector[NullableFloat4Vector](MinorType.FLOAT4.getType, null, metadata))

          case ObjectType.DOUBLE =>
            new ArrowDoubleWriter(toVector[NullableFloat8Vector](MinorType.FLOAT8.getType, null, metadata))

          case ObjectType.BOOLEAN =>
            new ArrowBooleanWriter(toVector[NullableBitVector](MinorType.BIT.getType, null, metadata))

          case ObjectType.LIST =>
            val vector = toVector[ListVector](MinorType.LIST.getType, null, metadata)
            new ArrowListWriter(vector, bindings(1), encoding)

          case ObjectType.MAP =>
            val vector = toVector[NullableMapVector](MinorType.MAP.getType, null, metadata)
            new ArrowMapWriter(vector, bindings(1), bindings(2), encoding)

          case ObjectType.BYTES =>
            new ArrowBytesWriter(toVector[NullableVarBinaryVector](MinorType.VARBINARY.getType, null, metadata))

          case ObjectType.JSON =>
            new ArrowStringWriter(toVector[NullableVarCharVector](MinorType.VARCHAR.getType, null, metadata))

          case ObjectType.UUID =>
            new ArrowStringWriter(toVector[NullableVarCharVector](MinorType.VARCHAR.getType, null, metadata))

          case _ => throw new IllegalArgumentException(s"Unexpected object type ${bindings.head}")
        }

      case Some(dict) =>
        val dictionaryEncoding = dict.encoding
        val dictionaryType = TypeBindings(bindings, encoding)
        if (dictionaryEncoding.getIndexType.getBitWidth == 8) {
          val vector = toVector[NullableTinyIntVector](MinorType.TINYINT.getType, dictionaryEncoding, metadata)
          new ArrowDictionaryByteWriter(vector, dict, dictionaryType)
        } else if (dictionaryEncoding.getIndexType.getBitWidth == 16) {
          val vector = toVector[NullableSmallIntVector](MinorType.SMALLINT.getType, dictionaryEncoding, metadata)
          new ArrowDictionaryShortWriter(vector, dict, dictionaryType)
        } else {
          val vector = toVector[NullableIntVector](MinorType.INT.getType, dictionaryEncoding, metadata)
          new ArrowDictionaryIntWriter(vector, dict, dictionaryType)
        }
    }
  }

  /**
    * Converts a value into a dictionary byte and writes it
    */
  class ArrowDictionaryByteWriter(override val vector: NullableTinyIntVector,
                                  val dictionary: ArrowDictionary,
                                  val dictionaryType: TypeBindings) extends ArrowAttributeWriter {

    private val mutator = vector.getMutator

    override def apply(i: Int, value: AnyRef): Unit =
      if (value == null) {
        mutator.setNull(i) // note: calls .setSafe internally
      } else {
        mutator.setSafe(i, dictionary.index(value).toByte)
      }
  }

  /**
    * Converts a value into a dictionary short and writes it
    */
  class ArrowDictionaryShortWriter(override val vector: NullableSmallIntVector,
                                   val dictionary: ArrowDictionary,
                                   val dictionaryType: TypeBindings) extends ArrowAttributeWriter {

    private val mutator = vector.getMutator

    override def apply(i: Int, value: AnyRef): Unit =
      if (value == null) {
        mutator.setNull(i) // note: calls .setSafe internally
      } else {
        mutator.setSafe(i, dictionary.index(value).toShort)
      }
  }

  /**
    * Converts a value into a dictionary int and writes it
    */
  class ArrowDictionaryIntWriter(override val vector: NullableIntVector,
                                 val dictionary: ArrowDictionary,
                                 val dictionaryType: TypeBindings) extends ArrowAttributeWriter {

    private val mutator = vector.getMutator

    override def apply(i: Int, value: AnyRef): Unit =
      if (value == null) {
        mutator.setNull(i) // note: calls .setSafe internally
      } else {
        mutator.setSafe(i, dictionary.index(value))
      }
  }

  /**
    * Writes geometries - delegates to our JTS geometry vectors
    */
  class ArrowGeometryWriter(toVector: ToVector,
                            binding: ObjectType,
                            metadata: Map[String, String],
                            precision: EncodingPrecision) extends ArrowAttributeWriter {
    private val (_vector: FieldVector, delegate: GeometryWriter[Geometry]) = {
      if (binding == ObjectType.POINT) {
        val vector = toVector.apply[FixedSizeListVector](AbstractPointVector.createFieldType(metadata))
        val delegate = precision match {
          case EncodingPrecision.Min => new PointFloatVector(vector).getWriter
          case EncodingPrecision.Max => new PointVector(vector).getWriter
        }
        (vector, delegate.asInstanceOf[GeometryWriter[Geometry]])
      } else if (binding == ObjectType.LINESTRING) {
        val vector = toVector.apply[ListVector](AbstractLineStringVector.createFieldType(metadata))
        val delegate = precision match {
          case EncodingPrecision.Min => new LineStringFloatVector(vector).getWriter
          case EncodingPrecision.Max => new LineStringVector(vector).getWriter
        }
        (vector, delegate.asInstanceOf[GeometryWriter[Geometry]])
      } else if (binding == ObjectType.POLYGON) {
        val vector = toVector.apply[ListVector](AbstractPolygonVector.createFieldType(metadata))
        val delegate = precision match {
          case EncodingPrecision.Min => new PolygonFloatVector(vector).getWriter
          case EncodingPrecision.Max => new PolygonVector(vector).getWriter
        }
        (vector, delegate.asInstanceOf[GeometryWriter[Geometry]])
      } else if (binding == ObjectType.MULTILINESTRING) {
        val vector = toVector.apply[ListVector](AbstractMultiLineStringVector.createFieldType(metadata))
        val delegate = precision match {
          case EncodingPrecision.Min => new MultiLineStringFloatVector(vector).getWriter
          case EncodingPrecision.Max => new MultiLineStringVector(vector).getWriter
        }
        (vector, delegate.asInstanceOf[GeometryWriter[Geometry]])
      } else if (binding == ObjectType.MULTIPOLYGON) {
        val vector = toVector.apply[ListVector](AbstractMultiPolygonVector.createFieldType(metadata))
        val delegate = precision match {
          case EncodingPrecision.Min => new MultiPolygonFloatVector(vector).getWriter
          case EncodingPrecision.Max => new MultiPolygonVector(vector).getWriter
        }
        (vector, delegate.asInstanceOf[GeometryWriter[Geometry]])
      } else if (binding == ObjectType.MULTIPOINT) {
        val vector = toVector.apply[ListVector](AbstractMultiPointVector.createFieldType(metadata))
        val delegate = precision match {
          case EncodingPrecision.Min => new MultiPointFloatVector(vector).getWriter
          case EncodingPrecision.Max => new MultiPointVector(vector).getWriter
        }
        (vector, delegate.asInstanceOf[GeometryWriter[Geometry]])
      } else if (binding == ObjectType.GEOMETRY_COLLECTION) {
        throw new NotImplementedError(s"Geometry type $binding is not supported")
      } else {
        throw new IllegalArgumentException(s"Expected geometry type, got $binding")
      }
    }

    override def vector: FieldVector = _vector
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

  class ArrowStringWriter(override val vector: NullableVarCharVector) extends ArrowAttributeWriter {

    private val mutator = vector.getMutator

    override def apply(i: Int, value: AnyRef): Unit =
      if (value == null) {
        mutator.setNull(i) // note: calls .setSafe internally
      } else {
        val bytes = value.toString.getBytes(StandardCharsets.UTF_8)
        mutator.setSafe(i, bytes, 0, bytes.length)
      }
  }

  class ArrowIntWriter(override val vector: NullableIntVector) extends ArrowAttributeWriter {

    private val mutator = vector.getMutator

    override def apply(i: Int, value: AnyRef): Unit = {
      if (value == null) {
        mutator.setNull(i) // note: calls .setSafe internally
      } else {
        mutator.setSafe(i, value.asInstanceOf[Int])
      }
    }

  }

  class ArrowLongWriter(override val vector: NullableBigIntVector) extends ArrowAttributeWriter {

    private val mutator = vector.getMutator

    override def apply(i: Int, value: AnyRef): Unit =
      if (value == null) {
        mutator.setNull(i) // note: calls .setSafe internally
      } else {
        mutator.setSafe(i, value.asInstanceOf[Long])
      }
  }

  class ArrowFloatWriter(override val vector: NullableFloat4Vector) extends ArrowAttributeWriter {

    private val mutator = vector.getMutator

    override def apply(i: Int, value: AnyRef): Unit =
      if (value == null) {
        mutator.setNull(i) // note: calls .setSafe internally
      } else {
        mutator.setSafe(i, value.asInstanceOf[Float])
      }
  }

  class ArrowDoubleWriter(override val vector: NullableFloat8Vector) extends ArrowAttributeWriter {

    private val mutator = vector.getMutator

    override def apply(i: Int, value: AnyRef): Unit =
      if (value == null) {
        mutator.setNull(i) // note: calls .setSafe internally
      } else {
        mutator.setSafe(i, value.asInstanceOf[Double])
      }
  }

  class ArrowBooleanWriter(override val vector: NullableBitVector) extends ArrowAttributeWriter {

    private val mutator = vector.getMutator

    override def apply(i: Int, value: AnyRef): Unit =
      if (value == null) {
        mutator.setNull(i) // note: calls .setSafe internally
      } else {
        mutator.setSafe(i, if (value.asInstanceOf[Boolean]) { 1 } else { 0 })
      }
  }

  class ArrowDateMillisWriter(override val vector: NullableBigIntVector) extends ArrowAttributeWriter {

    private val mutator = vector.getMutator

    override def apply(i: Int, value: AnyRef): Unit =
      if (value == null) {
        mutator.setNull(i) // note: calls .setSafe internally
      } else {
        mutator.setSafe(i, value.asInstanceOf[Date].getTime)
      }
  }

  class ArrowDateSecondsWriter(override val vector: NullableIntVector) extends ArrowAttributeWriter {

    private val mutator = vector.getMutator

    override def apply(i: Int, value: AnyRef): Unit =
      if (value == null) {
        mutator.setNull(i) // note: calls .setSafe internally
      } else {
        mutator.setSafe(i, (value.asInstanceOf[Date].getTime / 1000L).toInt)
      }
  }

  class ArrowBytesWriter(override val vector: NullableVarBinaryVector) extends ArrowAttributeWriter {

    private val mutator = vector.getMutator

    override def apply(i: Int, value: AnyRef): Unit =
      if (value == null) {
        mutator.setNull(i) // note: calls .setSafe internally
      } else {
        val bytes = value.asInstanceOf[Array[Byte]]
        mutator.setSafe(i, bytes, 0, bytes.length)
      }
  }

  class ArrowListWriter(override val vector: ListVector,
                        binding: ObjectType,
                        encoding: SimpleFeatureEncoding)
                       (implicit allocator: BufferAllocator) extends ArrowAttributeWriter {
    private val subWriter = ArrowAttributeWriter(Seq(binding), new ToListChildVector(vector), None, Map.empty[String, String], encoding)
    override def apply(i: Int, value: AnyRef): Unit = {
      val start = vector.getMutator.startNewValue(i)
      // note: null gets converted to empty list
      if (value == null) {
        vector.getMutator.endValue(i, 0)
      } else {
        val list = value.asInstanceOf[java.util.List[AnyRef]]
        var offset = 0
        while (offset < list.size()) {
          subWriter.apply(start + offset, list.get(offset))
          offset += 1
        }
        vector.getMutator.endValue(i, offset)
      }
    }
  }

  class ArrowMapWriter(override val vector: NullableMapVector,
                       keyBinding: ObjectType,
                       valueBinding: ObjectType,
                       encoding: SimpleFeatureEncoding)
                      (implicit allocator: BufferAllocator) extends ArrowAttributeWriter {
    private val keyVector = new ToMapChildVector("k", vector).apply[ListVector](FieldType.nullable(ArrowType.List.INSTANCE))
    private val valueVector = new ToMapChildVector("v", vector).apply[ListVector](FieldType.nullable(ArrowType.List.INSTANCE))
    private val keyWriter = new ArrowListWriter(keyVector, keyBinding, encoding)
    private val valueWriter = new ArrowListWriter(valueVector, valueBinding, encoding)
    override def apply(i: Int, value: AnyRef): Unit = {
      if (value == null) {
        vector.getMutator.setNull(i)
      } else {
        vector.getMutator.setIndexDefined(i)
        val map = value.asInstanceOf[java.util.Map[AnyRef, AnyRef]]
        val keys = new java.util.ArrayList[AnyRef](map.size())
        val values = new java.util.ArrayList[AnyRef](map.size())
        map.foreach { case (k, v) =>
          keys.add(k)
          values.add(v)
        }
        keyWriter.apply(i, keys)
        valueWriter.apply(i, values)
      }
    }
  }

  private sealed trait ToVector {
    def apply[T <: FieldVector](arrowType: ArrowType,
                                dictionary: DictionaryEncoding,
                                metadata: Map[String, String]): T =
      apply(new FieldType(true, arrowType, dictionary, metadata))

    def apply[T <: FieldVector](fieldType: FieldType): T
  }

  private class ToMapChildVector(val name: String, val container: NullableMapVector) extends ToVector {
    override def apply[T <: FieldVector](fieldType: FieldType): T = {
      var child = container.getChild(name)
      if (child == null) {
        child = container.addOrGet(name, fieldType, classOf[FieldVector])
        child.allocateNew()
      }
      child.asInstanceOf[T]
    }
  }

  private class ToListChildVector(val container: ListVector) extends ToVector {
    override def apply[T <: FieldVector](fieldType: FieldType): T = {
      var child = container.getDataVector.asInstanceOf[T]
      if (child == ZeroVector.INSTANCE) {
        child = container.addOrGetVector[T](fieldType).getVector
        child.allocateNew()
      }
      child
    }
  }

  private class ToNewVector(val name: String)(implicit allocator: BufferAllocator) extends ToVector {
    override def apply[T <: FieldVector](fieldType: FieldType): T = {
      val vector = fieldType.createNewSingleVector(name, allocator, null).asInstanceOf[T]
      vector.allocateNew()
      vector
    }
  }
}
