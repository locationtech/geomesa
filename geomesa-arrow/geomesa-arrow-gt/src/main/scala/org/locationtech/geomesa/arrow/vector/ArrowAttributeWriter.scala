/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.vector

import java.nio.charset.StandardCharsets
import java.util.Date

import org.locationtech.jts.geom._
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.{FixedSizeListVector, ListVector, StructVector}
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.pojo.{ArrowType, DictionaryEncoding, FieldType}
import org.locationtech.geomesa.arrow.TypeBindings
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding.Encoding
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding.Encoding.Encoding
import org.locationtech.geomesa.arrow.vector.impl._
import org.locationtech.geomesa.features.serialization.ObjectType
import org.locationtech.geomesa.features.serialization.ObjectType.ObjectType
import org.locationtech.geomesa.filter.function.ProxyIdFunction
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
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

  import scala.collection.JavaConversions._

  /**
    * Writer for feature ID. The return FeatureWriter expects to be passed the entire SimpleFeature, not
    * just the feature ID string (this is to support cached UUIDs).
    *
    * @param vector simple feature vector
    * @param encoding actually write the feature ids, or omit them, in which case the writer is a no-op
    * @param allocator buffer allocator
    * @return feature ID writer
    */
  def id(sft: SimpleFeatureType,
         vector: Option[StructVector],
         encoding: SimpleFeatureEncoding)
        (implicit allocator: BufferAllocator): ArrowAttributeWriter = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    val name = SimpleFeatureVector.FeatureIdField
    val toVector = vector.map(new ToMapChildVector(name, _)).getOrElse(new ToNewVector(name))

    encoding.fids match {
      case None => ArrowNoopWriter

      case Some(Encoding.Min) =>
        val child = toVector[IntVector](FieldType.nullable(MinorType.INT.getType))
        if (sft.isUuid) {
          new ArrowFeatureIdMinimalUuidWriter(child)
        } else {
          new ArrowFeatureIdMinimalStringWriter(child)
        }

      case Some(Encoding.Max) if sft.isUuid =>
        val child = toVector[FixedSizeListVector](FieldType.nullable(new ArrowType.FixedSizeList(2)))
        new ArrowFeatureIdUuidWriter(child)

      case Some(Encoding.Max) =>
        val child = toVector[VarCharVector](FieldType.nullable(MinorType.VARCHAR.getType))
        new ArrowFeatureIdStringWriter(child)
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
            vector: Option[StructVector],
            dictionaries: Map[String, ArrowDictionary],
            encoding: SimpleFeatureEncoding = SimpleFeatureEncoding.Min)
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
            vector: Option[StructVector],
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
            vector: Option[StructVector],
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
            encoding.date match {
              case Encoding.Min => new ArrowDateSecondsWriter(toVector[IntVector](MinorType.INT.getType, null, metadata))
              case Encoding.Max => new ArrowDateMillisWriter(toVector[BigIntVector](MinorType.BIGINT.getType, null, metadata))
            }

          case ObjectType.STRING =>
            new ArrowStringWriter(toVector(MinorType.VARCHAR.getType, null, metadata))

          case ObjectType.INT =>
            new ArrowIntWriter(toVector(MinorType.INT.getType, null, metadata))

          case ObjectType.LONG =>
            new ArrowLongWriter(toVector(MinorType.BIGINT.getType, null, metadata))

          case ObjectType.FLOAT =>
            new ArrowFloatWriter(toVector(MinorType.FLOAT4.getType, null, metadata))

          case ObjectType.DOUBLE =>
            new ArrowDoubleWriter(toVector(MinorType.FLOAT8.getType, null, metadata))

          case ObjectType.BOOLEAN =>
            new ArrowBooleanWriter(toVector(MinorType.BIT.getType, null, metadata))

          case ObjectType.LIST =>
            new ArrowListWriter(toVector(MinorType.LIST.getType, null, metadata), bindings(1), encoding)

          case ObjectType.MAP =>
            new ArrowMapWriter(toVector(MinorType.STRUCT.getType, null, metadata), bindings(1), bindings(2), encoding)

          case ObjectType.BYTES =>
            new ArrowBytesWriter(toVector(MinorType.VARBINARY.getType, null, metadata))

          case ObjectType.JSON =>
            new ArrowStringWriter(toVector(MinorType.VARCHAR.getType, null, metadata))

          case ObjectType.UUID =>
            new ArrowStringWriter(toVector(MinorType.VARCHAR.getType, null, metadata))

          case _ => throw new IllegalArgumentException(s"Unexpected object type ${bindings.head}")
        }

      case Some(dict) =>
        val dictionaryEncoding = dict.encoding
        val dictionaryType = TypeBindings(bindings, encoding)
        if (dictionaryEncoding.getIndexType.getBitWidth == 8) {
          val vector = toVector[TinyIntVector](MinorType.TINYINT.getType, dictionaryEncoding, metadata)
          new ArrowDictionaryByteWriter(vector, dict, dictionaryType)
        } else if (dictionaryEncoding.getIndexType.getBitWidth == 16) {
          val vector = toVector[SmallIntVector](MinorType.SMALLINT.getType, dictionaryEncoding, metadata)
          new ArrowDictionaryShortWriter(vector, dict, dictionaryType)
        } else {
          val vector = toVector[IntVector](MinorType.INT.getType, dictionaryEncoding, metadata)
          new ArrowDictionaryIntWriter(vector, dict, dictionaryType)
        }
    }
  }

  /**
    * Converts a value into a dictionary byte and writes it
    */
  class ArrowDictionaryByteWriter(override val vector: TinyIntVector,
                                  val dictionary: ArrowDictionary,
                                  val dictionaryType: TypeBindings) extends ArrowAttributeWriter {
    // note: nulls get encoded in the dictionary
    override def apply(i: Int, value: AnyRef): Unit = vector.setSafe(i, dictionary.index(value).toByte)
  }

  /**
    * Converts a value into a dictionary short and writes it
    */
  class ArrowDictionaryShortWriter(override val vector: SmallIntVector,
                                   val dictionary: ArrowDictionary,
                                   val dictionaryType: TypeBindings) extends ArrowAttributeWriter {
    // note: nulls get encoded in the dictionary
    override def apply(i: Int, value: AnyRef): Unit = vector.setSafe(i, dictionary.index(value).toShort)
  }

  /**
    * Converts a value into a dictionary int and writes it
    */
  class ArrowDictionaryIntWriter(override val vector: IntVector,
                                 val dictionary: ArrowDictionary,
                                 val dictionaryType: TypeBindings) extends ArrowAttributeWriter {
    // note: nulls get encoded in the dictionary
    override def apply(i: Int, value: AnyRef): Unit = vector.setSafe(i, dictionary.index(value))
  }

  /**
    * Writes geometries - delegates to our JTS geometry vectors
    */
  class ArrowGeometryWriter(toVector: ToVector,
                            binding: ObjectType,
                            metadata: Map[String, String],
                            encoding: Encoding) extends ArrowAttributeWriter {
    private val delegate: GeometryVector[Geometry, FieldVector] = {
      if (binding == ObjectType.POINT) {
        val vector = toVector.apply[FixedSizeListVector](AbstractPointVector.createFieldType(metadata))
        encoding match {
          case Encoding.Min => new PointFloatVector(vector).asInstanceOf[GeometryVector[Geometry, FieldVector]]
          case Encoding.Max => new PointVector(vector).asInstanceOf[GeometryVector[Geometry, FieldVector]]
        }
      } else if (binding == ObjectType.LINESTRING) {
        val vector = toVector.apply[ListVector](AbstractLineStringVector.createFieldType(metadata))
        encoding match {
          case Encoding.Min => new LineStringFloatVector(vector).asInstanceOf[GeometryVector[Geometry, FieldVector]]
          case Encoding.Max => new LineStringVector(vector).asInstanceOf[GeometryVector[Geometry, FieldVector]]
        }
      } else if (binding == ObjectType.POLYGON) {
        val vector = toVector.apply[ListVector](AbstractPolygonVector.createFieldType(metadata))
        encoding match {
          case Encoding.Min => new PolygonFloatVector(vector).asInstanceOf[GeometryVector[Geometry, FieldVector]]
          case Encoding.Max => new PolygonVector(vector).asInstanceOf[GeometryVector[Geometry, FieldVector]]
        }
      } else if (binding == ObjectType.MULTILINESTRING) {
        val vector = toVector.apply[ListVector](AbstractMultiLineStringVector.createFieldType(metadata))
        encoding match {
          case Encoding.Min => new MultiLineStringFloatVector(vector).asInstanceOf[GeometryVector[Geometry, FieldVector]]
          case Encoding.Max => new MultiLineStringVector(vector).asInstanceOf[GeometryVector[Geometry, FieldVector]]
        }
      } else if (binding == ObjectType.MULTIPOLYGON) {
        val vector = toVector.apply[ListVector](AbstractMultiPolygonVector.createFieldType(metadata))
        encoding match {
          case Encoding.Min => new MultiPolygonFloatVector(vector).asInstanceOf[GeometryVector[Geometry, FieldVector]]
          case Encoding.Max => new MultiPolygonVector(vector).asInstanceOf[GeometryVector[Geometry, FieldVector]]
        }
      } else if (binding == ObjectType.MULTIPOINT) {
        val vector = toVector.apply[ListVector](AbstractMultiPointVector.createFieldType(metadata))
        encoding match {
          case Encoding.Min => new MultiPointFloatVector(vector).asInstanceOf[GeometryVector[Geometry, FieldVector]]
          case Encoding.Max => new MultiPointVector(vector).asInstanceOf[GeometryVector[Geometry, FieldVector]]
        }
      } else if (binding == ObjectType.GEOMETRY_COLLECTION) {
        throw new NotImplementedError(s"Geometry type $binding is not supported")
      } else {
        throw new IllegalArgumentException(s"Expected geometry type, got $binding")
      }
    }

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

  class ArrowFeatureIdMinimalUuidWriter(override val vector: IntVector) extends ArrowAttributeWriter {
    override def apply(i: Int, value: AnyRef): Unit = {
      import org.locationtech.geomesa.utils.geotools.Conversions.RichSimpleFeature
      val (msb, lsb) = value.asInstanceOf[SimpleFeature].getUuid
      vector.setSafe(i, ProxyIdFunction.proxyId(msb, lsb))
    }
  }

  class ArrowFeatureIdMinimalStringWriter(override val vector: IntVector) extends ArrowAttributeWriter {
    override def apply(i: Int, value: AnyRef): Unit =
      vector.setSafe(i, ProxyIdFunction.proxyId(value.asInstanceOf[SimpleFeature].getID))
  }

  class ArrowFeatureIdUuidWriter(override val vector: FixedSizeListVector) extends ArrowAttributeWriter {
    private val bits =
      vector.addOrGetVector(FieldType.nullable(new ArrowType.Int(64, true))).getVector.asInstanceOf[BigIntVector]

    override def apply(i: Int, value: AnyRef): Unit = {
      import org.locationtech.geomesa.utils.geotools.Conversions.RichSimpleFeature
      val (msb, lsb) = value.asInstanceOf[SimpleFeature].getUuid
      vector.setNotNull(i)
      bits.setSafe(i * 2, msb)
      bits.setSafe(i * 2 + 1, lsb)
    }
  }

  class ArrowFeatureIdStringWriter(override val vector: VarCharVector) extends ArrowAttributeWriter {
    override def apply(i: Int, value: AnyRef): Unit = {
      val bytes = value.asInstanceOf[SimpleFeature].getID.getBytes(StandardCharsets.UTF_8)
      vector.setSafe(i, bytes, 0, bytes.length)
    }
  }

  class ArrowStringWriter(override val vector: VarCharVector) extends ArrowAttributeWriter {
    override def apply(i: Int, value: AnyRef): Unit = {
      if (value == null) {
        vector.setNull(i) // note: calls .setSafe internally
      } else {
        val bytes = value.toString.getBytes(StandardCharsets.UTF_8)
        vector.setSafe(i, bytes, 0, bytes.length)
      }
    }
  }

  class ArrowIntWriter(override val vector: IntVector) extends ArrowAttributeWriter {
    override def apply(i: Int, value: AnyRef): Unit = {
      if (value == null) {
        vector.setNull(i) // note: calls .setSafe internally
      } else {
        vector.setSafe(i, value.asInstanceOf[Int])
      }
    }

  }

  class ArrowLongWriter(override val vector: BigIntVector) extends ArrowAttributeWriter {
    override def apply(i: Int, value: AnyRef): Unit =
      if (value == null) {
        vector.setNull(i) // note: calls .setSafe internally
      } else {
        vector.setSafe(i, value.asInstanceOf[Long])
      }
  }

  class ArrowFloatWriter(override val vector: Float4Vector) extends ArrowAttributeWriter {
    override def apply(i: Int, value: AnyRef): Unit =
      if (value == null) {
        vector.setNull(i) // note: calls .setSafe internally
      } else {
        vector.setSafe(i, value.asInstanceOf[Float])
      }
  }

  class ArrowDoubleWriter(override val vector: Float8Vector) extends ArrowAttributeWriter {
    override def apply(i: Int, value: AnyRef): Unit =
      if (value == null) {
        vector.setNull(i) // note: calls .setSafe internally
      } else {
        vector.setSafe(i, value.asInstanceOf[Double])
      }
  }

  class ArrowBooleanWriter(override val vector: BitVector) extends ArrowAttributeWriter {
    override def apply(i: Int, value: AnyRef): Unit =
      if (value == null) {
        vector.setNull(i) // note: calls .setSafe internally
      } else {
        vector.setSafe(i, if (value.asInstanceOf[Boolean]) { 1 } else { 0 })
      }
  }

  class ArrowDateMillisWriter(override val vector: BigIntVector) extends ArrowAttributeWriter {
    override def apply(i: Int, value: AnyRef): Unit =
      if (value == null) {
        vector.setNull(i) // note: calls .setSafe internally
      } else {
        vector.setSafe(i, value.asInstanceOf[Date].getTime)
      }
  }

  class ArrowDateSecondsWriter(override val vector: IntVector) extends ArrowAttributeWriter {
    override def apply(i: Int, value: AnyRef): Unit =
      if (value == null) {
        vector.setNull(i) // note: calls .setSafe internally
      } else {
        vector.setSafe(i, (value.asInstanceOf[Date].getTime / 1000L).toInt)
      }
  }

  class ArrowBytesWriter(override val vector: VarBinaryVector) extends ArrowAttributeWriter {
    override def apply(i: Int, value: AnyRef): Unit =
      if (value == null) {
        vector.setNull(i) // note: calls .setSafe internally
      } else {
        val bytes = value.asInstanceOf[Array[Byte]]
        vector.setSafe(i, bytes, 0, bytes.length)
      }
  }

  class ArrowListWriter(override val vector: ListVector,
                        binding: ObjectType,
                        encoding: SimpleFeatureEncoding)
                       (implicit allocator: BufferAllocator) extends ArrowAttributeWriter {
    private val subWriter = ArrowAttributeWriter(Seq(binding), new ToListChildVector(vector), None, Map.empty[String, String], encoding)
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

  class ArrowMapWriter(override val vector: StructVector,
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
        vector.setNull(i)
      } else {
        vector.setIndexDefined(i)
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

  private class ToMapChildVector(val name: String, val container: StructVector) extends ToVector {
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
