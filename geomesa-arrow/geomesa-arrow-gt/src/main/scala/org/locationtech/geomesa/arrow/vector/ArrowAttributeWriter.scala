/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
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
import org.apache.arrow.vector.complex.writer.BaseWriter.{ListWriter, MapWriter}
import org.apache.arrow.vector.complex.writer._
import org.apache.arrow.vector.complex.{ListVector, NullableMapVector}
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.pojo.{ArrowType, FieldType}
import org.locationtech.geomesa.arrow.TypeBindings
import org.locationtech.geomesa.arrow.vector.ArrowDictionary.HasArrowDictionary
import org.locationtech.geomesa.arrow.vector.GeometryVector.GeometryWriter
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.EncodingPrecision.EncodingPrecision
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.{EncodingPrecision, SimpleFeatureEncoding}
import org.locationtech.geomesa.features.serialization.ObjectType
import org.locationtech.geomesa.features.serialization.ObjectType.ObjectType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
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
  def setValueCount(count: Int): Unit = {}
}

object ArrowAttributeWriter {

  import scala.collection.JavaConversions._

  /**
    * Writer for feature ID
    *
    * @param vector simple feature vector
    * @param includeFids actually write the feature ids, or omit them, in which case the writer is a no-op
    * @param allocator buffer allocator
    * @return feature ID writer
    */
  def id(vector: NullableMapVector, includeFids: Boolean)(implicit allocator: BufferAllocator): ArrowAttributeWriter = {
    if (includeFids) {
      val name = SimpleFeatureVector.FeatureIdField
      ArrowAttributeWriter(name, Seq(ObjectType.STRING), classOf[String], vector, None, Map.empty, null)
    } else {
      ArrowAttributeWriter.ArrowNoopWriter
    }
  }

  /**
    * Creates a sequence of attribute writers for a simple feature type. Each attribute in the feature type
    * will map to a writer in the returned sequence.
    *
    * @param sft simple feature type
    * @param vector simple feature vector
    * @param dictionaries dictionaries, if any
    * @param encoding encoding options
    * @param allocator buffer allocator
    * @return attribute writers
    */
  def apply(sft: SimpleFeatureType,
            vector: NullableMapVector,
            dictionaries: Map[String, ArrowDictionary],
            encoding: SimpleFeatureEncoding = SimpleFeatureEncoding.min(false))
           (implicit allocator: BufferAllocator): Seq[ArrowAttributeWriter] = {
    sft.getAttributeDescriptors.map { descriptor =>
      val name = descriptor.getLocalName
      val metadata = Map(SimpleFeatureVector.DescriptorKey -> SimpleFeatureTypes.encodeDescriptor(sft, descriptor))
      val classBinding = descriptor.getType.getBinding
      val (objectType, bindings) = ObjectType.selectType(classBinding, descriptor.getUserData)
      val dictionary = dictionaries.get(name)
      apply(name, bindings.+:(objectType), classBinding, vector, dictionary, metadata, encoding)
    }
  }

  /**
    * Creates a writer for a single attribute
    *
    * @param name name of the attribute, generally including the class binding so the sft can be re-created
    * @param bindings object bindings, the attribute type plus any subtypes (e.g. for lists or maps)
    * @param classBinding the explicit class binding of the attribute
    * @param vector the simple feature vector to write to
    * @param dictionary the dictionary for the attribute, if any
    * @param metadata metadata to encode in the field
    * @param encoding encoding options
    * @param allocator buffer allocator
    * @return attribute writer
    */
  def apply(name: String,
            bindings: Seq[ObjectType],
            classBinding: Class[_],
            vector: NullableMapVector,
            dictionary: Option[ArrowDictionary],
            metadata: Map[String, String],
            encoding: SimpleFeatureEncoding)
           (implicit allocator: BufferAllocator): ArrowAttributeWriter = {
    dictionary match {
      case None =>
        bindings.head match {
          case ObjectType.GEOMETRY =>
            new ArrowGeometryWriter(vector, name, classBinding, metadata, encoding.geometry)

          case ObjectType.DATE =>
            if (encoding.date == EncodingPrecision.Min) {
              val child = ensureChildVector(vector, name, MinorType.INT.getType, classOf[NullableIntVector], metadata)
              new ArrowDateSecondsWriter(child.getMutator)
            } else {
              val child = ensureChildVector(vector, name, MinorType.BIGINT.getType, classOf[NullableBigIntVector], metadata)
              new ArrowDateMillisWriter(child.getMutator)
            }

          case ObjectType.STRING =>
            val child = ensureChildVector(vector, name, MinorType.VARCHAR.getType, classOf[NullableVarCharVector], metadata)
            new ArrowStringWriter(child.getMutator)

          case ObjectType.INT =>
            val child = ensureChildVector(vector, name, MinorType.INT.getType, classOf[NullableIntVector], metadata)
            new ArrowIntWriter(child.getMutator)

          case ObjectType.LONG =>
            val child = ensureChildVector(vector, name, MinorType.BIGINT.getType, classOf[NullableBigIntVector], metadata)
            new ArrowLongWriter(child.getMutator)

          case ObjectType.FLOAT =>
            val child = ensureChildVector(vector, name, MinorType.FLOAT4.getType, classOf[NullableFloat4Vector], metadata)
            new ArrowFloatWriter(child.getMutator)

          case ObjectType.DOUBLE =>
            val child = ensureChildVector(vector, name, MinorType.FLOAT8.getType, classOf[NullableFloat8Vector], metadata)
            new ArrowDoubleWriter(child.getMutator)

          case ObjectType.BOOLEAN =>
            val child = ensureChildVector(vector, name, MinorType.BIT.getType, classOf[NullableBitVector], metadata)
            new ArrowBooleanWriter(child.getMutator)

          case ObjectType.LIST =>
            // TODO list types
            val child = ensureChildVector(vector, name, MinorType.LIST.getType, classOf[ListVector], metadata)
            new ArrowListWriter(vector.getWriter.list(name), bindings(1), allocator)

          case ObjectType.MAP =>
            // TODO map types
            val child = ensureChildVector(vector, name, MinorType.MAP.getType, classOf[NullableMapVector], metadata)
            new ArrowMapWriter(vector.getWriter.map(name), bindings(1), bindings(2), allocator)

          case ObjectType.BYTES =>
            val child = ensureChildVector(vector, name, MinorType.VARBINARY.getType, classOf[NullableVarBinaryVector], metadata)
            new ArrowBytesWriter(child.getMutator)

          case ObjectType.JSON =>
            val child = ensureChildVector(vector, name, MinorType.VARCHAR.getType, classOf[NullableVarCharVector], metadata)
            new ArrowStringWriter(child.getMutator)

          case ObjectType.UUID =>
            val child = ensureChildVector(vector, name, MinorType.VARCHAR.getType, classOf[NullableVarCharVector], metadata)
            new ArrowStringWriter(child.getMutator)

          case _ => throw new IllegalArgumentException(s"Unexpected object type ${bindings.head}")
        }

      case Some(dict) =>
        val dictionaryEncoding = dict.encoding
        val dictionaryType = TypeBindings(bindings, classBinding, encoding)
        if (dictionaryEncoding.getIndexType.getBitWidth == 8) {
          val fieldType = new FieldType(true, MinorType.TINYINT.getType, dictionaryEncoding, metadata)
          vector.addOrGet(name, fieldType, classOf[NullableTinyIntVector])
          new ArrowDictionaryByteWriter(vector.getWriter.tinyInt(name), dict, dictionaryType)
        } else if (dictionaryEncoding.getIndexType.getBitWidth == 16) {
          val fieldType = new FieldType(true, MinorType.SMALLINT.getType, dictionaryEncoding, metadata)
          vector.addOrGet(name, fieldType, classOf[NullableSmallIntVector])
          new ArrowDictionaryShortWriter(vector.getWriter.smallInt(name), dict, dictionaryType)
        } else {
          val fieldType = new FieldType(true, MinorType.INT.getType, dictionaryEncoding, metadata)
          vector.addOrGet(name, fieldType, classOf[NullableIntVector])
          new ArrowDictionaryIntWriter(vector.getWriter.integer(name), dict, dictionaryType)
        }
    }
  }

  private def ensureChildVector[T <: FieldVector](vector: NullableMapVector,
                                                  name: String,
                                                  arrowType: ArrowType,
                                                  clazz: Class[T],
                                                  metadata: Map[String, String]): T = {
    var child = vector.getChild(name).asInstanceOf[T]
    if (child == null) {
      child = vector.addOrGet(name, new FieldType(true, arrowType, null, metadata), clazz)
      child.allocateNew()
    }
    child
  }

  /**
    * Converts a value into a dictionary byte and writes it
    */
  class ArrowDictionaryByteWriter(writer: TinyIntWriter,
                                  val dictionary: ArrowDictionary,
                                  val dictionaryType: TypeBindings) extends ArrowAttributeWriter with HasArrowDictionary {
    override def apply(i: Int, value: AnyRef): Unit = if (value != null) {
      writer.setPosition(i)
      writer.writeTinyInt(dictionary.index(value).toByte)
    }
  }

  /**
    * Converts a value into a dictionary short and writes it
    */
  class ArrowDictionaryShortWriter(writer: SmallIntWriter,
                                   val dictionary: ArrowDictionary,
                                   val dictionaryType: TypeBindings) extends ArrowAttributeWriter with HasArrowDictionary {
    override def apply(i: Int, value: AnyRef): Unit = if (value != null) {
      writer.setPosition(i)
      writer.writeSmallInt(dictionary.index(value).toShort)
    }
  }

  /**
    * Converts a value into a dictionary int and writes it
    */
  class ArrowDictionaryIntWriter(writer: IntWriter,
                                 val dictionary: ArrowDictionary,
                                 val dictionaryType: TypeBindings) extends ArrowAttributeWriter with HasArrowDictionary {
    override def apply(i: Int, value: AnyRef): Unit = if (value != null) {
      writer.setPosition(i)
      writer.writeInt(dictionary.index(value))
    }
  }

  /**
    * Writes geometries - delegates to our JTS geometry vectors
    */
  class ArrowGeometryWriter(vector: NullableMapVector,
                            name: String,
                            binding: Class[_],
                            metadata: Map[String, String],
                            precision: EncodingPrecision) extends ArrowAttributeWriter {
    private val delegate: GeometryWriter[Geometry] = {
      val untyped = if (binding == classOf[Point]) {
        precision match {
          case EncodingPrecision.Min => new PointFloatVector(name, vector, metadata).getWriter;
          case EncodingPrecision.Max => new PointVector(name, vector, metadata).getWriter;
        }
      } else if (binding == classOf[LineString]) {
        precision match {
          case EncodingPrecision.Min => new LineStringFloatVector(name, vector, metadata).getWriter;
          case EncodingPrecision.Max => new LineStringVector(name, vector, metadata).getWriter;
        }
      } else if (binding == classOf[Polygon]) {
        precision match {
          case EncodingPrecision.Min => new PolygonFloatVector(name, vector, metadata).getWriter;
          case EncodingPrecision.Max => new PolygonVector(name, vector, metadata).getWriter;
        }
      } else if (binding == classOf[MultiLineString]) {
        precision match {
          case EncodingPrecision.Min => new MultiLineStringFloatVector(name, vector, metadata).getWriter;
          case EncodingPrecision.Max => new MultiLineStringVector(name, vector, metadata).getWriter;
        }
      } else if (binding == classOf[MultiPolygon]) {
        precision match {
          case EncodingPrecision.Min => new MultiPolygonFloatVector(name, vector, metadata).getWriter;
          case EncodingPrecision.Max => new MultiPolygonVector(name, vector, metadata).getWriter;
        }
      } else if (binding == classOf[MultiPoint]) {
        precision match {
          case EncodingPrecision.Min => new MultiPointFloatVector(name, vector, metadata).getWriter;
          case EncodingPrecision.Max => new MultiPointVector(name, vector, metadata).getWriter;
        }
      } else if (classOf[Geometry].isAssignableFrom(binding)) {
        throw new NotImplementedError(s"Geometry type $binding is not supported")
      } else {
        throw new IllegalArgumentException(s"Expected geometry type, got $binding")
      }
      untyped.asInstanceOf[GeometryWriter[Geometry]]
    }

    override def apply(i: Int, value: AnyRef): Unit = if (value != null) {
      delegate.set(i, value.asInstanceOf[Geometry])
    }

    override def setValueCount(count: Int): Unit = delegate.setValueCount(count)
  }

  /**
    * Doesn't actually write anything
    */
  object ArrowNoopWriter extends ArrowAttributeWriter {
    override def apply(i: Int, value: AnyRef): Unit = {}
  }

  class ArrowStringWriter(mutator: NullableVarCharVector#Mutator) extends ArrowAttributeWriter {
    override def apply(i: Int, value: AnyRef): Unit = if (value != null) {
      val bytes = value.toString.getBytes(StandardCharsets.UTF_8)
      mutator.setSafe(i, bytes, 0, bytes.length)
    }
  }

  class ArrowIntWriter(mutator: NullableIntVector#Mutator) extends ArrowAttributeWriter {
    override def apply(i: Int, value: AnyRef): Unit = if (value != null) {
      mutator.set(i, value.asInstanceOf[Int])
    }
  }

  class ArrowLongWriter(mutator: NullableBigIntVector#Mutator) extends ArrowAttributeWriter {
    override def apply(i: Int, value: AnyRef): Unit = if (value != null) {
      mutator.set(i, value.asInstanceOf[Long])
    }
  }

  class ArrowFloatWriter(mutator: NullableFloat4Vector#Mutator) extends ArrowAttributeWriter {
    override def apply(i: Int, value: AnyRef): Unit = if (value != null) {
      mutator.set(i, value.asInstanceOf[Float])
    }
  }

  class ArrowDoubleWriter(mutator: NullableFloat8Vector#Mutator) extends ArrowAttributeWriter {
    override def apply(i: Int, value: AnyRef): Unit = if (value != null) {
      mutator.set(i, value.asInstanceOf[Double])
    }
  }

  class ArrowBooleanWriter(mutator: NullableBitVector#Mutator) extends ArrowAttributeWriter {
    override def apply(i: Int, value: AnyRef): Unit = if (value != null) {
      mutator.set(i, if (value.asInstanceOf[Boolean]) { 1 } else { 0 })
    }
  }

  class ArrowDateMillisWriter(mutator: NullableBigIntVector#Mutator) extends ArrowAttributeWriter {
    override def apply(i: Int, value: AnyRef): Unit = if (value != null) {
      mutator.set(i, value.asInstanceOf[Date].getTime)
    }
  }

  class ArrowDateSecondsWriter(mutator: NullableIntVector#Mutator) extends ArrowAttributeWriter {
    override def apply(i: Int, value: AnyRef): Unit = if (value != null) {
      mutator.set(i, (value.asInstanceOf[Date].getTime / 1000L).toInt)
    }
  }

  class ArrowBytesWriter(mutator: NullableVarBinaryVector#Mutator) extends ArrowAttributeWriter {
    override def apply(i: Int, value: AnyRef): Unit = if (value != null) {
      val bytes = value.asInstanceOf[Array[Byte]]
      mutator.setSafe(i, bytes, 0, bytes.length)
    }
  }

  class ArrowListWriter(writer: ListWriter, binding: ObjectType, allocator: BufferAllocator)
      extends ArrowAttributeWriter {
    val subWriter = toListWriter(writer, binding, allocator)
    override def apply(i: Int, value: AnyRef): Unit = if (value != null) {
      writer.setPosition(i)
      writer.startList()
      value.asInstanceOf[java.util.List[AnyRef]].foreach(subWriter)
      writer.endList()
    }
  }

  class ArrowMapWriter(writer: MapWriter, keyBinding: ObjectType, valueBinding: ObjectType, allocator: BufferAllocator)
      extends ArrowAttributeWriter {
    val keyList   = writer.list("k")
    val valueList = writer.list("v")
    val keyWriter   = toListWriter(keyList, keyBinding, allocator)
    val valueWriter = toListWriter(valueList, valueBinding, allocator)
    override def apply(i: Int, value: AnyRef): Unit = if (value != null) {
      writer.setPosition(i)
      writer.start()
      keyList.startList()
      valueList.startList()
      value.asInstanceOf[java.util.Map[AnyRef, AnyRef]].foreach { case (k, v) =>
        keyWriter(k)
        valueWriter(v)
      }
      keyList.endList()
      valueList.endList()
      writer.end()
    }
  }

  private def toListWriter(writer: ListWriter, binding: ObjectType, allocator: BufferAllocator): (AnyRef) => Unit = {
    if (binding == ObjectType.STRING || binding == ObjectType.JSON || binding == ObjectType.UUID) {
      (value: AnyRef) => if (value != null) {
        val bytes = value.toString.getBytes(StandardCharsets.UTF_8)
        val buffer = allocator.buffer(bytes.length)
        buffer.setBytes(0, bytes)
        writer.varChar().writeVarChar(0, bytes.length, buffer)
        buffer.close()
      }
    } else if (binding == ObjectType.INT) {
      (value: AnyRef) => if (value != null) {
        writer.integer().writeInt(value.asInstanceOf[Int])
      }
    } else if (binding == ObjectType.LONG) {
      (value: AnyRef) => if (value != null) {
        writer.bigInt().writeBigInt(value.asInstanceOf[Long])
      }
    } else if (binding == ObjectType.FLOAT) {
      (value: AnyRef) => if (value != null) {
        writer.float4().writeFloat4(value.asInstanceOf[Float])
      }
    } else if (binding == ObjectType.DOUBLE) {
      (value: AnyRef) => if (value != null) {
        writer.float8().writeFloat8(value.asInstanceOf[Double])
      }
    } else if (binding == ObjectType.BOOLEAN) {
      (value: AnyRef) => if (value != null) {
        writer.bit().writeBit(if (value.asInstanceOf[Boolean]) { 1 } else { 0 })
      }
    } else if (binding == ObjectType.DATE) {
      (value: AnyRef) => if (value != null) {
        writer.dateMilli().writeDateMilli(value.asInstanceOf[Date].getTime)
      }
    } else if (binding == ObjectType.GEOMETRY) {
      (value: AnyRef) => if (value != null) {
        val bytes = WKTUtils.write(value.asInstanceOf[Geometry]).getBytes(StandardCharsets.UTF_8)
        val buffer = allocator.buffer(bytes.length)
        buffer.setBytes(0, bytes)
        writer.varChar().writeVarChar(0, bytes.length, buffer)
        buffer.close()
      }
    } else if (binding == ObjectType.BYTES) {
      (value: AnyRef) => if (value != null) {
        val bytes = value.asInstanceOf[Array[Byte]]
        val buffer = allocator.buffer(bytes.length)
        buffer.setBytes(0, bytes)
        writer.varBinary().writeVarBinary(0, bytes.length, buffer)
        buffer.close()
      }
    } else {
      throw new IllegalArgumentException(s"Unexpected list object type $binding")
    }
  }
}
