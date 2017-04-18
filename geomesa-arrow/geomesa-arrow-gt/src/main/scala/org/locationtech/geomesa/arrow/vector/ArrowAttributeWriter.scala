/***********************************************************************
* Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.arrow.vector

import java.nio.charset.StandardCharsets
import java.util.Date

import com.vividsolutions.jts.geom._
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.complex.NullableMapVector
import org.apache.arrow.vector.complex.writer.BaseWriter.{ListWriter, MapWriter}
import org.apache.arrow.vector.complex.writer._
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.pojo.FieldType
import org.apache.arrow.vector.{NullableIntVector, NullableSmallIntVector, NullableTinyIntVector}
import org.locationtech.geomesa.arrow.TypeBindings
import org.locationtech.geomesa.arrow.vector.ArrowDictionary.HasArrowDictionary
import org.locationtech.geomesa.arrow.vector.GeometryVector.GeometryWriter
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.GeometryPrecision
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.GeometryPrecision.GeometryPrecision
import org.locationtech.geomesa.arrow.vector.floats._
import org.locationtech.geomesa.features.serialization.ObjectType
import org.locationtech.geomesa.features.serialization.ObjectType.ObjectType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.SimpleFeatureType

trait ArrowAttributeWriter {
  def apply(i: Int, value: AnyRef): Unit
  def setValueCount(count: Int): Unit = {}
}

object ArrowAttributeWriter {

  import scala.collection.JavaConversions._

  def id(vector: NullableMapVector, includeFids: Boolean)(implicit allocator: BufferAllocator): ArrowAttributeWriter = {
    if (includeFids) {
      ArrowAttributeWriter("id", Seq(ObjectType.STRING), classOf[String], vector, None, null)
    } else {
      ArrowAttributeWriter.ArrowNoopWriter
    }
  }

  def apply(sft: SimpleFeatureType,
            vector: NullableMapVector,
            dictionaries: Map[String, ArrowDictionary],
            precision: GeometryPrecision = GeometryPrecision.Double)
           (implicit allocator: BufferAllocator): Seq[ArrowAttributeWriter] = {
    sft.getAttributeDescriptors.map { descriptor =>
      val name = SimpleFeatureTypes.encodeDescriptor(sft, descriptor)
      val classBinding = descriptor.getType.getBinding
      val (objectType, bindings) = ObjectType.selectType(classBinding, descriptor.getUserData)
      val dictionary = dictionaries.get(name).orElse(dictionaries.get(descriptor.getLocalName))
      apply(name, bindings.+:(objectType), classBinding, vector, dictionary, precision)
    }
  }

  def apply(name: String,
            bindings: Seq[ObjectType],
            classBinding: Class[_],
            vector: NullableMapVector,
            dictionary: Option[ArrowDictionary],
            precision: GeometryPrecision)
           (implicit allocator: BufferAllocator): ArrowAttributeWriter = {
    dictionary match {
      case None =>
        bindings.head match {
          case ObjectType.STRING   => new ArrowStringWriter(vector.getWriter.varChar(name), allocator)
          case ObjectType.GEOMETRY => new ArrowGeometryWriter(vector, name, classBinding, precision)
          case ObjectType.INT      => new ArrowIntWriter(vector.getWriter.integer(name))
          case ObjectType.LONG     => new ArrowLongWriter(vector.getWriter.bigInt(name))
          case ObjectType.FLOAT    => new ArrowFloatWriter(vector.getWriter.float4(name))
          case ObjectType.DOUBLE   => new ArrowDoubleWriter(vector.getWriter.float8(name))
          case ObjectType.BOOLEAN  => new ArrowBooleanWriter(vector.getWriter.bit(name))
          case ObjectType.DATE     => new ArrowDateWriter(vector.getWriter.dateMilli(name))
          case ObjectType.LIST     => new ArrowListWriter(vector.getWriter.list(name), bindings(1), allocator)
          case ObjectType.MAP      => new ArrowMapWriter(vector.getWriter.map(name), bindings(1), bindings(2), allocator)
          case ObjectType.BYTES    => new ArrowBytesWriter(vector.getWriter.varBinary(name), allocator)
          case ObjectType.JSON     => new ArrowStringWriter(vector.getWriter.varChar(name), allocator)
          case ObjectType.UUID     => new ArrowStringWriter(vector.getWriter.varChar(name), allocator)
          case _ => throw new IllegalArgumentException(s"Unexpected object type ${bindings.head}")
        }

      case Some(dict) =>
        val encoding = dict.encoding
        val dictionaryType = TypeBindings(bindings, classBinding, precision)
        if (encoding.getIndexType.getBitWidth == 8) {
          vector.addOrGet(name, new FieldType(true, MinorType.TINYINT.getType, encoding), classOf[NullableTinyIntVector])
          new ArrowDictionaryByteWriter(vector.getWriter.tinyInt(name), dict, dictionaryType)
        } else if (encoding.getIndexType.getBitWidth == 16) {
          vector.addOrGet(name, new FieldType(true, MinorType.SMALLINT.getType, encoding), classOf[NullableSmallIntVector])
          new ArrowDictionaryShortWriter(vector.getWriter.smallInt(name), dict, dictionaryType)
        } else {
          vector.addOrGet(name, new FieldType(true, MinorType.INT.getType, encoding), classOf[NullableIntVector])
          new ArrowDictionaryIntWriter(vector.getWriter.integer(name), dict, dictionaryType)
        }
    }
  }

  class ArrowDictionaryByteWriter(writer: TinyIntWriter,
                                  val dictionary: ArrowDictionary,
                                  val dictionaryType: TypeBindings) extends ArrowAttributeWriter with HasArrowDictionary {
    override def apply(i: Int, value: AnyRef): Unit = if (value != null) {
      writer.setPosition(i)
      writer.writeTinyInt(dictionary.index(value).toByte)
    }
  }

  class ArrowDictionaryShortWriter(writer: SmallIntWriter,
                                   val dictionary: ArrowDictionary,
                                   val dictionaryType: TypeBindings) extends ArrowAttributeWriter with HasArrowDictionary {
    override def apply(i: Int, value: AnyRef): Unit = if (value != null) {
      writer.setPosition(i)
      writer.writeSmallInt(dictionary.index(value).toShort)
    }
  }

  class ArrowDictionaryIntWriter(writer: IntWriter,
                                 val dictionary: ArrowDictionary,
                                 val dictionaryType: TypeBindings) extends ArrowAttributeWriter with HasArrowDictionary {
    override def apply(i: Int, value: AnyRef): Unit = if (value != null) {
      writer.setPosition(i)
      writer.writeInt(dictionary.index(value))
    }
  }

  class ArrowGeometryWriter(vector: NullableMapVector, name: String, binding: Class[_], precision: GeometryPrecision)
      extends ArrowAttributeWriter {
    private val delegate: GeometryWriter[Geometry] = {
      val untyped = if (binding == classOf[Point]) {
        precision match {
          case GeometryPrecision.Float  => new PointFloatVector(name, vector).getWriter;
          case GeometryPrecision.Double => new PointVector(name, vector).getWriter;
        }
      } else if (binding == classOf[LineString]) {
        precision match {
          case GeometryPrecision.Float  => new LineStringFloatVector(name, vector).getWriter;
          case GeometryPrecision.Double => new LineStringVector(name, vector).getWriter;
        }
      } else if (binding == classOf[Polygon]) {
        precision match {
          case GeometryPrecision.Float  => new PolygonFloatVector(name, vector).getWriter;
          case GeometryPrecision.Double => new PolygonVector(name, vector).getWriter;
        }
      } else if (binding == classOf[MultiLineString]) {
        precision match {
          case GeometryPrecision.Float  => new MultiLineStringFloatVector(name, vector).getWriter;
          case GeometryPrecision.Double => new MultiLineStringVector(name, vector).getWriter;
        }
      } else if (binding == classOf[MultiPolygon]) {
        precision match {
          case GeometryPrecision.Float  => new MultiPolygonFloatVector(name, vector).getWriter;
          case GeometryPrecision.Double => new MultiPolygonVector(name, vector).getWriter;
        }
      } else if (binding == classOf[MultiPoint]) {
        precision match {
          case GeometryPrecision.Float  => new MultiPointFloatVector(name, vector).getWriter;
          case GeometryPrecision.Double => new MultiPointVector(name, vector).getWriter;
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

  object ArrowNoopWriter extends ArrowAttributeWriter {
    override def apply(i: Int, value: AnyRef): Unit = {}
  }

  class ArrowStringWriter(writer: VarCharWriter, allocator: BufferAllocator) extends ArrowAttributeWriter {
    override def apply(i: Int, value: AnyRef): Unit = if (value != null) {
      writer.setPosition(i)
      val bytes = value.toString.getBytes(StandardCharsets.UTF_8)
      val buffer = allocator.buffer(bytes.length)
      buffer.setBytes(0, bytes)
      writer.writeVarChar(0, bytes.length, buffer)
      buffer.close()
    }
  }

  class ArrowIntWriter(writer: IntWriter) extends ArrowAttributeWriter {
    override def apply(i: Int, value: AnyRef): Unit = if (value != null) {
      writer.setPosition(i)
      writer.writeInt(value.asInstanceOf[Int])
    }
  }

  class ArrowLongWriter(writer: BigIntWriter) extends ArrowAttributeWriter {
    override def apply(i: Int, value: AnyRef): Unit = if (value != null) {
      writer.setPosition(i)
      writer.writeBigInt(value.asInstanceOf[Long])
    }
  }

  class ArrowFloatWriter(writer: Float4Writer) extends ArrowAttributeWriter {
    override def apply(i: Int, value: AnyRef): Unit = if (value != null) {
      writer.setPosition(i)
      writer.writeFloat4(value.asInstanceOf[Float])
    }
  }

  class ArrowDoubleWriter(writer: Float8Writer) extends ArrowAttributeWriter {
    override def apply(i: Int, value: AnyRef): Unit = if (value != null) {
      writer.setPosition(i)
      writer.writeFloat8(value.asInstanceOf[Double])
    }
  }

  class ArrowBooleanWriter(writer: BitWriter) extends ArrowAttributeWriter {
    override def apply(i: Int, value: AnyRef): Unit = if (value != null) {
      writer.setPosition(i)
      writer.writeBit(if (value.asInstanceOf[Boolean]) { 1 } else { 0 })
    }
  }

  class ArrowDateWriter(writer: DateMilliWriter) extends ArrowAttributeWriter {
    override def apply(i: Int, value: AnyRef): Unit = if (value != null) {
      writer.setPosition(i)
      writer.writeDateMilli(value.asInstanceOf[Date].getTime)
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

  class ArrowBytesWriter(writer: VarBinaryWriter, allocator: BufferAllocator) extends ArrowAttributeWriter {
    override def apply(i: Int, value: AnyRef): Unit = if (value != null) {
      writer.setPosition(i)
      val bytes = value.asInstanceOf[Array[Byte]]
      val buffer = allocator.buffer(bytes.length)
      buffer.setBytes(0, bytes)
      writer.writeVarBinary(0, bytes.length, buffer)
      buffer.close()
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
