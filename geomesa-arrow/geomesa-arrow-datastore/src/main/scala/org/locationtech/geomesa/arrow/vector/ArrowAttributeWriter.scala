/*******************************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ******************************************************************************/

package org.locationtech.geomesa.arrow.vector

import java.io.Closeable
import java.nio.charset.StandardCharsets
import java.util.Date

import com.vividsolutions.jts.geom.{Geometry, Point}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.complex.impl.NullableMapWriter
import org.apache.arrow.vector.complex.writer.BaseWriter.{ListWriter, MapWriter}
import org.apache.arrow.vector.complex.writer._
import org.locationtech.geomesa.arrow.vector.writer.{GeometryWriter, PointWriter}
import org.locationtech.geomesa.features.serialization.ObjectType
import org.locationtech.geomesa.features.serialization.ObjectType.ObjectType
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.`type`.AttributeDescriptor

trait ArrowAttributeWriter extends Closeable {
  def apply(value: AnyRef): Unit
  override def close(): Unit = {}
}

object ArrowAttributeWriter {

  import scala.collection.JavaConversions._

  def apply(descriptor: AttributeDescriptor, writer: NullableMapWriter, allocator: BufferAllocator): ArrowAttributeWriter = {
    val name = descriptor.getLocalName
    val classBinding = descriptor.getType.getBinding
    val (objectType, bindings) = ObjectType.selectType(classBinding, descriptor.getUserData)
    apply(name, bindings.+:(objectType), classBinding, writer, allocator)
  }

  def apply(name: String,
            bindings: Seq[ObjectType],
            classBinding: Class[_],
            writer: NullableMapWriter,
            allocator: BufferAllocator): ArrowAttributeWriter = {
    bindings.head match {
      case ObjectType.STRING   => new ArrowStringWriter(writer.varChar(name), allocator)
      case ObjectType.GEOMETRY => new ArrowGeometryWriter(writer.map(name), classBinding)
      case ObjectType.INT      => new ArrowIntWriter(writer.integer(name))
      case ObjectType.LONG     => new ArrowLongWriter(writer.bigInt(name))
      case ObjectType.FLOAT    => new ArrowFloatWriter(writer.float4(name))
      case ObjectType.DOUBLE   => new ArrowDoubleWriter(writer.float8(name))
      case ObjectType.BOOLEAN  => new ArrowBooleanWriter(writer.bit(name))
      case ObjectType.DATE     => new ArrowDateWriter(writer.date(name))
      case ObjectType.LIST     => new ArrowListWriter(writer.list(name), bindings(1), allocator)
      case ObjectType.MAP      => new ArrowMapWriter(writer.map(name), bindings(1), bindings(2), allocator)
      case ObjectType.BYTES    => new ArrowByteWriter(writer.varBinary(name), allocator)
      case ObjectType.JSON     => new ArrowStringWriter(writer.varChar(name), allocator)
      case ObjectType.UUID     => new ArrowStringWriter(writer.varChar(name), allocator)
      case _ => throw new IllegalArgumentException(s"Unexpected object type ${bindings.head}")
    }
  }

  class ArrowGeometryWriter(writer: MapWriter, binding: Class[_]) extends ArrowAttributeWriter {
    private val delegate: GeometryWriter[Geometry] = if (binding == classOf[Point]) {
      new PointWriter(writer).asInstanceOf[GeometryWriter[Geometry]]
    } else if (classOf[Geometry].isAssignableFrom(binding)) {
      throw new NotImplementedError("Currently only supports points")
    } else {
      throw new IllegalArgumentException(s"Expected geometry type, got $binding")
    }

    override def apply(value: AnyRef): Unit = delegate.set(value.asInstanceOf[Geometry])
    override def close(): Unit = delegate.close()
  }

  class ArrowStringWriter(writer: VarCharWriter, allocator: BufferAllocator) extends ArrowAttributeWriter {
    override def apply(value: AnyRef): Unit = {
      val bytes = value.toString.getBytes(StandardCharsets.UTF_8)
      val buffer = allocator.buffer(bytes.length)
      buffer.setBytes(0, bytes)
      writer.writeVarChar(0, bytes.length, buffer)
      buffer.close()
    }
  }

  class ArrowIntWriter(writer: IntWriter) extends ArrowAttributeWriter {
    override def apply(value: AnyRef): Unit = {
      writer.writeInt(value.asInstanceOf[Int])
    }
  }

  class ArrowLongWriter(writer: BigIntWriter) extends ArrowAttributeWriter {
    override def apply(value: AnyRef): Unit = {
      writer.writeBigInt(value.asInstanceOf[Long])
    }
  }

  class ArrowFloatWriter(writer: Float4Writer) extends ArrowAttributeWriter {
    override def apply(value: AnyRef): Unit = {
      writer.writeFloat4(value.asInstanceOf[Float])
    }
  }

  class ArrowDoubleWriter(writer: Float8Writer) extends ArrowAttributeWriter {
    override def apply(value: AnyRef): Unit = {
      writer.writeFloat8(value.asInstanceOf[Double])
    }
  }

  class ArrowBooleanWriter(writer: BitWriter) extends ArrowAttributeWriter {
    override def apply(value: AnyRef): Unit = {
      writer.writeBit(if (value.asInstanceOf[Boolean]) { 1 } else { 0 })
    }
  }

  class ArrowDateWriter(writer: DateWriter) extends ArrowAttributeWriter {
    override def apply(value: AnyRef): Unit = {
      writer.writeDate(value.asInstanceOf[Date].getTime)
    }
  }

  class ArrowListWriter(writer: ListWriter, binding: ObjectType, allocator: BufferAllocator)
      extends ArrowAttributeWriter {
    val subWriter = toListWriter(writer, binding, allocator)
    override def apply(value: AnyRef): Unit = {
      writer.startList()
      value.asInstanceOf[java.util.List[AnyRef]].foreach(subWriter)
      writer.endList()
    }
  }

  class ArrowMapWriter(writer: MapWriter, keyBinding: ObjectType, valueBinding: ObjectType, allocator: BufferAllocator)
      extends ArrowAttributeWriter {
    val keyWriter   = toMapWriter(writer, "k", keyBinding, allocator)
    val valueWriter = toMapWriter(writer, "v", valueBinding, allocator)
    override def apply(value: AnyRef): Unit = {
      writer.start()
      value.asInstanceOf[java.util.Map[AnyRef, AnyRef]].foreach { case (k, v) =>
        keyWriter(k)
        valueWriter(v)
      }
      writer.end()
    }
  }

  class ArrowByteWriter(writer: VarBinaryWriter, allocator: BufferAllocator) extends ArrowAttributeWriter {
    override def apply(value: AnyRef): Unit = {
      val bytes = value.asInstanceOf[Array[Byte]]
      val buffer = allocator.buffer(bytes.length)
      buffer.setBytes(0, bytes)
      writer.writeVarBinary(0, bytes.length, buffer)
      buffer.close()
    }
  }

  // TODO close allocated buffers
  private def toListWriter(writer: ListWriter, binding: ObjectType, allocator: BufferAllocator): (AnyRef) => Unit = {
    if (binding == ObjectType.STRING || binding == ObjectType.JSON || binding == ObjectType.UUID) {
      (value: AnyRef) => {
        val bytes = value.toString.getBytes(StandardCharsets.UTF_8)
        val buffer = allocator.buffer(bytes.length)
        buffer.setBytes(0, bytes)
        writer.varChar().writeVarChar(0, bytes.length, buffer)
      }
    } else if (binding == ObjectType.INT) {
      (value: AnyRef) => {
        writer.integer().writeInt(value.asInstanceOf[Int])
      }
    } else if (binding == ObjectType.LONG) {
      (value: AnyRef) => {
        writer.bigInt().writeBigInt(value.asInstanceOf[Long])
      }
    } else if (binding == ObjectType.FLOAT) {
      (value: AnyRef) => {
        writer.float4().writeFloat4(value.asInstanceOf[Float])
      }
    } else if (binding == ObjectType.DOUBLE) {
      (value: AnyRef) => {
        writer.float8().writeFloat8(value.asInstanceOf[Double])
      }
    } else if (binding == ObjectType.BOOLEAN) {
      (value: AnyRef) => {
        writer.bit().writeBit(if (value.asInstanceOf[Boolean]) { 1 } else { 0 })
      }
    } else if (binding == ObjectType.DATE) {
      (value: AnyRef) => {
        writer.date().writeDate(value.asInstanceOf[Date].getTime)
      }
    } else if (binding == ObjectType.GEOMETRY) {
      (value: AnyRef) => {
        val bytes = WKTUtils.write(value.asInstanceOf[Geometry]).getBytes(StandardCharsets.UTF_8)
        val buffer = allocator.buffer(bytes.length)
        buffer.setBytes(0, bytes)
        writer.varChar().writeVarChar(0, bytes.length, buffer)
      }
    } else if (binding == ObjectType.BYTES) {
      (value: AnyRef) => {
        val bytes = value.asInstanceOf[Array[Byte]]
        val buffer = allocator.buffer(bytes.length)
        buffer.setBytes(0, bytes)
        writer.varBinary().writeVarBinary(0, bytes.length, buffer)
      }
    } else {
      throw new IllegalArgumentException(s"Unexpected list object type $binding")
    }
  }

  private def toMapWriter(writer: MapWriter, key: String, binding: ObjectType, allocator: BufferAllocator): (AnyRef) => Unit = {
    if (binding == ObjectType.STRING || binding == ObjectType.JSON || binding == ObjectType.UUID) {
      val subWriter = writer.varChar(key)
      (value: AnyRef) => {
        val bytes = value.toString.getBytes(StandardCharsets.UTF_8)
        val buffer = allocator.buffer(bytes.length)
        buffer.setBytes(0, bytes)
        subWriter.writeVarChar(0, bytes.length, buffer)
      }
    } else if (binding == ObjectType.INT) {
      val subWriter = writer.integer(key)
      (value: AnyRef) => {
        subWriter.writeInt(value.asInstanceOf[Int])
      }
    } else if (binding == ObjectType.LONG) {
      val subWriter = writer.bigInt(key)
      (value: AnyRef) => {
        subWriter.writeBigInt(value.asInstanceOf[Long])
      }
    } else if (binding == ObjectType.FLOAT) {
      val subWriter = writer.float4(key)
      (value: AnyRef) => {
        subWriter.writeFloat4(value.asInstanceOf[Float])
      }
    } else if (binding == ObjectType.DOUBLE) {
      val subWriter = writer.float8(key)
      (value: AnyRef) => {
        subWriter.writeFloat8(value.asInstanceOf[Double])
      }
    } else if (binding == ObjectType.BOOLEAN) {
      val subWriter = writer.bit(key)
      (value: AnyRef) => {
        subWriter.writeBit(if (value.asInstanceOf[Boolean]) { 1 } else { 0 })
      }
    } else if (binding == ObjectType.DATE) {
      val subWriter = writer.date(key)
      (value: AnyRef) => {
        subWriter.writeDate(value.asInstanceOf[Date].getTime)
      }
    } else if (binding == ObjectType.GEOMETRY) {
      val subWriter = writer.varChar(key)
      (value: AnyRef) => {
        val bytes = WKTUtils.write(value.asInstanceOf[Geometry]).getBytes(StandardCharsets.UTF_8)
        val buffer = allocator.buffer(bytes.length)
        buffer.setBytes(0, bytes)
        subWriter.writeVarChar(0, bytes.length, buffer)
      }
    } else if (binding == ObjectType.BYTES) {
      val subWriter = writer.varBinary(key)
      (value: AnyRef) => {
        val bytes = value.asInstanceOf[Array[Byte]]
        val buffer = allocator.buffer(bytes.length)
        buffer.setBytes(0, bytes)
        subWriter.writeVarBinary(0, bytes.length, buffer)
      }
    } else {
      throw new IllegalArgumentException(s"Unexpected list object type $binding")
    }
  }
}
