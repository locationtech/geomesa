/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.parquet

import java.nio.ByteBuffer
import java.util.{Date, UUID}

import org.locationtech.jts.geom.Point
import org.apache.parquet.io.api.{Binary, RecordConsumer}
import org.locationtech.geomesa.features.serialization.ObjectType
import org.locationtech.geomesa.features.serialization.ObjectType.ObjectType
import org.opengis.feature.`type`.AttributeDescriptor

/**
  * Writes a simple feature attribute to a Parquet file
  */
trait AttributeWriter {

  /**
    * Writes an attribute for the ith feature
    * @param recordConsumer the Parquet record consumer
    * @param value attribute value to write
    */
  def apply(recordConsumer: RecordConsumer, value: AnyRef): Unit
}

object AttributeWriter {

  def apply(descriptor: AttributeDescriptor, index: Int): AttributeWriter = {
    val name = descriptor.getLocalName
    val classBinding = descriptor.getType.getBinding
    val bindings = ObjectType.selectType(classBinding, descriptor.getUserData)
    apply(name, index, bindings)
  }

  def apply(name: String, index: Int, bindings: Seq[ObjectType]): AttributeWriter =
    bindings.head match {
      // TODO linestrings and polygons https://geomesa.atlassian.net/browse/GEOMESA-1936
      case ObjectType.GEOMETRY => new PointAttributeWriter(name, index)
      case ObjectType.DATE     => new DateWriter(name, index)
      case ObjectType.DOUBLE   => new DoubleWriter(name, index)
      case ObjectType.FLOAT    => new FloatWriter(name, index)
      case ObjectType.INT      => new IntegerWriter(name, index)
      case ObjectType.LONG     => new LongWriter(name, index)
      case ObjectType.STRING   => new StringWriter(name, index)
      case ObjectType.LIST     => new ListWriter(name, index, bindings(1))
      case ObjectType.MAP      => new MapWriter(name, index, bindings(1), bindings(2))
      case ObjectType.UUID     => new UUIDWriter(name, index)

    }

  abstract class AbstractAttributeWriter(fieldName: String,
                                         fieldIndex: Int) extends AttributeWriter {

    def write(recordConsumer: RecordConsumer, value: AnyRef): Unit

    override def apply(recordConsumer: RecordConsumer, value: AnyRef): Unit = {
      if (value != null) {
        recordConsumer.startField(fieldName, fieldIndex)
        write(recordConsumer, value)
        recordConsumer.endField(fieldName, fieldIndex)
      }
    }
  }

  class PointAttributeWriter(fieldName: String, fieldIndex: Int) extends AbstractAttributeWriter(fieldName, fieldIndex) {
    override def write(recordConsumer: RecordConsumer, value: AnyRef): Unit = {
      val pt = value.asInstanceOf[Point]
      recordConsumer.startGroup()
      recordConsumer.startField("x", 0)
      recordConsumer.addDouble(pt.getX)
      recordConsumer.endField("x", 0)
      recordConsumer.startField("y", 1)
      recordConsumer.addDouble(pt.getY)
      recordConsumer.endField("y", 1)
      recordConsumer.endGroup()
    }
  }

  class DateWriter(fieldName: String, fieldIndex: Int) extends AbstractAttributeWriter(fieldName, fieldIndex) {
    override def write(recordConsumer: RecordConsumer, value: AnyRef): Unit = {
      recordConsumer.addLong(value.asInstanceOf[Date].getTime)
    }
  }

  class DoubleWriter(fieldName: String, fieldIndex: Int) extends AbstractAttributeWriter(fieldName, fieldIndex) {
    override def write(recordConsumer: RecordConsumer, value: AnyRef): Unit = {
      recordConsumer.addDouble(value.asInstanceOf[java.lang.Double])
    }
  }

  class FloatWriter(fieldName: String, fieldIndex: Int) extends AbstractAttributeWriter(fieldName, fieldIndex) {
    override def write(recordConsumer: RecordConsumer, value: AnyRef): Unit = {
      recordConsumer.addFloat(value.asInstanceOf[java.lang.Float])
    }
  }

  class IntegerWriter(fieldName: String, fieldIndex: Int) extends AbstractAttributeWriter(fieldName, fieldIndex) {
    override def write(recordConsumer: RecordConsumer, value: AnyRef): Unit = {
      recordConsumer.addInteger(value.asInstanceOf[java.lang.Integer])
    }
  }

  class LongWriter(fieldName: String, fieldIndex: Int) extends AbstractAttributeWriter(fieldName, fieldIndex) {
    override def write(recordConsumer: RecordConsumer, value: AnyRef): Unit = {
      recordConsumer.addLong(value.asInstanceOf[Long])
    }
  }

  class StringWriter(fieldName: String, fieldIndex: Int) extends AbstractAttributeWriter(fieldName, fieldIndex) {
    override def write(recordConsumer: RecordConsumer, value: AnyRef): Unit = {
      recordConsumer.addBinary(Binary.fromString(value.asInstanceOf[String]))
    }
  }

  class ListWriter(fieldName: String, fieldIndex: Int, valueType: ObjectType)
    extends AbstractAttributeWriter(fieldName, fieldIndex) {
    val elementWriter = AttributeWriter("element", 0, Seq(valueType))

    override def write(recordConsumer: RecordConsumer, value: AnyRef): Unit = {
      recordConsumer.startGroup()
      val thelist = value.asInstanceOf[List[AnyRef]]

      if (thelist != null && thelist.nonEmpty) {
        recordConsumer.startField(fieldName, 0)

        thelist.foreach { e =>
          recordConsumer.startGroup()
          if (e != null) {
           elementWriter(recordConsumer, e)
          }
          recordConsumer.endGroup()
        }

        recordConsumer.endField(fieldName, 0)
      }
      recordConsumer.endGroup()
    }
  }

  class MapWriter(fieldName: String, fieldIndex: Int, keyType: ObjectType, valueType: ObjectType)
    extends AbstractAttributeWriter(fieldName, fieldIndex) {
    val keyWriter = AttributeWriter("key", 0, Seq(keyType))
    val valueWriter = AttributeWriter("value", 1, Seq(valueType))

    override def write(recordConsumer: RecordConsumer, value: AnyRef): Unit = {
      recordConsumer.startGroup()
      val themap = value.asInstanceOf[Map[AnyRef, AnyRef]]

      if (themap != null && themap.nonEmpty) {
        recordConsumer.startField(fieldName, 0)

        themap.foreach { case (k, v) =>
          recordConsumer.startGroup()
          keyWriter(recordConsumer, k)

          if (v != null) {
            valueWriter(recordConsumer, v)
          }

          recordConsumer.endGroup()
        }

        recordConsumer.endField(fieldName, 0)
      }
      recordConsumer.endGroup()
    }

  }

  class UUIDWriter(fieldName: String, fieldIndex: Int) extends AbstractAttributeWriter(fieldName, fieldIndex) {
    override def write(recordConsumer: RecordConsumer, value: AnyRef): Unit = {
      val uuid = value.asInstanceOf[UUID]
      val bb = ByteBuffer.wrap(new Array[Byte](16))
      bb.putLong(uuid.getMostSignificantBits)
      bb.putLong(uuid.getLeastSignificantBits)
      recordConsumer.addBinary(Binary.fromConstantByteArray(bb.array()))
    }
  }


}
