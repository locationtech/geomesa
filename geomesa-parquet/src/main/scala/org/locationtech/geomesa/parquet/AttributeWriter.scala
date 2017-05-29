/***********************************************************************
* Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/


package org.locationtech.geomesa.parquet

import java.nio.ByteBuffer
import java.util.Date

import com.vividsolutions.jts.geom.Point
import org.apache.parquet.io.api.{Binary, RecordConsumer}
import org.locationtech.geomesa.features.serialization.ObjectType
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
    val (objectType, _) = ObjectType.selectType(classBinding, descriptor.getUserData)
    objectType match {
      case ObjectType.GEOMETRY => new PointAttributeWriter(name, index)
      case ObjectType.DATE     => new DateWriter(name, index)
      case ObjectType.DOUBLE   => new DoubleWriter(name, index)
      case ObjectType.FLOAT    => new FloatWriter(name, index)
      case ObjectType.INT      => new IntegerWriter(name, index)
      case ObjectType.LONG     => new LongWriter(name, index)
      case ObjectType.STRING   => new StringWriter(name, index)
    }
  }

  abstract class AbstractAttributeWriter(fieldName: String,
                                         fieldIndex: Int) extends AttributeWriter {

    def write(recordConsumer: RecordConsumer, value: AnyRef): Unit

    override def apply(recordConsumer: RecordConsumer, value: AnyRef): Unit = {
      recordConsumer.startField(fieldName, fieldIndex)
      write(recordConsumer, value)
      recordConsumer.endField(fieldName, fieldIndex)
    }
  }

  // NOTE: not thread safe
  class PointAttributeWriter(fieldName: String, fieldIndex: Int) extends AbstractAttributeWriter(fieldName, fieldIndex) {
    private val bytes = ByteBuffer.allocate(16)

    override def write(recordConsumer: RecordConsumer, value: AnyRef): Unit = {
      val pt = value.asInstanceOf[Point]
      bytes.position(0)
      bytes.putDouble(pt.getX)
      bytes.putDouble(pt.getY)
      bytes.position(0)
      recordConsumer.addBinary(Binary.fromReusedByteBuffer(bytes))
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

}
