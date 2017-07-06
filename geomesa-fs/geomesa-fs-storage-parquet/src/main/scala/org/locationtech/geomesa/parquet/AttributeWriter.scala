/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.parquet

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
      // TODO linestrings and polygons
      case ObjectType.GEOMETRY => new PointAttributeWriter(name, index)
      case ObjectType.DATE     => new DateWriter(name, index)
      case ObjectType.DOUBLE   => new DoubleWriter(name, index)
      case ObjectType.FLOAT    => new FloatWriter(name, index)
      case ObjectType.INT      => new IntegerWriter(name, index)
      case ObjectType.LONG     => new LongWriter(name, index)
      case ObjectType.STRING   => new StringWriter(name, index)

      // TODO implement
      case ObjectType.LIST     => throw new NotImplementedError()
      case ObjectType.MAP      => throw new NotImplementedError()
      case ObjectType.UUID     => throw new NotImplementedError()

    }
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

  // NOTE: not thread safe
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

}
