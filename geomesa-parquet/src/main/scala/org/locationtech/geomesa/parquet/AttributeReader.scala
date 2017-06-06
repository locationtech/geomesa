/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.parquet


/**
  * Reads a simple feature attribute from a Parquet file
  */
trait AttributeReader {
  /**
    * Read an attribute from the ith feature in the simple feature vector
    *
    * @param i index of the feature to read
    * @return the attribute value
    */
  def apply(i: Int): AnyRef

}

object AttributeReader {

/*
  def apply(descriptor: AttributeDescriptor, index: Int): AttributeReader = {
    val name = descriptor.getLocalName
    val classBinding = descriptor.getType.getBinding
    val (objectType, _) = ObjectType.selectType(classBinding, descriptor.getUserData)
    objectType match {
      case ObjectType.GEOMETRY => new PointAttributeReader(name, index)
      case ObjectType.DATE     => new DateReader(name, index)
      case ObjectType.DOUBLE   => new DoubleReader(name, index)
      case ObjectType.FLOAT    => new FloatReader(name, index)
      case ObjectType.INT      => new IntegerReader(name, index)
      case ObjectType.LONG     => new LongReader(name, index)
      case ObjectType.STRING   => new StringReader(name, index)
    }
  }
*/

/*
  abstract class AbstractAttributeReader(fieldName: String,
                                         fieldIndex: Int) extends AttributeReader {

    def write(recordConsumer: RecordConsumer, value: AnyRef): Unit

    override def apply(recordConsumer: RecordConsumer, value: AnyRef): Unit = {
      recordConsumer.startField(fieldName, fieldIndex)
      write(recordConsumer, value)
      recordConsumer.endField(fieldName, fieldIndex)
    }
  }

  class PointAttributeReader(fieldName: String, fieldIndex: Int) extends AbstractAttributeReader(fieldName, fieldIndex) {
    private val bytes = ByteBuffer.allocate(16)

    override def write(recordConsumer: RecordConsumer, value: AnyRef): Unit = {
      val pt = value.asInstanceOf[Point]
      bytes.position(0)
      bytes.putDouble(pt.getX)
      bytes.putDouble(pt.getY)
      recordConsumer.addBinary(Binary.fromReusedByteBuffer(bytes))
    }
  }


  class DateReader(fieldName: String, fieldIndex: Int) extends AbstractAttributeReader(fieldName, fieldIndex) {
    override def write(recordConsumer: RecordConsumer, value: AnyRef): Unit = {
      recordConsumer.addLong(value.asInstanceOf[Date].getTime)
    }
  }

  class DoubleReader(fieldName: String, fieldIndex: Int) extends AbstractAttributeReader(fieldName, fieldIndex) {
    override def write(recordConsumer: RecordConsumer, value: AnyRef): Unit = {
      recordConsumer.addDouble(value.asInstanceOf[java.lang.Double])
    }
  }

  class FloatReader(fieldName: String, fieldIndex: Int) extends AbstractAttributeReader(fieldName, fieldIndex) {
    override def write(recordConsumer: RecordConsumer, value: AnyRef): Unit = {
      recordConsumer.addFloat(value.asInstanceOf[java.lang.Float])
    }
  }

  class IntegerReader(fieldName: String, fieldIndex: Int) extends AbstractAttributeReader(fieldName, fieldIndex) {
    override def write(recordConsumer: RecordConsumer, value: AnyRef): Unit = {
      recordConsumer.addInteger(value.asInstanceOf[java.lang.Integer])
    }
  }

  class LongReader(fieldName: String, fieldIndex: Int) extends AbstractAttributeReader(fieldName, fieldIndex) {
    override def write(recordConsumer: RecordConsumer, value: AnyRef): Unit = {
      recordConsumer.addLong(value.asInstanceOf[Long])
    }
  }

  class StringReader(fieldName: String, fieldIndex: Int) extends AbstractAttributeReader(fieldName, fieldIndex) {
    override def write(recordConsumer: RecordConsumer, value: AnyRef): Unit = {
      recordConsumer.addBinary(Binary.fromString(value.asInstanceOf[String]))
    }
  }
*/

}

