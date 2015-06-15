/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.features.kryo.serialization

import java.io.{InputStream, OutputStream}

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}

import scala.reflect.ClassTag

trait KryoSerializer[T] {

  /**
   * Serialize the object into bytes
   *
   * @param obj
   * @return
   */
  def write(obj: T): Array[Byte]

  /**
   * Serialize the object into a byte stream
   *
   * @param obj
   * @param out
   */
  def write(obj: T, out: OutputStream): Unit

  /**
   * Deserialize the object from bytes - note that the buffer may be mutated during the read, but
   * will be returned to normal.
   *
   * @param value
   * @return
   */
  def read(value: Array[Byte]): T

  /**
   * Deserialize the object from a byte stream
   *
   * @param in
   * @return
   */
  def read(in: InputStream): T
}

/**
 * Abstract class for serializing and deserializing objects. Not thread safe.
 */
abstract class KryoSerializerBase[T]()(implicit classTag: ClassTag[T]) extends KryoSerializer[T] {

  def serializer: Serializer[T]

  protected[kryo] val binding: Class[T] = classTag.runtimeClass.asInstanceOf[Class[T]]

  protected[kryo] val output = new Output(1024, -1)
  protected[kryo] val input = new Input(Array.empty[Byte])
  protected[kryo] lazy val streamBuffer = new Array[Byte](1024)

  protected[kryo] val kryo = new Kryo()
  kryo.setReferences(false)

  override def write(obj: T): Array[Byte] = {
    output.clear()
    kryo.writeObject(output, obj, serializer)
    output.toBytes()
  }

  override def write(obj: T, out: OutputStream): Unit = {
    output.clear()
    output.setOutputStream(out)
    kryo.writeObject(output, obj, serializer)
    output.flush()
    output.setOutputStream(null)
  }

  override def read(value: Array[Byte]): T = {
    input.setBuffer(value)
    kryo.readObject(input, binding, serializer)
  }

  override def read(in: InputStream): T = {
    input.setBuffer(streamBuffer)
    input.setInputStream(in)
    val obj = kryo.readObject(input, binding, serializer)
    input.setInputStream(null)
    obj
  }
}
