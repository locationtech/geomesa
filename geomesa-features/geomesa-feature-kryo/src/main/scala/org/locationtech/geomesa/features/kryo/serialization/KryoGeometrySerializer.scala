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
import com.vividsolutions.jts.geom.Geometry

import scala.ref.SoftReference

object KryoGeometrySerializer extends KryoSerializer[Geometry] {

  private val cache = new ThreadLocal[SoftReference[KryoGeometrySerializer]]()

  private def get() = Option(cache.get()).flatMap(_.get).getOrElse {
    val serializer = new KryoGeometrySerializer(new GeometrySerializer())
    cache.set(new SoftReference(serializer))
    serializer
  }

  override def write(obj: Geometry): Array[Byte] = get().write(obj)

  override def write(obj: Geometry, out: OutputStream): Unit = get().write(obj, out)

  override def read(value: Array[Byte]): Geometry = get().read(value)

  override def read(in: InputStream): Geometry = get().read(in)
}

/**
 * Class for serializing and deserializing geometries
 *
 * @param serializer
 */
case class KryoGeometrySerializer(serializer: Serializer[Geometry]) extends KryoSerializerBase[Geometry]

/**
 * Kryo serializer for geometries.  Not thread safe.
 */
class GeometrySerializer extends Serializer[Geometry] {

  lazy val kryoReader = new KryoReader()
  lazy val kryoWriter = new KryoWriter()

  override def write(kryo: Kryo, output: Output, geom: Geometry): Unit = {
    output.writeInt(SimpleFeatureSerializer.VERSION, true)
    kryoWriter.writeGeometry(output, geom)
  }

  override def read(kryo: Kryo, input: Input, typ: Class[Geometry]): Geometry = {
    input.readInt(true) // not used
    kryoReader.readGeometryDirectly(input)
  }
}