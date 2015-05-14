/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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