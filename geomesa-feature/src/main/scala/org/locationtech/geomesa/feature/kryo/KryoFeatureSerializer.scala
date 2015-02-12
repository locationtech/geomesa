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

package org.locationtech.geomesa.feature.kryo

import java.io.{InputStream, OutputStream}

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import org.locationtech.geomesa.feature.{AvroSimpleFeature, ScalaSimpleFeature}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
 * Class for serializing and deserializing simple features. Not thread safe.
 *
 * @param serializer
 */
case class KryoFeatureSerializer(serializer: Serializer[SimpleFeature]) {

  private val kryo = new Kryo()

  // TODO test spark
  kryo.setReferences(false)
  kryo.register(classOf[ScalaSimpleFeature], serializer,  kryo.getNextRegistrationId)
  kryo.register(classOf[KryoFeatureId], new FeatureIdSerializer(),  kryo.getNextRegistrationId)
  kryo.register(classOf[SimpleFeature], serializer,  kryo.getNextRegistrationId)
  kryo.register(classOf[AvroSimpleFeature], serializer,  kryo.getNextRegistrationId)

  val output = new Output(1024, -1)
  val input = new Input(Array.empty[Byte])
  lazy val streamBuffer = new Array[Byte](1024)

  /**
   * Serialize the feature into bytes
   *
   * @param sf
   * @return
   */
  def write(sf: SimpleFeature): Array[Byte] = {
    output.clear()
    kryo.writeObject(output, sf)
    output.toBytes()
  }

  /**
   * Serialize the feature into a byte stream
   *
   * @param sf
   * @param out
   */
  def write(sf: SimpleFeature, out: OutputStream): Unit = {
    output.clear()
    output.setOutputStream(out)
    kryo.writeObject(output, sf)
    output.flush()
    output.setOutputStream(null)
  }

  /**
   * Deserialize the feature from bytes - note that the buffer may be mutated during the read, but
   * will be returned to normal.
   *
   * @param value
   * @return
   */
  def read(value: Array[Byte]): SimpleFeature = {
    input.setBuffer(value)
    kryo.readObject(input, classOf[SimpleFeature])
  }

  /**
   * Deserialize the feature from a byte stream
   *
   * @param in
   * @return
   */
  def read(in: InputStream): SimpleFeature = {
    input.setBuffer(streamBuffer)
    input.setInputStream(in)
    val sf = kryo.readObject(input, classOf[SimpleFeature])
    input.setInputStream(null)
    sf
  }

  /**
   * Read only the id from a serialized feature
   *
   * @param value
   * @return
   */
  def readId(value: Array[Byte]): String = {
    input.setBuffer(value)
    kryo.readObject(input, classOf[KryoFeatureId]).id
  }
}

object KryoFeatureSerializer {

  def apply(sft: SimpleFeatureType): KryoFeatureSerializer = apply(new SimpleFeatureSerializer(sft))

  def apply(sft: SimpleFeatureType, decodeAs: SimpleFeatureType): KryoFeatureSerializer =
    if (sft.eq(decodeAs)) apply(sft) else apply(new TransformingSimpleFeatureSerializer(sft, decodeAs))
}

case class KryoFeatureId(id: String)