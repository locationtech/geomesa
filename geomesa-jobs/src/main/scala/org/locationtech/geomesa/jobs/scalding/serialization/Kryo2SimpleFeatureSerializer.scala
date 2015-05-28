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

package org.locationtech.geomesa.jobs.scalding.serialization

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.twitter.chill.config.Config
import com.twitter.scalding.serialization.KryoHadoop
import org.locationtech.geomesa.features.SimpleFeatureSerializers
import org.locationtech.geomesa.jobs.mapreduce.SimpleFeatureSerialization
import org.locationtech.geomesa.jobs.scalding.serialization.Kryo2SimpleFeatureSerializer._
import org.opengis.feature.simple.SimpleFeature

/**
 * Scalding compatible kryo 2.21 serializer. Delegates to the hadoop serializer.
 */
class Kryo2SimpleFeatureSerializer extends Serializer[SimpleFeature] {

  override def write(kryo: Kryo, output: Output, sf: SimpleFeature) = {
    val serializer = hadoopSerialization.getSerializer(classOf[SimpleFeature])
    val byteStream = new ByteArrayOutputStream()
    serializer.open(byteStream)
    serializer.serialize(sf)
    serializer.close()
    val bytes = byteStream.toByteArray
    output.writeInt(bytes.length, true)
    output.write(bytes)
  }

  override def read(kryo: Kryo, input: Input, clazz: Class[SimpleFeature]) = {
    val deserializer = hadoopSerialization.getDeserializer(classOf[SimpleFeature])
    val bytes = Array.ofDim[Byte](input.readInt(true))
    input.read(bytes)
    val byteStream = new ByteArrayInputStream(bytes)
    deserializer.open(byteStream)
    val sf = deserializer.deserialize(null)
    deserializer.close()
    sf
  }
}

object Kryo2SimpleFeatureSerializer {
  val hadoopSerialization = new SimpleFeatureSerialization()
}

/**
 * Overrides the kryo serializer to register our custom serialization
 */
class SimpleFeatureKryoHadoop(config: Config) extends KryoHadoop(config) {
  override def newKryo(): Kryo = {
    val kryo = super.newKryo()
    val serializer = new Kryo2SimpleFeatureSerializer
    SimpleFeatureSerializers.simpleFeatureImpls.foreach(kryo.register(_, serializer))
    kryo
  }
}