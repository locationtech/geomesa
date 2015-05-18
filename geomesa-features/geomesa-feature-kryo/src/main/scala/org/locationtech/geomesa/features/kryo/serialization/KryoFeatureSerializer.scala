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

import com.esotericsoftware.kryo.Serializer
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
 * Class for serializing and deserializing simple features. Not thread safe.
 *
 * @param serializer
 */
case class KryoFeatureSerializer(serializer: Serializer[SimpleFeature])
    extends KryoSerializerBase[SimpleFeature] {

  private val idSerializer = new FeatureIdSerializer()

  /**
   * Read only the id from a serialized feature
   *
   * @param value
   * @return
   */
  def readId(value: Array[Byte]): String = {
    input.setBuffer(value)
    kryo.readObject(input, classOf[KryoFeatureId], idSerializer).id
  }
}

object KryoFeatureSerializer {

  def apply(sft: SimpleFeatureType, options: SerializationOptions = SerializationOptions.none): KryoFeatureSerializer =
    apply(new SimpleFeatureSerializer(sft, options))

  def apply(sft: SimpleFeatureType, decodeAs: SimpleFeatureType, options: SerializationOptions): KryoFeatureSerializer = {
    if (sft.eq(decodeAs)) {
      apply(sft, options)
    } else {
      apply(new TransformingSimpleFeatureSerializer(sft, decodeAs, options))
    }
  }
}

case class KryoFeatureId(id: String)