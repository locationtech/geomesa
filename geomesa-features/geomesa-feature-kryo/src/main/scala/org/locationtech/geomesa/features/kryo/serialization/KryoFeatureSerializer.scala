/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.features.kryo.serialization

import com.esotericsoftware.kryo.Serializer
import org.locationtech.geomesa.features.SerializationOption.SerializationOption
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

  def apply(sft: SimpleFeatureType, options: Set[SerializationOption] = Set.empty): KryoFeatureSerializer =
    apply(new SimpleFeatureSerializer(sft, options))

  def apply(sft: SimpleFeatureType,
            decodeAs: SimpleFeatureType,
            options: Set[SerializationOption]): KryoFeatureSerializer = {
    if (sft.eq(decodeAs)) {
      apply(sft, options)
    } else {
      apply(new TransformingSimpleFeatureSerializer(sft, decodeAs, options))
    }
  }
}

case class KryoFeatureId(id: String)