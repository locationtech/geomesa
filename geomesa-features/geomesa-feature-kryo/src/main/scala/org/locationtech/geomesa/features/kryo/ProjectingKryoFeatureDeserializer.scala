/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.kryo

import java.io.InputStream

import com.esotericsoftware.kryo.io.Input
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.SerializationOption.SerializationOption
import org.locationtech.geomesa.features.kryo.impl.{KryoFeatureDeserialization, KryoFeatureSerialization}
import org.locationtech.geomesa.features.kryo.serialization.KryoUserDataSerialization
import org.locationtech.geomesa.utils.cache.CacheKeyGenerator
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
  * @param original the simple feature type that was encoded
  * @param projected the simple feature type to project to when decoding
  * @param options the options what were applied when encoding
  */
class ProjectingKryoFeatureDeserializer(original: SimpleFeatureType,
                                        projected: SimpleFeatureType,
                                        val options: Set[SerializationOption] = Set.empty)
    extends KryoFeatureSerialization {

  override private [kryo] def serializeSft = original

  private val numProjectedAttributes = projected.getAttributeCount
  private val offsets = Array.fill[Int](numProjectedAttributes)(-1)
  private val readersInOrder = Array.ofDim[(Input) => AnyRef](numProjectedAttributes)
  private val indices = Array.ofDim[Int](original.getAttributeCount)
  private val withoutId = options.withoutId

  setup()

  private def setup(): Unit = {
    val originalReaders = KryoFeatureDeserialization.getReaders(CacheKeyGenerator.cacheKey(original), original)
    var i = 0
    while (i < indices.length) {
      val index = projected.indexOf(original.getDescriptor(i).getLocalName)
      indices(i) = index
      if (index != -1) {
        readersInOrder(index) = originalReaders(i)
      }
      i += 1
    }
  }

  override def deserialize(bytes: Array[Byte]): SimpleFeature = deserialize("", bytes, 0, bytes.length)

  override def deserialize(id: String, bytes: Array[Byte]): SimpleFeature = deserialize(id, bytes, 0, bytes.length)

  override def deserialize(in: InputStream): SimpleFeature = throw new NotImplementedError

  override def deserialize(id: String, in: InputStream): SimpleFeature = throw new NotImplementedError

  override def deserialize(bytes: Array[Byte], offset: Int, length: Int): SimpleFeature =
    deserialize("", bytes, offset, length)

  override def deserialize(id: String, bytes: Array[Byte], offset: Int, length: Int): SimpleFeature = {
    val input = KryoFeatureDeserialization.getInput(bytes, offset, length)
    if (input.readInt(true) != KryoFeatureSerializer.VERSION) {
      throw new IllegalArgumentException("Can't process features serialized with an older version")
    }
    val attributes = Array.ofDim[AnyRef](numProjectedAttributes)
    // read in the offsets
    val offsetStart = offset + input.readInt()
    val finalId = if (withoutId) { id }  else { input.readString() }
    input.setPosition(offsetStart)
    var i = 0
    while (i < indices.length) {
      val iOffset = if (input.position < input.limit) { offset + input.readInt(true) } else { -1 }
      val index = indices(i)
      if (index != -1) {
        offsets(index) = iOffset
      }
      i += 1
    }
    // read in the values
    i = 0
    while (i < numProjectedAttributes) {
      val offset = offsets(i)
      if (offset != -1) {
        input.setPosition(offset)
        attributes(i) = readersInOrder(i)(input)
      }
      i += 1
    }
    val sf = new ScalaSimpleFeature(projected, finalId, attributes)
    if (options.withUserData) {
      // skip offset data
      input.setPosition(offsetStart)
      var i = 0
      while (i < original.getAttributeCount) {
        input.readInt(true)
        i += 1
      }
      val ud = KryoUserDataSerialization.deserialize(input)
      sf.getUserData.putAll(ud)
      sf
    }
    sf
  }
}
