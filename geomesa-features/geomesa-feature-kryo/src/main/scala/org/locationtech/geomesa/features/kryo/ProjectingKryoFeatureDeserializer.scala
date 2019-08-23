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
import org.locationtech.geomesa.features.kryo.impl.{KryoFeatureDeserialization, KryoFeatureDeserializationV2, KryoFeatureSerialization}
import org.locationtech.geomesa.features.kryo.serialization.KryoUserDataSerialization
import org.locationtech.geomesa.utils.cache.CacheKeyGenerator
import org.locationtech.geomesa.utils.collection.IntBitSet
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
  * @param original the simple feature type that was encoded
  * @param projected the simple feature type to project to when decoding
  * @param options the options what were applied when encoding
  */
class ProjectingKryoFeatureDeserializer(
    original: SimpleFeatureType,
    projected: SimpleFeatureType,
    val options: Set[SerializationOption] = Set.empty
  ) extends KryoFeatureSerialization {

  override protected [kryo] def in: SimpleFeatureType = original

  private val key = CacheKeyGenerator.cacheKey(original)

  private val withoutId = options.withoutId
  private val withoutUserData = !options.withUserData
  private val numProjectedAttributes = projected.getAttributeCount
  private val readers = KryoFeatureDeserialization.getReaders(key, original)
  private val indices =
    Array.tabulate(numProjectedAttributes)(i => original.indexOf(projected.getDescriptor(i).getLocalName))

  private lazy val offsetsV2 = Array.fill[Int](numProjectedAttributes)(-1)
  private lazy val readersInOrderV2 = {
    val originalReaders = KryoFeatureDeserializationV2.getReaders(key, original)
    val mapped = Array.ofDim[Input => AnyRef](numProjectedAttributes)
    var i = 0
    while (i < indices.length) {
      val index = indices(i)
      if (index != -1) {
        mapped(index) = originalReaders(i)
      }
      i += 1
    }
    mapped
  }

  override def deserialize(bytes: Array[Byte]): SimpleFeature = deserialize("", bytes, 0, bytes.length)

  override def deserialize(id: String, bytes: Array[Byte]): SimpleFeature = deserialize(id, bytes, 0, bytes.length)

  override def deserialize(in: InputStream): SimpleFeature = throw new NotImplementedError

  override def deserialize(id: String, in: InputStream): SimpleFeature = throw new NotImplementedError

  override def deserialize(bytes: Array[Byte], offset: Int, length: Int): SimpleFeature =
    deserialize("", bytes, offset, length)

  override def deserialize(id: String, bytes: Array[Byte], offset: Int, length: Int): SimpleFeature = {
    val input = KryoFeatureDeserialization.getInput(bytes, offset, length)
    input.readByte() match {
      case KryoFeatureSerializer.Version3 => readFeatureV3(id, input)
      case KryoFeatureSerializer.Version2 => readFeatureV2(id, input)
      case b => throw new IllegalArgumentException(s"Can't process features serialized with version: $b")
    }
  }

  private def readFeatureV3(id: String, input: Input): SimpleFeature = {
    val count = input.readShort()
    val size = input.readByte()
    val offset = input.position()

    // read our null mask
    input.setPosition(offset + size * (count + 1))
    val nulls = IntBitSet.deserialize(input, count)

    // we should now be positioned to read the feature id
    val finalId = if (withoutId) { id } else { input.readString() }

    val attributes = indices.map { i =>
      if (i == -1 || i >= count || nulls.contains(i)) { null } else {
        // read the offset and go to the position for reading
        input.setPosition(offset + (i * size))
        input.setPosition(offset + (if (size == 2) { input.readShortUnsigned() } else { input.readInt() }))
        readers(i).apply(input)
      }
    }

    val userData = if (withoutUserData) { null } else {
      // read the offset and go to the position for reading
      input.setPosition(offset + (count * size))
      input.setPosition(offset + (if (size == 2) { input.readShortUnsigned() } else { input.readInt() }))
      KryoUserDataSerialization.deserialize(input)
    }

    new ScalaSimpleFeature(projected, finalId, attributes, userData)
  }

  private def readFeatureV2(id: String, input: Input): SimpleFeature = {
    val attributes = Array.ofDim[AnyRef](numProjectedAttributes)
    // read in the offsets
    val offset = input.position()
    val offsetStart = offset + input.readInt()
    val finalId = if (withoutId) { id }  else { input.readString() }
    input.setPosition(offsetStart)
    var i = 0
    while (i < indices.length) {
      val iOffset = if (input.position < input.limit) { offset + input.readInt(true) } else { -1 }
      val index = indices(i)
      if (index != -1) {
        offsetsV2(index) = iOffset
      }
      i += 1
    }
    // read in the values
    i = 0
    while (i < numProjectedAttributes) {
      val offset = offsetsV2(i)
      if (offset != -1) {
        input.setPosition(offset)
        attributes(i) = readersInOrderV2(i)(input)
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
      sf.getUserData.putAll(KryoUserDataSerialization.deserialize(input))
    }
    sf
  }
}
