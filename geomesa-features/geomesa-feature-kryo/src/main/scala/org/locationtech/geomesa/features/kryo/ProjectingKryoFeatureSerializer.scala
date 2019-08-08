/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.kryo

import java.io.OutputStream

import com.esotericsoftware.kryo.io.Output
import org.locationtech.geomesa.features.SerializationOption.SerializationOption
import org.locationtech.geomesa.features.SimpleFeatureSerializer
import org.locationtech.geomesa.features.kryo.impl.ActiveDeserialization.MutableActiveDeserialization
import org.locationtech.geomesa.features.kryo.impl.KryoFeatureSerialization
import org.locationtech.geomesa.features.kryo.serialization.KryoUserDataSerialization
import org.locationtech.geomesa.utils.cache.CacheKeyGenerator
import org.locationtech.geomesa.utils.collection.IntBitSet
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
  * @param original the simple feature type that will be serialized
  * @param projected the simple feature type to project to when serializing
  */
class ProjectingKryoFeatureSerializer(
    original: SimpleFeatureType,
    projected: SimpleFeatureType,
    val options: Set[SerializationOption] = Set.empty
  ) extends SimpleFeatureSerializer with MutableActiveDeserialization {

  override protected [kryo] def out: SimpleFeatureType = projected

  private val withId = !options.withoutId
  private val withUserData = options.withUserData
  private val count = projected.getAttributeCount
  private val writers = KryoFeatureSerialization.getWriters(CacheKeyGenerator.cacheKey(projected), projected)
  private val mappings = Array.tabulate(count)(i => original.indexOf(projected.getDescriptor(i).getLocalName))

  override def serialize(feature: SimpleFeature, out: OutputStream): Unit =
    writeFeature(feature, KryoFeatureSerialization.getOutput(out))

  override def serialize(feature: SimpleFeature): Array[Byte] = {
    val output = KryoFeatureSerialization.getOutput(null)
    writeFeature(feature, output)
    output.toBytes
  }

  private def writeFeature(sf: SimpleFeature, output: Output): Unit = {
    output.writeByte(KryoFeatureSerializer.Version)
    output.writeShort(count) // track the number of attributes
    val offset = output.position()
    output.setPosition(offset + metadataSize(count))
    if (withId) {
      output.writeString(sf.getID) // TODO optimize for uuids?
    }
    // write attributes and keep track off offset into byte array
    val nulls = IntBitSet(count)
    var i = 0
    while (i < count) {
      val position = output.position()
      output.setPosition(offset + (i * 2))
      output.writeShort(position - offset)
      output.setPosition(position)
      val attribute = sf.getAttribute(mappings(i))
      if (attribute == null) {
        nulls.add(i)
      } else {
        writers(i).apply(output, attribute)
      }
      i += 1
    }
    val userDataPosition = output.position()
    output.setPosition(offset + (i * 2))
    output.writeShort(userDataPosition - offset)
    output.setPosition(userDataPosition)
    if (withUserData) {
      KryoUserDataSerialization.serialize(output, sf.getUserData)
    }
    val end = output.position()
    if (end - offset > Short.MaxValue.toInt) {
      // TODO handle overflow
      throw new NotImplementedError(s"Serialized feature exceeds max byte size (${Short.MaxValue}): ${end - offset}")
    }
    // go back and write the nulls
    output.setPosition(offset + (2 * count) + 2)
    nulls.serialize(output)
    // reset the position back to the end of the buffer so the bytes aren't lost
    output.setPosition(end)
  }
}
