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
    output.writeByte(KryoFeatureSerializer.Version3)
    val offset = Metadata.write(output, count, 2)
    if (withId) {
      output.writeString(sf.getID) // TODO optimize for uuids?
    }
    // write attributes and keep track off offset into byte array
    val offsets = Array.ofDim[Int](count + 1)
    val nulls = IntBitSet(count)
    var i = 0
    while (i < count) {
      offsets(i) = output.position() - offset
      val attribute = sf.getAttribute(mappings(i))
      if (attribute == null) {
        nulls.add(i)
      } else {
        writers(i).apply(output, attribute)
      }
      i += 1
    }
    offsets(i) = output.position() - offset // user data position
    if (withUserData) {
      KryoUserDataSerialization.serialize(output, sf.getUserData)
    }
    val end = output.position()
    if (end - offset > KryoFeatureSerialization.MaxUnsignedShort) {
      // we need to shift the bytes rightwards to add space for writing ints instead of shorts for the offsets
      val shift = 2 * (count + 1)
      if (output.getBuffer.length < end + shift) {
        val expanded = Array.ofDim[Byte](end + shift)
        System.arraycopy(output.getBuffer, 0, expanded, 0, end)
        output.setBuffer(expanded)
      }
      val buffer = output.getBuffer
      var i = end
      while (i > offset) {
        buffer(i + shift) = buffer(i)
        i -= 1
      }
      // go back and write the offsets and nulls
      output.setPosition(offset - 1)
      output.write(4) // 4 bytes per offset
      offsets.foreach(output.writeInt)
      nulls.serialize(output)
      // reset the position back to the end of the buffer so the bytes aren't lost
      output.setPosition(end + shift)
    } else {
      // go back and write the offsets and nulls
      output.setPosition(offset)
      offsets.foreach(output.writeShort)
      nulls.serialize(output)
      // reset the position back to the end of the buffer so the bytes aren't lost
      output.setPosition(end)
    }
  }
}
