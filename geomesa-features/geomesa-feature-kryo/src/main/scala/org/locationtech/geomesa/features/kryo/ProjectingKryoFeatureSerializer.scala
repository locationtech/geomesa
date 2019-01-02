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
import org.locationtech.geomesa.utils.cache.CacheKeyGenerator
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
  * @param original the simple feature type that will be serialized
  * @param projected the simple feature type to project to when serializing
  */
class ProjectingKryoFeatureSerializer(original: SimpleFeatureType,
                                      projected: SimpleFeatureType,
                                      val options: Set[SerializationOption] = Set.empty)
    extends SimpleFeatureSerializer with MutableActiveDeserialization {

  import KryoFeatureSerializer._

  import scala.collection.JavaConversions._

  require(!options.withUserData, "User data serialization not supported")

  override private [kryo] def deserializeSft = projected

  private val cacheKey = CacheKeyGenerator.cacheKey(projected)
  private val numAttributes = projected.getAttributeCount
  private val writers = KryoFeatureSerialization.getWriters(cacheKey, projected)
  private val mappings = Array.ofDim[Int](numAttributes)
  private val withId = !options.withoutId

  projected.getAttributeDescriptors.zipWithIndex.foreach { case (d, i) =>
    mappings(i) = original.indexOf(d.getLocalName)
  }

  override def serialize(feature: SimpleFeature, out: OutputStream): Unit =
    writeFeature(feature, KryoFeatureSerialization.getOutput(out))

  override def serialize(feature: SimpleFeature): Array[Byte] = {
    val output = KryoFeatureSerialization.getOutput(null)
    writeFeature(feature, output)
    output.toBytes
  }

  private def writeFeature(sf: SimpleFeature, output: Output): Unit = {
    val offsets = KryoFeatureSerialization.getOffsets(cacheKey, numAttributes)
    output.writeInt(VERSION, true)
    output.setPosition(5) // leave 4 bytes to write the offsets
    if (withId) {
      output.writeString(sf.getID)  // TODO optimize for uuids?
    }
    // write attributes and keep track off offset into byte array
    var i = 0
    while (i < numAttributes) {
      offsets(i) = output.position()
      writers(i)(output, sf.getAttribute(mappings(i)))
      i += 1
    }
    // write the offsets - variable width
    i = 0
    val offsetStart = output.position()
    while (i < numAttributes) {
      output.writeInt(offsets(i), true)
      i += 1
    }
    // got back and write the start position for the offsets
    val total = output.position()
    output.setPosition(1)
    output.writeInt(offsetStart)
    // reset the position back to the end of the buffer so that toBytes works, and we can keep writing user data
    output.setPosition(total)
  }
}
