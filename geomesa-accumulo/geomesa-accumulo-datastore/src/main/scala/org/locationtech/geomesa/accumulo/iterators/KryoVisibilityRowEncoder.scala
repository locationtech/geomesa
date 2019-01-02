/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import com.esotericsoftware.kryo.io.Output
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.accumulo.core.iterators.user.RowEncodingIterator
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.features.kryo.impl.{KryoFeatureDeserialization, KryoFeatureSerialization}
import org.locationtech.geomesa.index.iterators.IteratorCache
import org.locationtech.geomesa.utils.cache.CacheKeyGenerator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType

/**
  * Assumes cq are byte-encoded attribute number
  */
class KryoVisibilityRowEncoder extends RowEncodingIterator {

  private var sft: SimpleFeatureType = _
  private var nullBytes: Array[Array[Byte]] = _
  private var offsets: Array[Int] = _
  private var offsetStart: Int = -1

  private val output: Output = new Output(128, -1)

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: java.util.Map[String, String],
                    env: IteratorEnvironment): Unit = {
    super.init(source, options, env)

    sft = IteratorCache.sft(options.get(KryoVisibilityRowEncoder.SftOpt))
    if (offsets == null || offsets.length != sft.getAttributeCount) {
      offsets = Array.ofDim[Int](sft.getAttributeCount)
    }
    val cacheKey = CacheKeyGenerator.cacheKey(sft)
    nullBytes = KryoFeatureSerialization.getWriters(cacheKey, sft).map { writer =>
      output.clear()
      writer(output, null)
      output.toBytes
    }
  }

  override def rowEncoder(keys: java.util.List[Key], values: java.util.List[Value]): Value = {
    if (values.size() == 1) {
      return values.get(0)
    }

    val allValues = Array.ofDim[Array[Byte]](sft.getAttributeCount)
    var i = 0
    while (i < keys.size) {
      val cq = keys.get(i).getColumnQualifier
      val comma = cq.find(",")
      val indices = if (comma == -1) cq.getBytes.map(_.toInt) else cq.getBytes.drop(comma + 1).map(_.toInt)

      val bytes = values.get(i).get

      readOffsets(bytes)

      // set the non-null values
      indices.foreach { index =>
        val endIndex = offsets.indexWhere(_ != -1, index + 1)
        val end = if (endIndex == -1) offsetStart else offsets(endIndex)
        val length = end - offsets(index)
        val values = Array.ofDim[Byte](length)
        System.arraycopy(bytes, offsets(index), values, 0, length)
        allValues(index) = values
      }

      i += 1
    }

    i = 0
    while (i < allValues.length) {
      if (allValues(i) == null) {
        allValues(i) = nullBytes(i)
      }
      i += 1
    }

    // TODO if we don't have a geometry, skip the record?
    KryoVisibilityRowEncoder.encode(allValues, output, offsets)
  }

  /**
    * Reads offsets in the 'offsets' array and sets the start of the offset block
    *
    * @param bytes kryo feature bytes
    * @return
    */
  private def readOffsets(bytes: Array[Byte]): Unit = {
    val input = KryoFeatureDeserialization.getInput(bytes, 0, bytes.length)
    // reset our offsets
    input.setPosition(1) // skip version
    offsetStart = input.readInt()
    input.setPosition(offsetStart) // set to offsets start
    var i = 0
    while (i < offsets.length) {
      offsets(i) = if (input.position < input.limit) input.readInt(true) else -1
      i += 1
    }
  }

  override def rowDecoder(rowKey: Key, rowValue: Value): java.util.SortedMap[Key, Value] =
    throw new NotImplementedError("")

  override def deepCopy(env: IteratorEnvironment): SortedKeyValueIterator[Key, Value] = {
    val iterator = new KryoVisibilityRowEncoder
    if (sourceIter != null) {
      iterator.sourceIter = sourceIter.deepCopy(env)
    }
    iterator.sft = sft
    iterator.offsets = Array.ofDim[Int](sft.getAttributeCount)
    iterator.nullBytes = nullBytes
    iterator
  }
}

object KryoVisibilityRowEncoder {

  val SftOpt   = "sft"

  val DefaultPriority = 21 // needs to be first thing that runs after the versioning iterator at 20

  def configure(sft: SimpleFeatureType, priority: Int = DefaultPriority): IteratorSetting = {
    val is = new IteratorSetting(priority, "feature-merge-iter", classOf[KryoVisibilityRowEncoder])
    is.addOption(SftOpt, SimpleFeatureTypes.encodeType(sft, includeUserData = true)) // need user data for id calc
    is
  }

  private def encode(values: Array[Array[Byte]], output: Output, offsets: Array[Int]): Value = {
    output.clear()
    output.writeInt(KryoFeatureSerializer.VERSION, true)
    output.setPosition(5) // leave 4 bytes to write the offsets
    // note: we don't write ID - tables are assumed to be using serialization without IDs
    // write attributes and keep track off offset into byte array
    var i = 0
    while (i < values.length) {
      offsets(i) = output.position()
      output.write(values(i))
      i += 1
    }
    // write the offsets - variable width
    i = 0
    val offsetStart = output.position()
    while (i < values.length) {
      output.writeInt(offsets(i), true)
      i += 1
    }
    // got back and write the start position for the offsets
    val total = output.position()
    output.setPosition(1)
    output.writeInt(offsetStart)
    output.setPosition(total) // set back to the end so that we get all the bytes

    new Value(output.toBytes)
  }
}
