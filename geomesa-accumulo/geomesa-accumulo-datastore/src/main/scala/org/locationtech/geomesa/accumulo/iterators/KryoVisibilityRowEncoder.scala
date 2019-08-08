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
import org.locationtech.geomesa.features.kryo.impl.KryoFeatureDeserialization
import org.locationtech.geomesa.features.serialization.ObjectType
import org.locationtech.geomesa.index.iterators.IteratorCache
import org.locationtech.geomesa.utils.collection.IntBitSet
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType

/**
  * Assumes cq are byte-encoded attribute number
  */
class KryoVisibilityRowEncoder extends RowEncodingIterator {

  private var sft: SimpleFeatureType = _
  private var count: Int = -1
  private var attributes: Array[(Array[Byte], Int, Int)] = _ // (bytes, offset, length)

  private var nullBytesV2: Array[Array[Byte]] = _
  private var offsetsV2: Array[Int] = _
  private var offsetStart: Int = -1

  private val output: Output = new Output(128, -1)

  override def init(
      source: SortedKeyValueIterator[Key, Value],
      options: java.util.Map[String, String],
      env: IteratorEnvironment): Unit = {
    super.init(source, options, env)
    sft = IteratorCache.sft(options.get(KryoVisibilityRowEncoder.SftOpt))
    count = sft.getAttributeCount
    if (attributes == null || attributes.length < count) {
      attributes = Array.ofDim[(Array[Byte], Int, Int)](count)
    }
    nullBytesV2 = null // lazily initialized when we hit v2 encoded data
  }

  override def rowEncoder(keys: java.util.List[Key], values: java.util.List[Value]): Value = {
    if (values.size() == 1) {
      return values.get(0)
    }
    // TODO if we don't have a geometry, skip the record?
    values.get(0).get.head match {
      case KryoFeatureSerializer.Version  => encodeV3(keys, values)
      case KryoFeatureSerializer.Version2 => encodeV2(keys, values)
    }
  }

  private def encodeV3(keys: java.util.List[Key], values: java.util.List[Value]): Value = {
    // 1 byte for version + 2 bytes for count + metadata
    var length = 3 + org.locationtech.geomesa.features.kryo.metadataSize(count)
    var valueCursor = length

    var i = 0
    while (i < keys.size) {
      val bytes = values.get(i).get
      val input = KryoFeatureDeserialization.getInput(bytes, 0, bytes.length)

      keys.get(i).getColumnQualifier.getBytes.foreach { unsigned =>
        val index = java.lang.Byte.toUnsignedInt(unsigned)
        input.setPosition(3 + (2 * index))
        val pos = input.readShort()
        val len = input.readShort() - pos
        attributes(index) = (bytes, 3 + pos, len)
        length += len
      }
      i += 1
    }

    val value = Array.ofDim[Byte](length)
    val output = new Output(value)
    output.writeByte(KryoFeatureSerializer.Version)
    output.writeShort(count)

    val nulls = IntBitSet(count)

    i = 0
    while (i < count) {
      output.writeShort(valueCursor - 3) // offset relative to version + count
      val attribute = attributes(i)
      if (attribute == null) { nulls.add(i) } else {
        val (bytes, offset, len) = attribute
        System.arraycopy(bytes, offset, value, valueCursor, len)
        valueCursor += len
        attributes(i) = null // reset for next time through
      }
      i += 1
    }
    output.writeShort(valueCursor) // user-data position

    // write nulls - we should already be in the right position
    nulls.serialize(output)

    new Value(value)
  }

  private def encodeV2(keys: java.util.List[Key], values: java.util.List[Value]): Value = {
    setupV2()
    val allValues = Array.ofDim[Array[Byte]](sft.getAttributeCount)
    var i = 0
    while (i < keys.size) {
      val cq = keys.get(i).getColumnQualifier
      val comma = cq.find(",")
      // convert unsigned bytes to int
      val indices = if (comma == -1) { cq.getBytes.map(java.lang.Byte.toUnsignedInt) } else { cq.getBytes.drop(comma + 1).map(java.lang.Byte.toUnsignedInt) }

      val bytes = values.get(i).get

      val input = KryoFeatureDeserialization.getInput(bytes, 0, bytes.length)
      // reset our offsets
      input.setPosition(1) // skip version
      offsetStart = input.readInt()
      input.setPosition(offsetStart) // set to offsets start
      var j = 0
      while (j < offsetsV2.length) {
        offsetsV2(j) = if (input.position < input.limit) { input.readInt(true) } else { -1 }
        j += 1
      }

      // set the non-null values
      indices.foreach { index =>
        val endIndex = offsetsV2.indexWhere(_ != -1, index + 1)
        val end = if (endIndex == -1) offsetStart else offsetsV2(endIndex)
        val length = end - offsetsV2(index)
        val values = Array.ofDim[Byte](length)
        System.arraycopy(bytes, offsetsV2(index), values, 0, length)
        allValues(index) = values
      }

      i += 1
    }

    i = 0
    while (i < allValues.length) {
      if (allValues(i) == null) {
        allValues(i) = nullBytesV2(i)
      }
      i += 1
    }

    KryoVisibilityRowEncoder.encodeV2(allValues, output, offsetsV2)
  }

  private def setupV2(): Unit = {
    if (nullBytesV2 == null) {
      if (offsetsV2 == null || offsetsV2.length != sft.getAttributeCount) {
        offsetsV2 = Array.ofDim[Int](sft.getAttributeCount)
      }
      nullBytesV2 = Array.tabulate(sft.getAttributeCount) { i =>
        val descriptor = sft.getDescriptor(i)
        val bindings = ObjectType.selectType(descriptor)
        output.clear()
        bindings.head match {
          case ObjectType.STRING if bindings.last != ObjectType.JSON => output.writeString(null) // write string supports nulls
          case ObjectType.LIST | ObjectType.MAP | ObjectType.BYTES   => output.writeInt(-1, true)
          case _ => output.write(KryoFeatureSerializer.NullByte)
        }
        output.toBytes
      }
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
    iterator.offsetsV2 = Array.ofDim[Int](sft.getAttributeCount)
    iterator.nullBytesV2 = nullBytesV2
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

  private def encodeV2(values: Array[Array[Byte]], output: Output, offsets: Array[Int]): Value = {
    output.clear()
    output.writeInt(KryoFeatureSerializer.Version2, true)
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
