/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.features.kryo

import java.util.{Date, UUID, HashMap => jHashMap, List => jList, Map => jMap}

import com.esotericsoftware.kryo.io.{Input, Output}
import com.vividsolutions.jts.geom.Geometry
import org.locationtech.geomesa.features.SerializationOption.SerializationOption
import org.locationtech.geomesa.features._
import org.locationtech.geomesa.features.kryo.serialization.{KryoReader, KryoWriter}
import org.locationtech.geomesa.features.serialization.ObjectType
import org.locationtech.geomesa.features.serialization.ObjectType.ObjectType
import org.locationtech.geomesa.utils.cache.{CacheKeyGenerator, SoftThreadLocal, SoftThreadLocalCache}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._

class KryoFeatureSerializer(sft: SimpleFeatureType, val options: Set[SerializationOption] = Set.empty)
    extends KryoSimpleFeatureSerialization with KryoSimpleFeatureDeserialization {
  override private [kryo] def serializeSft = sft
  override private [kryo] def deserializeSft = sft
}

/**
  * @param original the simple feature type that will be serialized
  * @param projected the simple feature type to project to when serializing
  */
class ProjectingKryoFeatureSerializer(original: SimpleFeatureType,
                                      projected: SimpleFeatureType,
                                      val options: Set[SerializationOption] = Set.empty)
    extends SimpleFeatureSerializer with KryoSimpleFeatureDeserialization {

  import KryoFeatureSerializer._

  require(!options.withUserData, "User data serialization not supported")

  override private [kryo] def deserializeSft = projected

  private val cacheKey = CacheKeyGenerator.cacheKey(projected)
  private val numAttributes = projected.getAttributeCount
  private val writers = getWriters(cacheKey, projected)
  private val mappings = Array.ofDim[Int](numAttributes)

  projected.getAttributeDescriptors.zipWithIndex.foreach { case (d, i) =>
    mappings(i) = original.indexOf(d.getLocalName)
  }

  override def serialize(sf: SimpleFeature): Array[Byte] = {
    val offsets = getOffsets(cacheKey, numAttributes)
    val output = getOutput()
    output.writeInt(VERSION, true)
    output.setPosition(5) // leave 4 bytes to write the offsets
    if (!options.withoutId) {
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
    // reset the position back to the end of the buffer so that toBytes works
    output.setPosition(total)
    output.toBytes
  }
}

trait KryoSimpleFeatureSerialization extends SimpleFeatureSerializer {

  import KryoFeatureSerializer._

  private [kryo] def serializeSft: SimpleFeatureType

  private val cacheKey = CacheKeyGenerator.cacheKey(serializeSft)
  private val numAttributes = serializeSft.getAttributeCount
  private val writers = getWriters(cacheKey, serializeSft)

  override def serialize(sf: SimpleFeature): Array[Byte] = doWrite(sf)

  private val doWrite: (SimpleFeature) => Array[Byte] = if (options.withUserData) writeWithUserData else write

  private def write(sf: SimpleFeature): Array[Byte] = writeSf(sf).toBytes

  private def writeWithUserData(sf: SimpleFeature): Array[Byte] = {
    val out = writeSf(sf)
    kryoWriter.writeGenericMap(out, sf.getUserData)
    out.toBytes
  }

  private def writeSf(sf: SimpleFeature): Output = {
    val offsets = getOffsets(cacheKey, numAttributes)
    val output = getOutput()
    output.writeInt(VERSION, true)
    output.setPosition(5) // leave 4 bytes to write the offsets
    if (!options.withoutId) {
      output.writeString(sf.getID)  // TODO optimize for uuids?
    }
    // write attributes and keep track off offset into byte array
    var i = 0
    while (i < numAttributes) {
      offsets(i) = output.position()
      writers(i)(output, sf.getAttribute(i))
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
    // reset the position back to the end of the buffer so we can keep writing more things (e.g. user data)
    output.setPosition(total)
    output
  }
}

trait KryoSimpleFeatureDeserialization extends SimpleFeatureSerializer {

  import KryoFeatureSerializer._

  private[kryo] def deserializeSft: SimpleFeatureType

  private val cacheKey = CacheKeyGenerator.cacheKey(deserializeSft)
  private val numAttributes = deserializeSft.getAttributeCount

  private val readers = getReaders(cacheKey, deserializeSft)

  private lazy val legacySerializer = serialization.KryoFeatureSerializer(deserializeSft, deserializeSft, options)
  private lazy val lazyUserData: (Input) => jMap[AnyRef, AnyRef] = if (options.withUserData) {
    (input) => kryoReader.readGenericMap(VERSION)(input)
  } else {
    (input) => new jHashMap[AnyRef, AnyRef]
  }

  def getReusableFeature: KryoBufferSimpleFeature =
    new KryoBufferSimpleFeature(deserializeSft, readers, lazyUserData, options)

  override def deserialize(bytes: Array[Byte]): SimpleFeature = doRead(bytes)

  private val doRead: (Array[Byte]) => SimpleFeature = if (options.withUserData) readWithUserData else read

  protected[kryo] def read(bytes: Array[Byte]): SimpleFeature = readSf(bytes)._1

  protected[kryo] def readWithUserData(bytes: Array[Byte]): SimpleFeature = {
    val (sf, input) = readSf(bytes)
    // skip offset data
    var i = 0
    while (i < numAttributes) {
      input.readInt(true)
      i += 1
    }
    val ud = kryoReader.readGenericMap(VERSION)(input)
    sf.getUserData.putAll(ud)
    sf
  }

  protected[kryo] def readSf(bytes: Array[Byte]): (SimpleFeature, Input) = {
    val input = getInput(bytes)
    if (input.readInt(true) == 1) {
      return (legacySerializer.read(bytes), input)
    }
    val limit = input.readInt() // read the start of the offsets - we'll stop reading when we hit this
    val id = if (options.withoutId) "" else input.readString()
    val attributes = Array.ofDim[AnyRef](numAttributes)
    var i = 0
    while (i < numAttributes && input.position < limit) {
      attributes(i) = readers(i)(input)
      i += 1
    }
    (new ScalaSimpleFeature(id, deserializeSft, attributes), input)
  }
}

object KryoFeatureSerializer {

  val VERSION = 2
  assert(VERSION < 8, "Serialization expects version to be in one byte")

  val NULL_BYTE     = 0.asInstanceOf[Byte]
  val NON_NULL_BYTE = 1.asInstanceOf[Byte]

  private[this] val inputs  = new SoftThreadLocal[Input]()
  private[this] val outputs = new SoftThreadLocal[Output]()
  private[this] val readers = new SoftThreadLocalCache[String, Array[(Input) => AnyRef]]()
  private[this] val writers = new SoftThreadLocalCache[String, Array[(Output, AnyRef) => Unit]]()
  private[this] val offsets = new SoftThreadLocalCache[String, Array[Int]]()

  lazy val kryoReader = new KryoReader()
  lazy val kryoWriter = new KryoWriter()

  def getInput(bytes: Array[Byte]): Input = {
    val in = inputs.getOrElseUpdate(new Input)
    in.setBuffer(bytes)
    in
  }

  // noinspection AccessorLikeMethodIsEmptyParen
  def getOutput(): Output = {
    val out = outputs.getOrElseUpdate(new Output(1024, -1))
    out.clear()
    out
  }

  def getOffsets(sft: String, size: Int): Array[Int] =
    offsets.getOrElseUpdate(sft, Array.ofDim[Int](size))

  // noinspection UnitInMap
  def getWriters(key: String, sft: SimpleFeatureType): Array[(Output, AnyRef) => Unit] = {
    writers.getOrElseUpdate(key, sft.getAttributeDescriptors.map { ad =>
      val (otype, bindings) = ObjectType.selectType(ad.getType.getBinding, ad.getUserData)
      matchWriter(otype, bindings)
    }.toArray)
  }

  def matchWriter(otype: ObjectType, bindings: Seq[ObjectType] = Seq.empty): (Output, AnyRef) => Unit = {
    otype match {
      case ObjectType.STRING =>
        (o: Output, v: AnyRef) => o.writeString(v.asInstanceOf[String]) // write string supports nulls
      case ObjectType.INT =>
        val w = (o: Output, v: AnyRef) => o.writeInt(v.asInstanceOf[Int])
        writeNullable(w)
      case ObjectType.LONG =>
        val w = (o: Output, v: AnyRef) => o.writeLong(v.asInstanceOf[Long])
        writeNullable(w)
      case ObjectType.FLOAT =>
        val w = (o: Output, v: AnyRef) => o.writeFloat(v.asInstanceOf[Float])
        writeNullable(w)
      case ObjectType.DOUBLE =>
        val w = (o: Output, v: AnyRef) => o.writeDouble(v.asInstanceOf[Double])
        writeNullable(w)
      case ObjectType.BOOLEAN =>
        val w = (o: Output, v: AnyRef) => o.writeBoolean(v.asInstanceOf[Boolean])
        writeNullable(w)
      case ObjectType.DATE =>
        val w = (o: Output, v: AnyRef) => o.writeLong(v.asInstanceOf[Date].getTime)
        writeNullable(w)
      case ObjectType.UUID =>
        val w = (o: Output, v: AnyRef) => {
          val uuid = v.asInstanceOf[UUID]
          o.writeLong(uuid.getMostSignificantBits)
          o.writeLong(uuid.getLeastSignificantBits)
        }
        writeNullable(w)
      case ObjectType.GEOMETRY =>
        writeNullable((o: Output, v: AnyRef) => kryoWriter.writeGeometryNotNull(o, v.asInstanceOf[Geometry]))
      case ObjectType.LIST =>
        val valueWriter = matchWriter(bindings.head)
        (o: Output, v: AnyRef) => {
          val list = v.asInstanceOf[jList[AnyRef]]
          if (list == null) {
            o.writeInt(-1, true)
          } else {
            o.writeInt(list.size(), true)
            val iter = list.iterator()
            while (iter.hasNext) {
              valueWriter(o, iter.next())
            }
          }
        }
      case ObjectType.MAP =>
        val keyWriter = matchWriter(bindings.head)
        val valueWriter = matchWriter(bindings(1))
        (o: Output, v: AnyRef) => {
          val map = v.asInstanceOf[jMap[AnyRef, AnyRef]]
          if (map == null) {
            o.writeInt(-1, true)
          } else {
            o.writeInt(map.size(), true)
            val iter = map.entrySet.iterator()
            while (iter.hasNext) {
              val entry = iter.next()
              keyWriter(o, entry.getKey)
              valueWriter(o, entry.getValue)
            }
          }
        }
      case ObjectType.BYTES =>
        (o: Output, v: AnyRef) => {
          val arr = v.asInstanceOf[Array[Byte]]
          if (arr == null) {
            o.writeInt(-1, true)
          } else {
            o.writeInt(arr.length, true)
            o.writeBytes(arr)
          }
        }
    }
  }

  def writeNullable(wrapped: (Output, AnyRef) => Unit): (Output, AnyRef) => Unit = {
    (o: Output, v: AnyRef) => {
      if (v == null) {
        o.write(NULL_BYTE)
      } else {
        o.write(NON_NULL_BYTE)
        wrapped(o, v)
      }
    }
  }

  def getReaders(key: String, sft: SimpleFeatureType): Array[(Input) => AnyRef] = {
    readers.getOrElseUpdate(key, sft.getAttributeDescriptors.map { ad =>
      val (otype, bindings)  = ObjectType.selectType(ad.getType.getBinding, ad.getUserData)
      matchReader(otype, bindings)
    }.toArray)
  }

  def matchReader(otype: ObjectType, bindings: Seq[ObjectType] = Seq.empty): (Input) => AnyRef = {
    otype match {
      case ObjectType.STRING => (i: Input) => i.readString()
      case ObjectType.INT => readNullable((i: Input) => i.readInt().asInstanceOf[AnyRef])
      case ObjectType.LONG => readNullable((i: Input) => i.readLong().asInstanceOf[AnyRef])
      case ObjectType.FLOAT => readNullable((i: Input) => i.readFloat().asInstanceOf[AnyRef])
      case ObjectType.DOUBLE => readNullable((i: Input) => i.readDouble().asInstanceOf[AnyRef])
      case ObjectType.BOOLEAN => readNullable((i: Input) => i.readBoolean().asInstanceOf[AnyRef])
      case ObjectType.DATE => readNullable((i: Input) => new Date(i.readLong()).asInstanceOf[AnyRef])
      case ObjectType.UUID =>
        val w = (i: Input) => {
          val mostSignificantBits = i.readLong()
          val leastSignificantBits = i.readLong()
          new UUID(mostSignificantBits, leastSignificantBits)
        }
        readNullable(w)
      case ObjectType.GEOMETRY => readNullable((i: Input) => kryoReader.readGeometry(i))
      case ObjectType.LIST =>
        val valueReader = matchReader(bindings.head)
        (i: Input) => {
          val size = i.readInt(true)
          if (size == -1) {
            null
          } else {
            val list = new java.util.ArrayList[AnyRef](size)
            var index = 0
            while (index < size) {
              list.add(valueReader(i))
              index += 1
            }
            list
          }
        }
      case ObjectType.MAP =>
        val keyReader = matchReader(bindings.head)
        val valueReader = matchReader(bindings(1))
        (i: Input) => {
          val size = i.readInt(true)
          if (size == -1) {
            null
          } else {
            val map = new java.util.HashMap[AnyRef, AnyRef](size)
            var index = 0
            while (index < size) {
              map.put(keyReader(i), valueReader(i))
              index += 1
            }
            map
          }
        }
      case ObjectType.BYTES =>
        (i: Input) => {
          val size = i.readInt(true)
          if (size == -1) {
            null
          } else {
            val arr = new Array[Byte](size)
            i.read(arr)
            arr
          }
        }
    }
  }

  def readNullable(wrapped: (Input) => AnyRef): (Input) => AnyRef = {
    (i: Input) => {
      if (i.read() == NULL_BYTE) {
        null
      } else {
        wrapped(i)
      }
    }
  }
}


/**
  * @param original the simple feature type that was encoded
  * @param projected the simple feature type to project to when decoding
  * @param options the options what were applied when encoding
  */
@deprecated
class ProjectingKryoFeatureDeserializer(original: SimpleFeatureType,
                                        projected: SimpleFeatureType,
                                        val options: Set[SerializationOption] = Set.empty)
    extends KryoSimpleFeatureSerialization {

  import KryoFeatureSerializer._

  override private [kryo] def serializeSft = original

  private val numProjectedAttributes = projected.getAttributeCount
  private val offsets = Array.fill[Int](numProjectedAttributes)(-1)
  private val readersInOrder = Array.ofDim[(Input) => AnyRef](numProjectedAttributes)
  private val indices = Array.ofDim[Int](original.getAttributeCount)

  private lazy val legacySerializer = serialization.KryoFeatureSerializer(original, projected, options)

  setup()

  private def setup(): Unit = {
    val originalReaders = getReaders(CacheKeyGenerator.cacheKey(original), original)
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

  override def deserialize(bytes: Array[Byte]): SimpleFeature = {
    val input = getInput(bytes)
    if (input.readInt(true) == 1) {
      return legacySerializer.read(bytes)
    }
    val attributes = Array.ofDim[AnyRef](numProjectedAttributes)
    // read in the offsets
    val offsetStart = input.readInt()
    val id = input.readString()
    input.setPosition(offsetStart)
    var i = 0
    while (i < indices.length) {
      val offset = if (input.position < input.limit) input.readInt(true) else -1
      val index = indices(i)
      if (index != -1) {
        offsets(index) = offset
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
    val sf = new ScalaSimpleFeature(id, projected, attributes)
    if (options.withUserData) {
      // skip offset data
      input.setPosition(offsetStart)
      var i = 0
      while (i < original.getAttributeCount) {
        input.readInt(true)
        i += 1
      }
      val ud = kryoReader.readGenericMap(VERSION)(input)
      sf.getUserData.putAll(ud)
      sf
    }
    sf
  }
}
