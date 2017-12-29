/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.kryo.impl

import java.io.InputStream
import java.util.{Date, UUID}

import com.esotericsoftware.kryo.io.Input
import org.locationtech.geomesa.features.SimpleFeatureSerializer
import org.locationtech.geomesa.features.kryo.KryoBufferSimpleFeature
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer.NULL_BYTE
import org.locationtech.geomesa.features.kryo.impl.KryoFeatureDeserialization.getReaders
import org.locationtech.geomesa.features.kryo.json.KryoJsonSerialization
import org.locationtech.geomesa.features.kryo.serialization.{KryoGeometrySerialization, KryoUserDataSerialization}
import org.locationtech.geomesa.features.serialization.ObjectType
import org.locationtech.geomesa.features.serialization.ObjectType.ObjectType
import org.locationtech.geomesa.utils.cache.{CacheKeyGenerator, SoftThreadLocal, SoftThreadLocalCache}
import org.opengis.feature.simple.SimpleFeatureType

/**
  * Base trait for kryo deserialization
  */
trait KryoFeatureDeserialization extends SimpleFeatureSerializer {

  private [kryo] def deserializeSft: SimpleFeatureType

  private val withoutId = options.withoutId
  private val withoutUserData = !options.withUserData

  protected val readers: Array[Input => AnyRef] = getReaders(CacheKeyGenerator.cacheKey(deserializeSft), deserializeSft)

  protected def readUserData(input: Input, skipOffsets: Boolean): java.util.Map[AnyRef, AnyRef] = {
    if (withoutUserData) {
      new java.util.HashMap[AnyRef, AnyRef]
    } else {
      if (skipOffsets) {
        // skip offset data
        var i = 0
        while (i < readers.length) {
          input.readInt(true)
          i += 1
        }
      }
      KryoUserDataSerialization.deserialize(input)
    }
  }

  protected def readId(input: Input): String = {
    if (withoutId) { "" } else {
      input.readString()
    }
  }

  def getReusableFeature: KryoBufferSimpleFeature =
    new KryoBufferSimpleFeature(deserializeSft, readers, readUserData(_, skipOffsets = false), options)
}

object KryoFeatureDeserialization {

  private[this] val inputs  = new SoftThreadLocal[Input]()
  private[this] val readers = new SoftThreadLocalCache[String, Array[(Input) => AnyRef]]()

  def getInput(bytes: Array[Byte], offset: Int, count: Int): Input = {
    val in = inputs.getOrElseUpdate(new Input)
    in.setBuffer(bytes, offset, offset + count)
    in
  }

  def getInput(stream: InputStream): Input = {
    val in = inputs.getOrElseUpdate(new Input)
    in.setBuffer(Array.ofDim(1024))
    in.setInputStream(stream)
    in
  }

  private [kryo] def getReaders(key: String, sft: SimpleFeatureType): Array[(Input) => AnyRef] = {
    import scala.collection.JavaConversions._
    readers.getOrElseUpdate(key, sft.getAttributeDescriptors.map { ad =>
      val bindings = ObjectType.selectType(ad.getType.getBinding, ad.getUserData)
      matchReader(bindings)
    }.toArray)
  }

  private [kryo] def matchReader(bindings: Seq[ObjectType]): (Input) => AnyRef = {
    bindings.head match {
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
      case ObjectType.GEOMETRY => KryoGeometrySerialization.deserialize // null checks are handled by geometry serializer
      case ObjectType.JSON => (i: Input) => KryoJsonSerialization.deserializeAndRender(i)
      case ObjectType.LIST =>
        val valueReader = matchReader(bindings.drop(1))
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
        val keyReader = matchReader(bindings.slice(1, 2))
        val valueReader = matchReader(bindings.drop(2))
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

  private def readNullable(wrapped: (Input) => AnyRef): (Input) => AnyRef = {
    (i: Input) => {
      if (i.read() == NULL_BYTE) {
        null
      } else {
        wrapped(i)
      }
    }
  }
}
