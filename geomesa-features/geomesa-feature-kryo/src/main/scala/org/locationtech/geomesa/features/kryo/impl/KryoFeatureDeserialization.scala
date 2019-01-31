/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.kryo.impl

import java.io.InputStream
import java.util
import java.util.{Date, UUID}

import com.esotericsoftware.kryo.io.Input
import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.features.SimpleFeatureSerializer
import org.locationtech.geomesa.features.kryo.KryoBufferSimpleFeature
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer.NULL_BYTE
import org.locationtech.geomesa.features.kryo.impl.KryoFeatureDeserialization.getReaders
import org.locationtech.geomesa.features.kryo.json.KryoJsonSerialization
import org.locationtech.geomesa.features.kryo.serialization.{KryoGeometrySerialization, KryoUserDataSerialization}
import org.locationtech.geomesa.features.serialization.ObjectType
import org.locationtech.geomesa.features.serialization.ObjectType.ObjectType
import org.locationtech.geomesa.utils.cache.{CacheKeyGenerator, SoftThreadLocal, SoftThreadLocalCache}
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.SimpleFeatureType

import scala.util.control.NonFatal

/**
  * Base trait for kryo deserialization
  */
trait KryoFeatureDeserialization extends SimpleFeatureSerializer with LazyLogging {

  private [kryo] def deserializeSft: SimpleFeatureType

  private val withoutUserData = !options.withUserData
  protected val withoutId: Boolean = options.withoutId

  protected val readers: Array[Input => AnyRef] =
    getReaders(CacheKeyGenerator.cacheKey(deserializeSft), deserializeSft)

  protected def readUserData(input: Input, skipOffsets: Boolean): java.util.Map[AnyRef, AnyRef] = {
    if (withoutUserData) {
      new java.util.HashMap[AnyRef, AnyRef]
    } else {
      if (skipOffsets) {
        // skip offset data
        try {
          var i = 0
          while (i < readers.length) {
            input.readInt(true)
            i += 1
          }
        } catch {
          case NonFatal(e) =>
            logger.error("Error reading serialized kryo user data:", e)
            return new java.util.HashMap[AnyRef, AnyRef]()
        }
      }
      KryoUserDataSerialization.deserialize(input)
    }
  }

  def getReusableFeature: KryoBufferSimpleFeature =
    new KryoBufferSimpleFeature(deserializeSft, readers, readUserData(_, skipOffsets = false), options)
}

object KryoFeatureDeserialization extends LazyLogging {

  private[this] val inputs  = new SoftThreadLocal[Input]()
  private[this] val readers = new SoftThreadLocalCache[String, Array[Input => AnyRef]]()

  def getInput(bytes: Array[Byte], offset: Int, count: Int): Input = {
    val in = inputs.getOrElseUpdate(new Input)
    in.setBuffer(bytes, offset, count)
    in
  }

  def getInput(stream: InputStream): Input = {
    val in = inputs.getOrElseUpdate(new Input)
    in.setBuffer(Array.ofDim(1024))
    in.setInputStream(stream)
    in
  }

  private [geomesa] def getReaders(key: String, sft: SimpleFeatureType): Array[Input => AnyRef] = {
    readers.getOrElseUpdate(key, sft.getAttributeDescriptors.toArray.map {
      case ad: AttributeDescriptor => matchReader(ObjectType.selectType(ad))
    })
  }

  private [geomesa] def matchReader(bindings: Seq[ObjectType]): Input => AnyRef = {
    bindings.head match {
      case ObjectType.STRING   => StringReader
      case ObjectType.INT      => IntReader
      case ObjectType.LONG     => LongReader
      case ObjectType.FLOAT    => FloatReader
      case ObjectType.DOUBLE   => DoubleReader
      case ObjectType.DATE     => DateReader
      case ObjectType.UUID     => UuidReader
      case ObjectType.GEOMETRY => KryoGeometrySerialization.deserialize
      case ObjectType.JSON     => KryoJsonSerialization.deserializeAndRender
      case ObjectType.BYTES    => BytesReader
      case ObjectType.BOOLEAN  => BooleanReader
      case ObjectType.LIST     => new ListReader(matchReader(bindings.drop(1)))
      case ObjectType.MAP      => new MapReader(matchReader(bindings.slice(1, 2)), matchReader(bindings.drop(2)))
    }
  }

  private object StringReader extends (Input => String) {
    override def apply(input: Input): String = try { input.readString() } catch {
      case NonFatal(e) => logger.error("Error reading serialized kryo bytes:", e); null
    }
  }

  private object IntReader extends (Input => Integer) {
    override def apply(input: Input): Integer = try {
      if (input.read() == NULL_BYTE) { null } else { input.readInt() }
    } catch {
      case NonFatal(e) => logger.error("Error reading serialized kryo bytes:", e); null
    }
  }

  private object LongReader extends (Input => java.lang.Long) {
    override def apply(input: Input): java.lang.Long = try {
      if (input.read() == NULL_BYTE) { null } else { input.readLong() }
    } catch {
      case NonFatal(e) => logger.error("Error reading serialized kryo bytes:", e); null
    }
  }

  private object FloatReader extends (Input => java.lang.Float) {
    override def apply(input: Input): java.lang.Float = try {
      if (input.read() == NULL_BYTE) { null } else { input.readFloat() }
    } catch {
      case NonFatal(e) => logger.error("Error reading serialized kryo bytes:", e); null
    }
  }

  private object DoubleReader extends (Input => java.lang.Double) {
    override def apply(input: Input): java.lang.Double = try {
      if (input.read() == NULL_BYTE) { null } else { input.readDouble() }
    } catch {
      case NonFatal(e) => logger.error("Error reading serialized kryo bytes:", e); null
    }
  }

  private object BooleanReader extends (Input => java.lang.Boolean) {
    override def apply(input: Input): java.lang.Boolean = try {
      if (input.read() == NULL_BYTE) { null } else { input.readBoolean() }
    } catch {
      case NonFatal(e) => logger.error("Error reading serialized kryo bytes:", e); null
    }
  }

  private object DateReader extends (Input => Date) {
    override def apply(input: Input): Date = try {
      if (input.read() == NULL_BYTE) { null } else { new Date(input.readLong()) }
    } catch {
      case NonFatal(e) => logger.error("Error reading serialized kryo bytes", e); null
    }
  }

  private object UuidReader extends (Input => UUID) {
    override def apply(input: Input): UUID = try {
      if (input.read() == NULL_BYTE) { null } else { new UUID(input.readLong(), input.readLong()) }
    } catch {
      case NonFatal(e) => logger.error("Error reading serialized kryo bytes:", e); null
    }
  }

  private object BytesReader extends (Input => Array[Byte]) {
    override def apply(input: Input): Array[Byte] = try {
      val size = input.readInt(true)
      if (size == -1) { null } else {
        val array = new Array[Byte](size)
        input.read(array)
        array
      }
    } catch {
      case NonFatal(e) => logger.error("Error reading serialized kryo bytes:", e); null
    }
  }

  private class ListReader(valueReader: Input => AnyRef) extends (Input => java.util.List[AnyRef]) {
    override def apply(input: Input): util.List[AnyRef] = try {
      val size = input.readInt(true)
      if (size == -1) { null } else {
        val list = new java.util.ArrayList[AnyRef](size)
        var index = 0
        while (index < size) {
          list.add(valueReader.apply(input))
          index += 1
        }
        list
      }
    } catch {
      case NonFatal(e) => logger.error("Error reading serialized kryo bytes:", e); null
    }
  }

  private class MapReader(keyReader: Input => AnyRef, valueReader: Input => AnyRef)
      extends (Input => java.util.Map[AnyRef, AnyRef]) {
    override def apply(input: Input): util.Map[AnyRef, AnyRef] = try {
      val size = input.readInt(true)
      if (size == -1) { null } else {
        val map = new java.util.HashMap[AnyRef, AnyRef](size)
        var index = 0
        while (index < size) {
          map.put(keyReader.apply(input), valueReader.apply(input))
          index += 1
        }
        map
      }
    } catch {
      case NonFatal(e) => logger.error("Error reading serialized kryo bytes:", e); null
    }
  }
}
