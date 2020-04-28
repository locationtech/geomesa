/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.kryo
package impl

import java.io.InputStream
import java.util.{Date, UUID}

import com.esotericsoftware.kryo.io.Input
import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.features.SimpleFeatureSerializer
import org.locationtech.geomesa.features.kryo.impl.KryoFeatureDeserialization.KryoAttributeReader
import org.locationtech.geomesa.features.kryo.json.KryoJsonSerialization
import org.locationtech.geomesa.features.kryo.serialization.KryoGeometrySerialization
import org.locationtech.geomesa.features.serialization.ObjectType
import org.locationtech.geomesa.features.serialization.ObjectType.ObjectType
import org.locationtech.geomesa.utils.cache.{CacheKeyGenerator, SoftThreadLocal, ThreadLocalCache}
import org.locationtech.geomesa.utils.kryo.NonMutatingInput
import org.locationtech.jts.geom.Geometry
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.SimpleFeatureType

import scala.util.control.NonFatal

/**
  * Base trait for kryo deserialization
  */
trait KryoFeatureDeserialization extends SimpleFeatureSerializer with LazyLogging {

  private val key = CacheKeyGenerator.cacheKey(out)

  protected [kryo] val withoutUserData: Boolean = !options.withUserData
  protected [kryo] val withoutId: Boolean = options.withoutId

  protected [kryo] val readers: Array[KryoAttributeReader] = KryoFeatureDeserialization.getReaders(key, out)
  protected [kryo] lazy val readersV2: Array[Input => AnyRef] = KryoFeatureDeserializationV2.getReaders(key, out)

  protected [kryo] def out: SimpleFeatureType

  def getReusableFeature: KryoBufferSimpleFeature = new KryoBufferSimpleFeature(this)
}

object KryoFeatureDeserialization extends LazyLogging {

  private val inputBytes   = new SoftThreadLocal[Input]()
  private val inputStreams = new SoftThreadLocal[Input]()
  private val readers      = new ThreadLocalCache[String, Array[KryoAttributeReader]](SerializerCacheExpiry)

  def getInput(bytes: Array[Byte], offset: Int, count: Int): Input = {
    val in = inputBytes.getOrElseUpdate(new NonMutatingInput())
    in.setBuffer(bytes, offset, count)
    in
  }

  def getInput(stream: InputStream): Input = {
    val in = inputStreams.getOrElseUpdate(new NonMutatingInput(Array.ofDim(1024)))
    in.setInputStream(stream)
    in
  }

  def getReaders(key: String, sft: SimpleFeatureType): Array[KryoAttributeReader] = {
    readers.getOrElseUpdate(key, sft.getAttributeDescriptors.toArray.map {
      case ad: AttributeDescriptor => reader(ObjectType.selectType(ad))
    })
  }

  private [geomesa] def reader(bindings: Seq[ObjectType]): KryoAttributeReader = {
    bindings.head match {
      case ObjectType.STRING   => if (bindings.last == ObjectType.JSON) { KryoJsonReader } else { KryoStringReader }
      case ObjectType.INT      => KryoIntReader
      case ObjectType.LONG     => KryoLongReader
      case ObjectType.FLOAT    => KryoFloatReader
      case ObjectType.DOUBLE   => KryoDoubleReader
      case ObjectType.DATE     => KryoDateReader
      case ObjectType.GEOMETRY => KryoGeometryReader
      case ObjectType.BYTES    => KryoBytesReader
      case ObjectType.UUID     => KryoUuidReader
      case ObjectType.BOOLEAN  => KryoBooleanReader
      case ObjectType.LIST     => KryoListReader(reader(bindings.drop(1)))
      case ObjectType.MAP      => KryoMapReader(reader(bindings.slice(1, 2)), reader(bindings.drop(2)))

      case b => throw new NotImplementedError(s"Unexpected attribute type binding: $b")
    }
  }

  sealed trait KryoAttributeReader {
    def apply(input: Input): AnyRef
  }

  case object KryoStringReader extends KryoAttributeReader {
    override def apply(input: Input): String = try { input.readString() } catch {
      case NonFatal(e) => logger.error("Error reading serialized kryo bytes:", e); null
    }
  }

  case object KryoIntReader extends KryoAttributeReader {
    override def apply(input: Input): Integer = try { input.readInt() } catch {
      case NonFatal(e) => logger.error("Error reading serialized kryo bytes:", e); null
    }
  }

  case object KryoLongReader extends KryoAttributeReader {
    override def apply(input: Input): java.lang.Long = try { input.readLong() } catch {
      case NonFatal(e) => logger.error("Error reading serialized kryo bytes:", e); null
    }
  }

  case object KryoFloatReader extends KryoAttributeReader {
    override def apply(input: Input): java.lang.Float = try { input.readFloat() } catch {
      case NonFatal(e) => logger.error("Error reading serialized kryo bytes:", e); null
    }
  }

  case object KryoDoubleReader extends KryoAttributeReader {
    override def apply(input: Input): java.lang.Double = try { input.readDouble() } catch {
      case NonFatal(e) => logger.error("Error reading serialized kryo bytes:", e); null
    }
  }

  case object KryoBooleanReader extends KryoAttributeReader {
    override def apply(input: Input): java.lang.Boolean = try { input.readBoolean() } catch {
      case NonFatal(e) => logger.error("Error reading serialized kryo bytes:", e); null
    }
  }

  case object KryoDateReader extends KryoAttributeReader {
    override def apply(input: Input): Date = try { new Date(input.readLong()) } catch {
      case NonFatal(e) => logger.error("Error reading serialized kryo bytes", e); null
    }
  }

  case object KryoUuidReader extends KryoAttributeReader {
    override def apply(input: Input): UUID = try { new UUID(input.readLong(), input.readLong()) } catch {
      case NonFatal(e) => logger.error("Error reading serialized kryo bytes:", e); null
    }
  }

  case object KryoBytesReader extends KryoAttributeReader {
    override def apply(input: Input): Array[Byte] = try {
      val array = new Array[Byte](input.readInt(true))
      input.read(array)
      array
    } catch {
      case NonFatal(e) => logger.error("Error reading serialized kryo bytes:", e); null
    }
  }

  case object KryoJsonReader extends KryoAttributeReader {
    override def apply(input: Input): String = KryoJsonSerialization.deserializeAndRender(input)
  }

  case object KryoGeometryReader extends KryoAttributeReader {
    override def apply(input: Input): Geometry = KryoGeometrySerialization.deserialize(input)
  }

  case class KryoListReader(elements: KryoAttributeReader) extends KryoAttributeReader {
    override def apply(input: Input): java.util.List[AnyRef] = try {
      val size = input.readInt(true)
      val list = new java.util.ArrayList[AnyRef](size)
      var index = 0
      while (index < size) {
        list.add(elements.apply(input))
        index += 1
      }
      list
    } catch {
      case NonFatal(e) => logger.error("Error reading serialized kryo bytes:", e); null
    }
  }

  case class KryoMapReader(keys: KryoAttributeReader, values: KryoAttributeReader) extends KryoAttributeReader {
    override def apply(input: Input): java.util.Map[AnyRef, AnyRef] = try {
      val size = input.readInt(true)
      val map = new java.util.HashMap[AnyRef, AnyRef](size)
      var index = 0
      while (index < size) {
        map.put(keys.apply(input), values.apply(input))
        index += 1
      }
      map
    } catch {
      case NonFatal(e) => logger.error("Error reading serialized kryo bytes:", e); null
    }
  }
}
