/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.kryo
package impl

import java.util
import java.util.{Date, UUID}

import com.esotericsoftware.kryo.io.Input
import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer.NullByte
import org.locationtech.geomesa.features.kryo.json.KryoJsonSerialization
import org.locationtech.geomesa.features.kryo.serialization.KryoGeometrySerialization
import org.locationtech.geomesa.features.serialization.ObjectType
import org.locationtech.geomesa.features.serialization.ObjectType.ObjectType
import org.locationtech.geomesa.utils.cache.ThreadLocalCache
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.SimpleFeatureType

import scala.util.control.NonFatal

/**
  * Version 2 serialization scheme consists of:
  *
  * <ol>
  *   <li>one byte containing the serialization version (2)</li>
  *   <li>four byte int containing the offset to the per-attribute offsets</li>
  *   <li>the feature id as a string (if `withId`)</li>
  *   <li>the serialized attributes, in order, each prepended with a byte 0 or 1 to indicate null/not null</li>
  *   <li>a variable length int (1-4 bytes) per attribute, for the offset to the start of the attribute in the serialized bytes</li>
  *   <li>the serialized user data (if `withUserData`)</li>
  * </ol>
  */
object KryoFeatureDeserializationV2 extends LazyLogging {

  private val readers = new ThreadLocalCache[String, Array[Input => AnyRef]](SerializerCacheExpiry)

  private [geomesa] def getReaders(key: String, sft: SimpleFeatureType): Array[Input => AnyRef] = {
    readers.getOrElseUpdate(key, sft.getAttributeDescriptors.toArray.map {
      case ad: AttributeDescriptor => matchReader(ObjectType.selectType(ad))
    })
  }

  private [geomesa] def matchReader(bindings: Seq[ObjectType]): Input => AnyRef = {
    bindings.head match {
      case ObjectType.STRING   => if (bindings.last == ObjectType.JSON) KryoJsonSerialization.deserializeAndRender else StringReader
      case ObjectType.INT      => IntReader
      case ObjectType.LONG     => LongReader
      case ObjectType.FLOAT    => FloatReader
      case ObjectType.DOUBLE   => DoubleReader
      case ObjectType.DATE     => DateReader
      case ObjectType.UUID     => UuidReader
      case ObjectType.GEOMETRY => KryoGeometrySerialization.deserialize
      case ObjectType.BYTES    => BytesReader
      case ObjectType.BOOLEAN  => BooleanReader
      case ObjectType.LIST     => new ListReader(matchReader(bindings.drop(1)))
      case ObjectType.MAP      => new MapReader(matchReader(bindings.slice(1, 2)), matchReader(bindings.drop(2)))
    }
  }

  object StringReader extends (Input => String) {
    override def apply(input: Input): String = try { input.readString() } catch {
      case NonFatal(e) => logger.error("Error reading serialized kryo bytes:", e); null
    }
  }

  object IntReader extends (Input => Integer) {
    override def apply(input: Input): Integer = try {
      if (input.read() == NullByte) { null } else { input.readInt() }
    } catch {
      case NonFatal(e) => logger.error("Error reading serialized kryo bytes:", e); null
    }
  }

  object LongReader extends (Input => java.lang.Long) {
    override def apply(input: Input): java.lang.Long = try {
      if (input.read() == NullByte) { null } else { input.readLong() }
    } catch {
      case NonFatal(e) => logger.error("Error reading serialized kryo bytes:", e); null
    }
  }

  object FloatReader extends (Input => java.lang.Float) {
    override def apply(input: Input): java.lang.Float = try {
      if (input.read() == NullByte) { null } else { input.readFloat() }
    } catch {
      case NonFatal(e) => logger.error("Error reading serialized kryo bytes:", e); null
    }
  }

  object DoubleReader extends (Input => java.lang.Double) {
    override def apply(input: Input): java.lang.Double = try {
      if (input.read() == NullByte) { null } else { input.readDouble() }
    } catch {
      case NonFatal(e) => logger.error("Error reading serialized kryo bytes:", e); null
    }
  }

  object BooleanReader extends (Input => java.lang.Boolean) {
    override def apply(input: Input): java.lang.Boolean = try {
      if (input.read() == NullByte) { null } else { input.readBoolean() }
    } catch {
      case NonFatal(e) => logger.error("Error reading serialized kryo bytes:", e); null
    }
  }

  object DateReader extends (Input => Date) {
    override def apply(input: Input): Date = try {
      if (input.read() == NullByte) { null } else { new Date(input.readLong()) }
    } catch {
      case NonFatal(e) => logger.error("Error reading serialized kryo bytes", e); null
    }
  }

  object UuidReader extends (Input => UUID) {
    override def apply(input: Input): UUID = try {
      if (input.read() == NullByte) { null } else { new UUID(input.readLong(), input.readLong()) }
    } catch {
      case NonFatal(e) => logger.error("Error reading serialized kryo bytes:", e); null
    }
  }

  object BytesReader extends (Input => Array[Byte]) {
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

  class ListReader(valueReader: Input => AnyRef) extends (Input => java.util.List[AnyRef]) {
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

  class MapReader(keyReader: Input => AnyRef, valueReader: Input => AnyRef)
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
