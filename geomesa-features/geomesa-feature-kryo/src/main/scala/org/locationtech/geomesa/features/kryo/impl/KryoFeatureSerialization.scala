/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.kryo
package impl

import java.io.OutputStream
import java.util.{Date, UUID}

import com.esotericsoftware.kryo.io.Output
import org.locationtech.geomesa.features.SimpleFeatureSerializer
import org.locationtech.geomesa.features.kryo.json.KryoJsonSerialization
import org.locationtech.geomesa.features.kryo.serialization.{KryoGeometrySerialization, KryoUserDataSerialization}
import org.locationtech.geomesa.features.serialization.ObjectType
import org.locationtech.geomesa.features.serialization.ObjectType.ObjectType
import org.locationtech.geomesa.utils.cache.{CacheKeyGenerator, SoftThreadLocal, SoftThreadLocalCache}
import org.locationtech.geomesa.utils.collection.IntBitSet
import org.locationtech.geomesa.utils.geometry.GeometryPrecision
import org.locationtech.jts.geom.Geometry
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

trait KryoFeatureSerialization extends SimpleFeatureSerializer {

  protected [kryo] def in: SimpleFeatureType

  private val writers = KryoFeatureSerialization.getWriters(CacheKeyGenerator.cacheKey(in), in)

  private val withId = !options.withoutId
  private val withUserData = options.withUserData

  private val count = in.getAttributeCount

  override def serialize(sf: SimpleFeature): Array[Byte] = {
    val output = KryoFeatureSerialization.getOutput(null)
    writeFeature(sf, output)
    output.toBytes
  }

  override def serialize(sf: SimpleFeature, out: OutputStream): Unit = {
    val output = KryoFeatureSerialization.getOutput(out)
    writeFeature(sf, output)
    output.flush()
  }

  private def writeFeature(sf: SimpleFeature, output: Output): Unit = {
    output.writeByte(KryoFeatureSerializer.Version3)
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
      val attribute = sf.getAttribute(i)
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

object KryoFeatureSerialization {

  import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

  import scala.collection.JavaConverters._

  private [this] val outputs = new SoftThreadLocal[Output]()
  private [this] val writers = new SoftThreadLocalCache[String, Array[KryoAttributeWriter]]()

  /**
    * Gets a reusable, thread-local output. Don't hold on to the result, as it may be re-used on a per-thread basis
    *
    * @param stream output stream, may be null
    * @return
    */
  def getOutput(stream: OutputStream): Output = {
    val out = outputs.getOrElseUpdate(new Output(1024, -1))
    out.setOutputStream(stream)
    out
  }

  /**
    * Get attribute writes for a feature type
    *
    * @param key cache key for the feature type
    * @param sft simple feature type
    * @return
    */
  def getWriters(key: String, sft: SimpleFeatureType): Array[KryoAttributeWriter] =
    writers.getOrElseUpdate(key, sft.getAttributeDescriptors.asScala.map(writer).toArray)

  private [geomesa] def writer(descriptor: AttributeDescriptor): KryoAttributeWriter =
    writer(ObjectType.selectType(descriptor), descriptor)

  private [geomesa] def writer(bindings: Seq[ObjectType], ad: AttributeDescriptor): KryoAttributeWriter = {
    bindings.head match {
      case ObjectType.STRING   => if (bindings.last == ObjectType.JSON) { KryoJsonWriter } else { KryoStringWriter }
      case ObjectType.INT      => KryoIntWriter
      case ObjectType.LONG     => KryoLongWriter
      case ObjectType.FLOAT    => KryoFloatWriter
      case ObjectType.DOUBLE   => KryoDoubleWriter
      case ObjectType.DATE     => KryoDateWriter
      case ObjectType.GEOMETRY =>
        ad.getPrecision match {
          case GeometryPrecision.FullPrecision => KryoGeometryWkbWriter
          case precision: GeometryPrecision.TwkbPrecision => KryoGeometryTwkbWriter(precision)
        }
      case ObjectType.UUID     => KryoUuidWriter
      case ObjectType.BYTES    => KryoBytesWriter
      case ObjectType.BOOLEAN  => KryoBooleanWriter
      case ObjectType.LIST     => KryoListWriter(writer(bindings.drop(1), ad))
      case ObjectType.MAP      => KryoMapWriter(writer(bindings.slice(1, 2), ad), writer(bindings.drop(2), ad))

      case b => throw new NotImplementedError(s"Unexpected attribute type binding: $b")
    }
  }

  sealed trait KryoAttributeWriter {
    def apply(output: Output, value: AnyRef): Unit
  }

  case object KryoStringWriter extends KryoAttributeWriter {
    override def apply(output: Output, value: AnyRef): Unit = output.writeString(value.asInstanceOf[String])
  }

  case object KryoIntWriter extends KryoAttributeWriter {
    override def apply(output: Output, value: AnyRef): Unit = output.writeInt(value.asInstanceOf[Int])
  }

  case object KryoLongWriter extends KryoAttributeWriter {
    override def apply(output: Output, value: AnyRef): Unit = output.writeLong(value.asInstanceOf[Long])
  }

  case object KryoFloatWriter extends KryoAttributeWriter {
    override def apply(output: Output, value: AnyRef): Unit = output.writeFloat(value.asInstanceOf[Float])
  }

  case object KryoDoubleWriter extends KryoAttributeWriter {
    override def apply(output: Output, value: AnyRef): Unit = output.writeDouble(value.asInstanceOf[Double])
  }

  case object KryoBooleanWriter extends KryoAttributeWriter {
    override def apply(output: Output, value: AnyRef): Unit = output.writeBoolean(value.asInstanceOf[Boolean])
  }

  case object KryoDateWriter extends KryoAttributeWriter {
    override def apply(output: Output, value: AnyRef): Unit = output.writeLong(value.asInstanceOf[Date].getTime)
  }

  case object KryoUuidWriter extends KryoAttributeWriter {
    override def apply(output: Output, value: AnyRef): Unit = {
      val uuid = value.asInstanceOf[UUID]
      output.writeLong(uuid.getMostSignificantBits)
      output.writeLong(uuid.getLeastSignificantBits)
    }
  }

  case object KryoJsonWriter extends KryoAttributeWriter {
    override def apply(output: Output, value: AnyRef): Unit =
      KryoJsonSerialization.serialize(output, value.asInstanceOf[String])
  }

  case object KryoBytesWriter extends KryoAttributeWriter {
    override def apply(output: Output, value: AnyRef): Unit = {
      val array = value.asInstanceOf[Array[Byte]]
      output.writeInt(array.length, true)
      output.writeBytes(array)
    }
  }

  case object KryoGeometryWkbWriter extends KryoAttributeWriter {
    override def apply(output: Output, value: AnyRef): Unit =
      KryoGeometrySerialization.serializeWkb(output, value.asInstanceOf[Geometry])
  }

  case class KryoGeometryTwkbWriter(precision: GeometryPrecision.TwkbPrecision) extends KryoAttributeWriter {
    override def apply(output: Output, value: AnyRef): Unit =
      KryoGeometrySerialization.serialize(output, value.asInstanceOf[Geometry], precision)
  }

  case class KryoListWriter(elements: KryoAttributeWriter) extends KryoAttributeWriter {
    // TODO handle null elements?
    override def apply(output: Output, value: AnyRef): Unit = {
      val list = value.asInstanceOf[java.util.List[AnyRef]]
      output.writeInt(list.size(), true)
      val iter = list.iterator()
      while (iter.hasNext) {
        elements(output, iter.next())
      }
    }
  }

  case class KryoMapWriter(keys: KryoAttributeWriter, values: KryoAttributeWriter) extends KryoAttributeWriter {
    // TODO handle null keys/values?
    override def apply(output: Output, value: AnyRef): Unit = {
      val map = value.asInstanceOf[java.util.Map[AnyRef, AnyRef]]
      output.writeInt(map.size(), true)
      val iter = map.entrySet.iterator()
      while (iter.hasNext) {
        val entry = iter.next()
        keys(output, entry.getKey)
        values(output, entry.getValue)
      }
    }
  }
}