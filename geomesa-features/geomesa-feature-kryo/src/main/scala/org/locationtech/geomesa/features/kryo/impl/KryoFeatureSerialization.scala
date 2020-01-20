/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
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
import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.features.SimpleFeatureSerializer
import org.locationtech.geomesa.features.kryo.json.KryoJsonSerialization
import org.locationtech.geomesa.features.kryo.serialization.{KryoGeometrySerialization, KryoUserDataSerialization}
import org.locationtech.geomesa.features.serialization.ObjectType
import org.locationtech.geomesa.features.serialization.ObjectType.ObjectType
import org.locationtech.geomesa.utils.cache.{CacheKeyGenerator, SoftThreadLocal, ThreadLocalCache}
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
    // note: don't write directly to the output stream, we need to be able to reposition in the output buffer,
    // and if we use the output stream it might be flushed and reset during our write
    val output = KryoFeatureSerialization.getOutput(null)
    writeFeature(sf, output)
    out.write(output.getBuffer, 0, output.position())
  }

  private def writeFeature(sf: SimpleFeature, output: Output): Unit = {
    output.writeByte(KryoFeatureSerializer.Version3)
    val offset = Metadata.write(output, count, 2)
    if (withId) {
      output.writeString(sf.getID) // TODO optimize for uuids?
    }
    // write attributes and keep track of offset into the byte array
    val offsets = Array.ofDim[Int](count + 1)
    val nulls = IntBitSet(count)
    var i = 0
    while (i < count) {
      offsets(i) = output.position() - offset
      val attribute = sf.getAttribute(i)
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
        System.arraycopy(output.getBuffer, 0, expanded, 0, offset)
        System.arraycopy(output.getBuffer, offset, expanded, offset + shift, end - offset)
        output.setBuffer(expanded)
      } else {
        val buffer = output.getBuffer
        var i = end
        while (i > offset) {
          buffer(i + shift) = buffer(i)
          i -= 1
        }
      }
      // go back and write the offsets and nulls
      output.setPosition(offset - 1)
      output.write(4) // 4 bytes per offset
      offsets.foreach(o => output.writeInt(o + shift))
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

object KryoFeatureSerialization extends LazyLogging {

  import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

  import scala.collection.JavaConverters._

  val MaxUnsignedShort: Int = 65535

  private [this] val outputs = new SoftThreadLocal[Output]()
  private [this] val writers = new ThreadLocalCache[String, Array[KryoAttributeWriter]](SerializerCacheExpiry)

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
    override def apply(output: Output, value: AnyRef): Unit = {
      val list = value.asInstanceOf[java.util.List[AnyRef]]
      val pos = output.position()
      var size = list.size()
      output.writeInt(size, true)
      val start = output.position()
      val iter = list.iterator()
      while (iter.hasNext) {
        val next = iter.next()
        if (next == null) {
          size -= 1
        } else {
          elements(output, next)
        }
      }
      if (size != list.size()) {
        logger.warn(s"Dropping ${list.size() - size} null elements from serialized list")
        resize(output, size, pos, start)
      }
    }
  }

  case class KryoMapWriter(keys: KryoAttributeWriter, values: KryoAttributeWriter) extends KryoAttributeWriter {
    override def apply(output: Output, value: AnyRef): Unit = {
      val map = value.asInstanceOf[java.util.Map[AnyRef, AnyRef]]
      val pos = output.position()
      var size = map.size()
      output.writeInt(size, true)
      val start = output.position()
      val iter = map.entrySet.iterator()
      while (iter.hasNext) {
        val entry = iter.next()
        if (entry.getKey == null || entry.getValue == null) {
          size -= 1
        } else {
          keys(output, entry.getKey)
          values(output, entry.getValue)
        }
      }
      if (size != map.size()) {
        logger.warn(s"Dropping ${map.size() - size} entries with null keys or values from serialized map")
        resize(output, size, pos, start)
      }
    }
  }

  private def resize(output: Output, size: Int, position: Int, start: Int): Unit = {
    val end = output.position()
    output.setPosition(position)
    output.writeInt(size, true)
    val shift = start - output.position()
    if (shift != 0) {
      // the var-int encoding resulting in fewer bytes, we have to shift the result left
      val buf = output.getBuffer
      var i = output.position()
      while (i + shift < end) {
        buf(i) = buf(i + shift)
        i -= 1
      }
      output.setPosition(end - shift)
    }
  }
}