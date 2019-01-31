/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.kryo.impl

import java.io.OutputStream
import java.util.{Date, UUID}

import com.esotericsoftware.kryo.io.Output
import org.locationtech.jts.geom.Geometry
import org.locationtech.geomesa.features.SimpleFeatureSerializer
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer.{NON_NULL_BYTE, NULL_BYTE, VERSION}
import org.locationtech.geomesa.features.kryo.json.KryoJsonSerialization
import org.locationtech.geomesa.features.kryo.serialization.{KryoGeometrySerialization, KryoUserDataSerialization}
import org.locationtech.geomesa.features.serialization.ObjectType
import org.locationtech.geomesa.features.serialization.ObjectType.ObjectType
import org.locationtech.geomesa.utils.cache.{CacheKeyGenerator, SoftThreadLocal, SoftThreadLocalCache}
import org.locationtech.geomesa.utils.geometry.GeometryPrecision
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

trait KryoFeatureSerialization extends SimpleFeatureSerializer {

  private [kryo] def serializeSft: SimpleFeatureType

  private val cacheKey = CacheKeyGenerator.cacheKey(serializeSft)
  private val writers = KryoFeatureSerialization.getWriters(cacheKey, serializeSft)

  private val withId = !options.withoutId
  private val withUserData = options.withUserData

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
    val offsets = KryoFeatureSerialization.getOffsets(cacheKey, writers.length)
    val offset = output.position()
    output.writeInt(VERSION, true)
    output.setPosition(offset + 5) // leave 4 bytes to write the offsets
    if (withId) {
      // TODO optimize for uuids?
      output.writeString(sf.getID)
    }
    // write attributes and keep track off offset into byte array
    var i = 0
    while (i < writers.length) {
      offsets(i) = output.position() - offset
      writers(i)(output, sf.getAttribute(i))
      i += 1
    }
    // write the offsets - variable width
    i = 0
    val offsetStart = output.position() - offset
    while (i < writers.length) {
      output.writeInt(offsets(i), true)
      i += 1
    }
    // got back and write the start position for the offsets
    val end = output.position()
    output.setPosition(offset + 1)
    output.writeInt(offsetStart)
    // reset the position back to the end of the buffer so the bytes aren't lost, and we can keep writing user data
    output.setPosition(end)

    if (withUserData) {
      KryoUserDataSerialization.serialize(output, sf.getUserData)
    }
  }
}

object KryoFeatureSerialization {

  private [this] val outputs = new SoftThreadLocal[Output]()
  private [this] val writers = new SoftThreadLocalCache[String, Array[(Output, AnyRef) => Unit]]()
  private [this] val offsets = new SoftThreadLocalCache[String, Array[Int]]()

  def getOutput(stream: OutputStream): Output = {
    val out = outputs.getOrElseUpdate(new Output(1024, -1))
    out.setOutputStream(stream)
    out
  }

  private [kryo] def getOffsets(sft: String, size: Int): Array[Int] =
    offsets.getOrElseUpdate(sft, Array.ofDim[Int](size))

  private [geomesa] def getWriters(key: String, sft: SimpleFeatureType): Array[(Output, AnyRef) => Unit] = {
    import scala.collection.JavaConversions._
    writers.getOrElseUpdate(key,
      sft.getAttributeDescriptors.map(ad => matchWriter(ObjectType.selectType(ad), ad)).toArray)
  }

  private [geomesa] def matchWriter(bindings: Seq[ObjectType], descriptor: AttributeDescriptor): (Output, AnyRef) => Unit = {
    import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor
    bindings.head match {
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
        // null checks are handled by geometry serializer
        descriptor.getPrecision match {
          case GeometryPrecision.FullPrecision =>
            (o: Output, v: AnyRef) => KryoGeometrySerialization.serializeWkb(o, v.asInstanceOf[Geometry])
          case precision: GeometryPrecision.TwkbPrecision =>
            (o: Output, v: AnyRef) => KryoGeometrySerialization.serialize(o, v.asInstanceOf[Geometry], precision)
        }
      case ObjectType.JSON =>
        (o: Output, v: AnyRef) => KryoJsonSerialization.serialize(o, v.asInstanceOf[String])
      case ObjectType.LIST =>
        val valueWriter = matchWriter(bindings.drop(1), descriptor)
        (o: Output, v: AnyRef) => {
          val list = v.asInstanceOf[java.util.List[AnyRef]]
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
        val keyWriter = matchWriter(bindings.slice(1, 2), descriptor)
        val valueWriter = matchWriter(bindings.drop(2), descriptor)
        (o: Output, v: AnyRef) => {
          val map = v.asInstanceOf[java.util.Map[AnyRef, AnyRef]]
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

  private def writeNullable(wrapped: (Output, AnyRef) => Unit): (Output, AnyRef) => Unit = {
    (o: Output, v: AnyRef) => {
      if (v == null) {
        o.write(NULL_BYTE)
      } else {
        o.write(NON_NULL_BYTE)
        wrapped(o, v)
      }
    }
  }
}