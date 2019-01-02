/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.jobs.mapreduce

import java.io.{InputStream, OutputStream}

import com.google.common.primitives.Ints
import org.apache.hadoop.io.serializer.{Deserializer, Serialization, Serializer}
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.jobs.mapreduce.SimpleFeatureSerialization._
import org.locationtech.geomesa.utils.cache.SoftThreadLocalCache
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeature

/**
 * Hadoop writable serialization for simple features
 */
class SimpleFeatureSerialization extends Serialization[SimpleFeature] {

  override def accept(c: Class[_]) = classOf[SimpleFeature].isAssignableFrom(c)

  override def getSerializer(c: Class[SimpleFeature]) = new HadoopSimpleFeatureSerializer

  override def getDeserializer(c: Class[SimpleFeature]) = new HadoopSimpleFeatureDeserializer
}

object SimpleFeatureSerialization {
  // re-usable serializers since they are not thread safe
  val serializers = new SoftThreadLocalCache[String, KryoFeatureSerializer]()

  /**
   * Writes a string to the output stream
   */
  def writeString(out: OutputStream, value: String): Unit = {
    val bytes = value.getBytes("UTF-8")
    val length = bytes.length
    // bit-shift to write the 4 bytes of the int
    out.write(length >> 24)
    out.write(length >> 16)
    out.write(length >> 8)
    out.write(length) // >> 0
    out.write(bytes)
  }

  /**
   * Read a string from the input stream
   */
  def readString(in: InputStream): String = {
    implicit def intToByte(i: Int): Byte = i.asInstanceOf[Byte]
    // have to re-construct the int from 4 bytes
    val bytes = Array.ofDim[Byte](Ints.fromBytes(in.read(), in.read(), in.read(), in.read()))
    in.read(bytes)
    new String(bytes, "UTF-8")
  }
}

/**
 * Serializer class that delegates to kryo serialization. We also have to encode the sft, however.
 * It would be nice if there was some way to avoid doing that, but it seems impossible to avoid.
 */
class HadoopSimpleFeatureSerializer extends Serializer[SimpleFeature] {

  var out: OutputStream = null

  override def open(out: OutputStream) = this.out = out

  override def close() = out.close()

  override def serialize(sf: SimpleFeature) = {
    val sft = sf.getFeatureType
    writeString(out, sft.getTypeName)
    val sftString = SimpleFeatureTypes.encodeType(sft)
    writeString(out, sftString)
    serializers.getOrElseUpdate(s"${sft.getTypeName}:$sftString", KryoFeatureSerializer(sft)).serialize(sf, out)
  }
}

/**
 * Deserializer class that delegates to kryo serialization, plus the sft.
 */
class HadoopSimpleFeatureDeserializer extends Deserializer[SimpleFeature] {

  var in: InputStream = null

  override def open(in: InputStream) = this.in = in

  override def close() = in.close()

  override def deserialize(ignored: SimpleFeature) = {
    val sftName = readString(in)
    val sftString = readString(in)
    lazy val sft = SimpleFeatureTypes.createType(sftName, sftString)
    serializers.getOrElseUpdate(s"${sft.getTypeName}:$sftString", KryoFeatureSerializer(sft)).deserialize(in)
  }
}
