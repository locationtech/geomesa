/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.jobs.mapreduce

import java.io.{InputStream, OutputStream}

import org.apache.hadoop.conf.Configured
import org.apache.hadoop.io.serializer.{Deserializer, Serialization, Serializer}
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.jobs.GeoMesaConfigurator
import org.locationtech.geomesa.jobs.mapreduce.SimpleFeatureSerialization._
import org.locationtech.geomesa.utils.cache.ThreadLocalCache
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.index.ByteArrays
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
 * Hadoop writable serialization for simple features
 */
class SimpleFeatureSerialization extends Configured with Serialization[SimpleFeature] {

  lazy private val types = GeoMesaConfigurator.getSerialization(getConf)

  override def accept(c: Class[_]): Boolean = classOf[SimpleFeature].isAssignableFrom(c)

  override def getSerializer(c: Class[SimpleFeature]): Serializer[SimpleFeature] =
    new HadoopSimpleFeatureSerializer(types)

  override def getDeserializer(c: Class[SimpleFeature]): Deserializer[SimpleFeature] =
    new HadoopSimpleFeatureDeserializer(types)
}

object SimpleFeatureSerialization {

  import org.locationtech.geomesa.features.kryo.SerializerCacheExpiry

  // re-usable serializers since they are not thread safe
  private val serializers = new ThreadLocalCache[String, KryoFeatureSerializer](SerializerCacheExpiry)

  private def serializer(key: String, sft: SimpleFeatureType): KryoFeatureSerializer =
    serializers.getOrElseUpdate(key, KryoFeatureSerializer.builder(sft).withId.withUserData.build())

  private def readable(sft: SimpleFeatureType): String =
    s"${sft.getTypeName} identified ${SimpleFeatureTypes.encodeType(sft, includeUserData = true)}"

  /**
    * Serializer class that delegates to kryo serialization. Simple feature type must be configured
    * in the job, and is identified by a unique hash code
    *
    * @param types configured feature types
    */
  class HadoopSimpleFeatureSerializer(types: Seq[(String, Int, SimpleFeatureType)])
      extends Serializer[SimpleFeature] {

    private val hashBytes = Array.ofDim[Byte](4)
    private var out: OutputStream = _

    override def open(out: OutputStream): Unit = this.out = out

    override def close(): Unit = out.close()

    override def serialize(feature: SimpleFeature): Unit = {
      val (key, hash, sft) = matching(feature.getFeatureType)
      ByteArrays.writeInt(hash, hashBytes)
      out.write(hashBytes)
      serializer(key, sft).serialize(feature, out)
    }

    private def matching(sft: SimpleFeatureType): (String, Int, SimpleFeatureType) = {
      types.find(_._3 == sft).getOrElse {
        throw new IllegalStateException(s"Trying to serialize ${readable(sft)} but no matching " +
            s"configuration: ${types.map(t => readable(t._3)).mkString(", ")}")
      }
    }
  }

  /**
    * Deserializer class that delegates to kryo serialization. Simple feature type must be is configured
    * in the job, and is identified by a unique hash code
    *
    * @param types configured feature types
    */
  class HadoopSimpleFeatureDeserializer(types: Seq[(String, Int, SimpleFeatureType)])
      extends Deserializer[SimpleFeature] {

    private val hashBytes = Array.ofDim[Byte](4)
    private var in: InputStream = _

    override def open(in: InputStream): Unit = this.in = in

    override def close(): Unit = in.close()

    override def deserialize(ignored: SimpleFeature): SimpleFeature = {
      in.read(hashBytes)
      val hash = ByteArrays.readInt(hashBytes)
      val (key, sft) = matching(hash)
      serializer(key, sft).deserialize(in)
    }

    private def matching(hash: Int): (String, SimpleFeatureType) = {
      types.find(_._2 == hash).map(t => (t._1, t._3)).getOrElse {
        throw new IllegalStateException(s"Trying to deserialize $hash but no matching " +
            s"configuration: ${types.map(t => s"${t._2} :: ${readable(t._3)}").mkString(", ")}")
      }
    }
  }
}
