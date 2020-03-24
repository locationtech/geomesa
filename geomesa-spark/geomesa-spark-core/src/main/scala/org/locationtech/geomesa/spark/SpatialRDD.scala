/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark

import java.io.{BufferedWriter, StringWriter}

import org.apache.spark.geomesa.GeoMesaSparkKryoRegistratorEndpoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}
import org.locationtech.geomesa.features.serialization.GeoJsonSerializer
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
  * RDD with a known schema
  *
  * @param rdd simple feature RDD
  * @param sft simple feature type schema
  */
class SpatialRDD(rdd: RDD[SimpleFeature], sft: SimpleFeatureType) extends RDD[SimpleFeature](rdd) with Schema {

  GeoMesaSparkKryoRegistrator.register(sft)

  private val typeName = sft.getTypeName
  private val spec = SimpleFeatureTypes.encodeType(sft, includeUserData = true)

  @transient
  override lazy val schema: SimpleFeatureType = SimpleFeatureTypes.createType(typeName, spec)

  override def compute(split: Partition, context: TaskContext): Iterator[SimpleFeature] =
    firstParent.compute(split, context)

  override def getPartitions: Array[Partition] = firstParent.partitions
}

object SpatialRDD {

  import scala.collection.JavaConverters._

  GeoMesaSparkKryoRegistratorEndpoint.init()

  def apply(rdd: RDD[SimpleFeature], schema: SimpleFeatureType) = new SpatialRDD(rdd, schema)

  implicit def toValueSeq(in: RDD[SimpleFeature] with Schema): RDD[Seq[AnyRef]] =
    in.map(_.getAttributes.asScala)

  implicit def toKeyValueSeq(in: RDD[SimpleFeature] with Schema): RDD[Seq[(String, AnyRef)]] =
    in.map(_.getProperties.asScala.map(p => (p.getName.getLocalPart, p.getValue)).toSeq)

  implicit def toKeyValueMap(in: RDD[SimpleFeature] with Schema): RDD[Map[String, AnyRef]] =
    in.map(_.getProperties.asScala.map(p => (p.getName.getLocalPart, p.getValue)).toMap)

  implicit def toGeoJSONString(in: RDD[SimpleFeature] with Schema): RDD[String] = {
    val sft = in.schema
    in.mapPartitions { features =>
      val json = new GeoJsonSerializer(sft)
      val sw = new StringWriter
      // note: we don't need to close this since we're writing to a string
      val jw = GeoJsonSerializer.writer(sw)
      features.map { f =>
        sw.getBuffer.setLength(0)
        json.write(jw, f)
        jw.flush()
        sw.toString
      }
    }
  }

  implicit class SpatialRDDConversions(in: RDD[SimpleFeature] with Schema) {
    def asGeoJSONString: RDD[String] = toGeoJSONString(in)
    def asKeyValueMap: RDD[Map[String, AnyRef]] = toKeyValueMap(in)
    def asKeyValueSeq: RDD[Seq[(String, AnyRef)]] = toKeyValueSeq(in)
    def asValueSeq: RDD[Seq[AnyRef]] = toValueSeq(in)
  }
}
