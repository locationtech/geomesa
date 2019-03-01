/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark

import java.io.{BufferedWriter, StringWriter}
import java.util.ServiceLoader

import org.apache.hadoop.conf.Configuration
import org.apache.spark.geomesa.GeoMesaSparkKryoRegistratorEndpoint
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.geotools.data.Query
import org.geotools.geojson.feature.FeatureJSON
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._


object GeoMesaSpark {

  import scala.collection.JavaConversions._

  lazy val providers: ServiceLoader[SpatialRDDProvider] = ServiceLoader.load(classOf[SpatialRDDProvider])

  def apply(params: java.util.Map[String, java.io.Serializable]): SpatialRDDProvider =
    providers.find(_.canProcess(params)).getOrElse(throw new RuntimeException("Could not find a SpatialRDDProvider"))
}

trait SpatialRDDProvider {

  def canProcess(params: java.util.Map[String, java.io.Serializable]): Boolean

  def rdd(conf: Configuration, sc: SparkContext, params: Map[String, String], query: Query) : SpatialRDD

  /**
    * Writes this RDD to a GeoMesa table.
    * The type must exist in the data store, and all of the features in the RDD must be of this type.
    *
    * @param rdd
    * @param params
    * @param typeName
    */
  def save(rdd: RDD[SimpleFeature], params: Map[String, String], typeName: String): Unit

}

trait Schema {
  def schema: SimpleFeatureType
}

class SpatialRDD(rdd: RDD[SimpleFeature], sft: SimpleFeatureType) extends RDD[SimpleFeature](rdd) with Schema {

  GeoMesaSparkKryoRegistrator.register(sft)

  private val sft_name = sft.getTypeName
  private val sft_spec = SimpleFeatureTypes.encodeType(sft, true)

  @transient lazy val schema = SimpleFeatureTypes.createType(sft_name, sft_spec)

  def compute(split: Partition, context: TaskContext): Iterator[SimpleFeature] = firstParent.compute(split, context)
  def getPartitions: Array[Partition] = firstParent.partitions
}

object SpatialRDD {

  GeoMesaSparkKryoRegistratorEndpoint.init()

  def apply(rdd: RDD[SimpleFeature], schema: SimpleFeatureType) = new SpatialRDD(rdd, schema)

  implicit def toValueSeq(in: RDD[SimpleFeature] with Schema): RDD[Seq[AnyRef]] =
    in.map(sf => Seq(sf.getAttributes: _*))

  implicit def toKeyValueSeq(in: RDD[SimpleFeature] with Schema): RDD[Seq[(String, AnyRef)]] =
    in.map(_.getProperties.map(p => (p.getName.getLocalPart, p.getValue)).toSeq)

  implicit def toKeyValueMap(in: RDD[SimpleFeature] with Schema): RDD[Map[String, AnyRef]] =
    in.map(_.getProperties.map(p => (p.getName.getLocalPart, p.getValue)).toMap)

  implicit def toGeoJSONString(in: RDD[SimpleFeature] with Schema): RDD[String] = {
    in.mapPartitions(features => {
      val json = new FeatureJSON
      val sw = new StringWriter
      val bw = new BufferedWriter(sw)
      features.map(f => try {
        json.writeFeature(f, bw); sw.toString
      } finally {
        sw.getBuffer.setLength(0)
      })
    })
  }

  implicit class SpatialRDDConversions(in: RDD[SimpleFeature] with Schema) {
    def asGeoJSONString = toGeoJSONString(in)
    def asKeyValueMap = toKeyValueMap(in)
    def asKeyValueSeq = toKeyValueSeq(in)
    def asValueSeq = toValueSeq(in)
  }
}

// Resolve issue with wrapped instance of org.apache.spark.sql.execution.datasources.CaseInsensitiveMap in Scala 2.10
object CaseInsensitiveMapFix {
  import scala.collection.convert.Wrappers._

  trait MapWrapperFix[A,B] {
    this: MapWrapper[A,B] =>
    override def containsKey(key: AnyRef): Boolean = try {
      get(key) != null
    } catch {
      case ex: ClassCastException => false
    }
  }

  implicit def mapAsJavaMap[A <: String, B](m: scala.collection.Map[A, B]): java.util.Map[A, B] = m match {
    case JMapWrapper(wrapped) => wrapped.asInstanceOf[java.util.Map[A, B]]
    case _ => new MapWrapper[A,B](m) with MapWrapperFix[A, B]
  }
}
