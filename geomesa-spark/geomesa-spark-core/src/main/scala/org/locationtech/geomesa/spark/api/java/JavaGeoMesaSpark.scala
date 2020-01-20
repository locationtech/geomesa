/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.api.java

import java.util
import java.util.AbstractMap.SimpleEntry
import java.util.Map.Entry

import org.apache.hadoop.conf.Configuration
import org.apache.spark.api.java.JavaRDD._
import org.apache.spark.api.java._
import org.apache.spark.rdd.RDD
import org.geotools.data.Query
import org.locationtech.geomesa.spark.{GeoMesaSpark, Schema, SpatialRDD, SpatialRDDProvider}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

object JavaGeoMesaSpark {
  def apply(params: java.util.Map[String, _ <: java.io.Serializable]) =
    JavaSpatialRDDProvider(GeoMesaSpark.apply(params))
}

object JavaSpatialRDDProvider {
  def apply(provider: SpatialRDDProvider) = new JavaSpatialRDDProvider(provider)
}

class JavaSpatialRDDProvider(provider: SpatialRDDProvider) {

  import scala.collection.JavaConverters._

  def rdd(
      conf: Configuration,
      jsc: JavaSparkContext,
      params: java.util.Map[String, String],
      query: Query): JavaSpatialRDD =
    provider.rdd(conf, jsc.sc, params.asScala.toMap, query)

  def save(jrdd: JavaRDD[SimpleFeature], params: java.util.Map[String, String], typeName: String): Unit =
    provider.save(jrdd, params.asScala.toMap, typeName)
}

object JavaSpatialRDD {

  import scala.collection.JavaConverters._

  def apply(rdd: SpatialRDD): JavaSpatialRDD = new JavaSpatialRDD(rdd)

  implicit def toJavaSpatialRDD(rdd: SpatialRDD):JavaSpatialRDD = JavaSpatialRDD(rdd)

  implicit def toValueList(in: RDD[SimpleFeature] with Schema): RDD[java.util.List[AnyRef]] =
    in.map(_.getAttributes)

  implicit def toKeyValueEntryList(in: RDD[SimpleFeature] with Schema): RDD[java.util.List[Entry[String, AnyRef]]] = {
    in.map { sf =>
      val entries = new java.util.ArrayList[Entry[String, AnyRef]](sf.getAttributeCount)
      sf.getProperties.asScala.foreach(p => entries.add(new SimpleEntry(p.getName.getLocalPart, p.getValue)))
      entries
    }
  }

  implicit def toKeyValueArrayList(in: RDD[SimpleFeature] with Schema): RDD[java.util.List[Array[AnyRef]]] = {
    in.map { sf =>
      val entries = new java.util.ArrayList[Array[AnyRef]](sf.getAttributeCount)
      sf.getProperties.asScala.foreach(p => entries.add(Array[AnyRef](p.getName.getLocalPart, p.getValue)))
      entries
    }
  }

  implicit def toKeyValueJavaMap(in: RDD[SimpleFeature] with Schema): RDD[java.util.Map[String, AnyRef]] =
    SpatialRDD.toKeyValueMap(in).map(_.asJava)

  implicit def toGeoJSONString(in: RDD[SimpleFeature] with Schema): RDD[String] =
    SpatialRDD.toGeoJSONString(in)
}

class JavaSpatialRDD(val srdd: SpatialRDD) extends JavaRDD[SimpleFeature](srdd) with Schema {
  import JavaSpatialRDD._

  def schema: SimpleFeatureType = srdd.schema

  def asValueList:          JavaRDD[util.List[Object]]                         = toValueList(srdd)
  def asKeyValueEntryList:  JavaRDD[util.List[util.Map.Entry[String, Object]]] = toKeyValueEntryList(srdd)
  def asKeyValueArrayList:  JavaRDD[util.List[Array[AnyRef]]]                  = toKeyValueArrayList(srdd)
  def asKeyValueMap:        JavaRDD[util.Map[String, Object]]                  = toKeyValueJavaMap(srdd)
  def asGeoJSONString:      JavaRDD[String]                                    = toGeoJSONString(srdd)

  @deprecated
  def asKeyValueList = asKeyValueEntryList
}

