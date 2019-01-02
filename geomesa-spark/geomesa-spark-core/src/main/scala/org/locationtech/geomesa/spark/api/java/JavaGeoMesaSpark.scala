/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.api.java

import java.util
import java.util.AbstractMap.SimpleEntry

import org.apache.hadoop.conf.Configuration
import org.apache.spark.api.java._
import org.apache.spark.api.java.JavaRDD._
import org.geotools.data.Query
import org.locationtech.geomesa.spark.{GeoMesaSpark, Schema, SpatialRDD, SpatialRDDProvider}
import org.opengis.feature.simple.SimpleFeature
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._


object JavaGeoMesaSpark {

  def apply(params: java.util.Map[String, _ <: java.io.Serializable]) =
    JavaSpatialRDDProvider(GeoMesaSpark.apply(params.asInstanceOf[java.util.Map[String, java.io.Serializable]]))
}

object JavaSpatialRDDProvider {

  def apply(provider: SpatialRDDProvider) = new JavaSpatialRDDProvider(provider)
}

class JavaSpatialRDDProvider(provider: SpatialRDDProvider) {

  def rdd(conf: Configuration,
          jsc: JavaSparkContext,
          params: java.util.Map[String, String],
          query: Query): JavaSpatialRDD =
    provider.rdd(conf, jsc.sc, params.toMap, query)

  def save(jrdd: JavaRDD[SimpleFeature],
           params: util.Map[String, String],
           typeName: String): Unit =
    provider.save(jrdd, params.toMap, typeName)
}

object JavaSpatialRDD {

  def apply(rdd: SpatialRDD) = new JavaSpatialRDD(rdd)

  implicit def toJavaSpatialRDD(rdd: SpatialRDD):JavaSpatialRDD = JavaSpatialRDD(rdd)

  implicit def toValueList(in: RDD[SimpleFeature] with Schema): RDD[util.List[AnyRef]] =
    in.map(sf => util.Arrays.asList(sf.getAttributes: _*))

  implicit def toKeyValueEntryList(in: RDD[SimpleFeature] with Schema): RDD[util.List[util.Map.Entry[String, AnyRef]]] =
    in.map(_.getProperties.map(p => new SimpleEntry(p.getName.getLocalPart, p.getValue)))
      .map(i => util.Arrays.asList(i.toSeq: _*))

  implicit def toKeyValueArrayList(in: RDD[SimpleFeature] with Schema): RDD[util.List[Array[AnyRef]]] =
    in.map(_.getProperties.map(p => Array(p.getName.getLocalPart, p.getValue)))
      .map(i => util.Arrays.asList(i.toSeq: _*))

  implicit def toKeyValueJavaMap(in: RDD[SimpleFeature] with Schema): RDD[util.Map[String, AnyRef]] =
    SpatialRDD.toKeyValueMap(in)
      .map(new util.HashMap(_))

  implicit def toGeoJSONString(in: RDD[SimpleFeature] with Schema): RDD[String] =
    SpatialRDD.toGeoJSONString(in)

}

class JavaSpatialRDD(val srdd: SpatialRDD) extends JavaRDD[SimpleFeature](srdd) with Schema {
  import JavaSpatialRDD._

  def schema = srdd.schema

  def asValueList:          JavaRDD[util.List[Object]]                         = toValueList(srdd)
  def asKeyValueEntryList:  JavaRDD[util.List[util.Map.Entry[String, Object]]] = toKeyValueEntryList(srdd)
  def asKeyValueArrayList:  JavaRDD[util.List[Array[AnyRef]]]                  = toKeyValueArrayList(srdd)
  def asKeyValueMap:        JavaRDD[util.Map[String, Object]]                  = toKeyValueJavaMap(srdd)
  def asGeoJSONString:      JavaRDD[String]                                    = toGeoJSONString(srdd)

  @deprecated
  def asKeyValueList = asKeyValueEntryList
}

