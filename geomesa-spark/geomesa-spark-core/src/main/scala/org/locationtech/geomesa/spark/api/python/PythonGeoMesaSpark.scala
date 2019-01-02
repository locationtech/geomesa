/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.api.python

import org.apache.hadoop.conf.Configuration
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.geotools.data.Query
import org.locationtech.geomesa.spark.{GeoMesaSpark, Schema, SpatialRDD, SpatialRDDProvider}
import org.locationtech.geomesa.spark.api.java.JavaSpatialRDD
import org.opengis.feature.simple.SimpleFeature

import scala.collection.JavaConversions._

object PythonGeoMesaSpark {
  def apply(params: java.util.Map[String, _ <: java.io.Serializable]) =
    PythonSpatialRDDProvider(GeoMesaSpark.apply(params.asInstanceOf[java.util.Map[String, java.io.Serializable]]))
}

object PythonSpatialRDDProvider {
  def apply(provider: SpatialRDDProvider) = new PythonSpatialRDDProvider(provider)
}

class PythonSpatialRDDProvider(provider: SpatialRDDProvider) {
  def rdd(conf: Configuration,
          jsc: JavaSparkContext,
          params: java.util.Map[String, String],
          query: Query): PythonSpatialRDD =
    provider.rdd(conf, jsc.sc, params.toMap, query)
}

object PythonSpatialRDD {

  implicit def toPythonSpatialRDD(rdd: SpatialRDD): PythonSpatialRDD = PythonSpatialRDD(rdd)

  def apply(rdd: JavaSpatialRDD) = new PythonSpatialRDD(rdd)
}

class PythonSpatialRDD(jsrdd: JavaSpatialRDD) extends JavaRDD[SimpleFeature](jsrdd) with Schema {

  import org.apache.spark.geomesa.api.python.GeoMesaSeDerUtil._

  def schema = jsrdd.schema

  def asValueList:          JavaRDD[Array[Byte]]  = jsrdd.asValueList
  def asKeyValueTupleList:  JavaRDD[Array[Byte]]  = jsrdd.asKeyValueArrayList
  def asKeyValueDict:       JavaRDD[Array[Byte]]  = jsrdd.asKeyValueMap
  def asGeoJSONString:      JavaRDD[Array[Byte]]  = jsrdd.asGeoJSONString
}


