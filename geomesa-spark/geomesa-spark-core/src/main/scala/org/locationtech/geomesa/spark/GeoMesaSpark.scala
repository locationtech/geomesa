/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.spark

import java.util.ServiceLoader

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.geotools.data.Query
import org.opengis.feature.simple.SimpleFeature

trait SpatialRDDProvider {

  def canProcess(params: java.util.Map[String, java.io.Serializable]): Boolean

  def rdd(conf: Configuration,
          sc: SparkContext,
          dsParams: Map[String, String],
          query: Query): RDD[SimpleFeature]

  /**
    * Writes this RDD to a GeoMesa table.
    * The type must exist in the data store, and all of the features in the RDD must be of this type.
    *
    * @param rdd
    * @param writeDataStoreParams
    * @param writeTypeName
    */
  def save(rdd: RDD[SimpleFeature], writeDataStoreParams: Map[String, String], writeTypeName: String): Unit

}

object GeoMesaSpark {

  import scala.collection.JavaConversions._

  lazy val providers: ServiceLoader[SpatialRDDProvider] = ServiceLoader.load(classOf[SpatialRDDProvider])

  def apply(params: java.util.Map[String, java.io.Serializable]): SpatialRDDProvider =
    providers.find(_.canProcess(params)).getOrElse(throw new RuntimeException("Could not find a SparkGISProvider"))
}
