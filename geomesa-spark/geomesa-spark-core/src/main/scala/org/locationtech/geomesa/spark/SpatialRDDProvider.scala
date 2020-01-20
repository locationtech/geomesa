/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.geotools.data.{DataStore, Query}
import org.locationtech.geomesa.utils.io.WithStore
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
  * Provider trait for loading spatial RDDs, generally backed by GeoTools data store implementations
  */
trait SpatialRDDProvider {

  /**
    * Indicates if the provider can process the given parameters
    *
    * @param params data store parameters
    * @return
    */
  def canProcess(params: java.util.Map[String, _ <: java.io.Serializable]): Boolean

  /**
    * Load an RDD for the given query.
    *
    * This method is implicitly guarded by `canProcess`; it is an error to try to load an RDD if `canProcess`
    * returns false for the given parameters.
    *
    * @param conf conf
    * @param sc spark context
    * @param params data store parameters
    * @param query query for features to load
    * @return
    */
  def rdd(conf: Configuration, sc: SparkContext, params: Map[String, String], query: Query) : SpatialRDD

  /**
    * Persist an RDD to long-term storage.
    *
    * The type must exist in the data store, and all of the features in the RDD must be of this type.
    *
    * @param rdd rdd to save
    * @param params data store parameters
    * @param typeName simple feature type name
    */
  def save(rdd: RDD[SimpleFeature], params: Map[String, String], typeName: String): Unit

  /**
    * Load an existing simple feature type
    *
    * @param params data store parameters
    * @param typeName simple feature type name
    * @return
    */
  def sft(params: Map[String, String], typeName: String): Option[SimpleFeatureType] =
    Option(WithStore[DataStore](params)(_.getSchema(typeName)))
}
