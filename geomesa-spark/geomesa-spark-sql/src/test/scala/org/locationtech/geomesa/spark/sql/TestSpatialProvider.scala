/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.spark.sql

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.geotools.api.data.{DataStore, Query, Transaction}
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.locationtech.geomesa.memory.cqengine.datastore.GeoCQEngineDataStore
import org.locationtech.geomesa.spark.{SpatialRDD, SpatialRDDProvider}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.WithClose

import java.util.concurrent.ConcurrentHashMap

class TestSpatialProvider extends SpatialRDDProvider {

  override def canProcess(params: java.util.Map[String, _]): Boolean = params.containsKey(TestSpatialProvider.StoreParam)

  override def sft(params: Map[String, String], typeName: String): Option[SimpleFeatureType] =
    Option(lookup(params).getSchema(typeName))

  override def rdd(conf: Configuration, sc: SparkContext, params: Map[String, String], query: Query): SpatialRDD = {
    val ds = lookup(params)
    val (sft, features) = WithClose(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)) { reader =>
      (reader.getFeatureType, CloseableIterator(reader).toList)
    }
    SpatialRDD(sc.parallelize(features), sft)
  }

  override def save(rdd: RDD[SimpleFeature], params: Map[String, String], typeName: String): Unit = ???

  private def lookup(params: Map[String, String]): DataStore = {
      params.get(TestSpatialProvider.StoreParam)
        .flatMap(k => Option(TestSpatialProvider.data.get(k)))
        .getOrElse(throw new IllegalArgumentException(s"No spatial provider found for: $params"))
  }
}

object TestSpatialProvider {

  val StoreParam = "test-spark"

  private val data = new ConcurrentHashMap[String, GeoCQEngineDataStore]()

  def newStore(params: java.util.Map[String, _]): DataStore = {
    val key = params.get(StoreParam)
    require(key != null, s"No key defined under $StoreParam")
    val store = new GeoCQEngineDataStore(true)
    data.put(key.toString, store)
    store
  }

  def dispose(params: java.util.Map[String, _]): Unit = {
    val key = params.get(StoreParam)
    require(key != null, s"No key defined under $StoreParam")
    data.remove(key).dispose()
  }
}
