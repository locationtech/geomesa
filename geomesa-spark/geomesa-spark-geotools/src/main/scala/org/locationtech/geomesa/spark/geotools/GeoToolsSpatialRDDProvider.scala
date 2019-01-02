/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.geotools

import java.io.Serializable
import java.util

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.geotools.data.{DataStoreFinder, Query, Transaction}
import org.locationtech.geomesa.spark.{SpatialRDD, SpatialRDDProvider}
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.FeatureUtils
import org.locationtech.geomesa.utils.io.CloseQuietly
import org.opengis.feature.simple.SimpleFeature

import scala.collection.JavaConversions._

class GeoToolsSpatialRDDProvider extends SpatialRDDProvider with LazyLogging {
  import org.locationtech.geomesa.spark.CaseInsensitiveMapFix._

  override def canProcess(params: util.Map[String, Serializable]): Boolean = {
    params.containsKey("geotools") &&
      DataStoreFinder.getAllDataStores.exists(_.canProcess(params))
  }

  override def rdd(conf: Configuration,
                   sc: SparkContext,
                   params: Map[String, String],
                   query: Query): SpatialRDD = {
    val ds = DataStoreFinder.getDataStore(params)
    val fr = ds.getFeatureReader(query, Transaction.AUTO_COMMIT)
    val sft = fr.getFeatureType
    val rdd = sc.parallelize(SelfClosingIterator(fr).toList)
    ds.dispose()
    SpatialRDD(rdd, sft)
  }

  /**
    * Writes this RDD to a GeoMesa data store.
    * The type must exist in the data store, and all of the features in the RDD must be of this type.
    *
    * @param rdd
    * @param params
    * @param typeName
    */
  override def save(rdd: RDD[SimpleFeature], params: Map[String, String], typeName: String): Unit = {
    val ds = DataStoreFinder.getDataStore(params)

    try {
      require(ds.getSchema(typeName) != null,
        "Feature type must exist before calling save.  Call createSchema on the DataStore first.")
    } finally {
      ds.dispose()
    }

    rdd.foreachPartition { iter =>
      val ds = DataStoreFinder.getDataStore(params)
      val featureWriter = ds.getFeatureWriterAppend(typeName, Transaction.AUTO_COMMIT)
      try {
        iter.foreach { sf =>
          FeatureUtils.copyToWriter(featureWriter, sf, useProvidedFid = true)
          featureWriter.write()
        }
      } finally {
        CloseQuietly(featureWriter)
        ds.dispose()
      }
    }
  }
}
