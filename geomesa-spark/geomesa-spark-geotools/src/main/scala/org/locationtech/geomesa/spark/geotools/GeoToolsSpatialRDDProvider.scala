/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.spark.geotools

import java.io.Serializable
import java.util

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.geotools.data.{DataStoreFinder, Query, Transaction}
import org.locationtech.geomesa.spark.SpatialRDDProvider
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.locationtech.geomesa.utils.geotools.FeatureUtils
import org.locationtech.geomesa.utils.io.CloseQuietly
import org.opengis.feature.simple.SimpleFeature

import scala.collection.JavaConversions._

class GeoToolsSpatialRDDProvider extends SpatialRDDProvider with LazyLogging {
  override def canProcess(params: util.Map[String, Serializable]): Boolean = {
    params.containsKey("geotools") &&
      DataStoreFinder.getAllDataStores.exists(_.canProcess(params))
  }

  override def rdd(conf: Configuration,
                   sc: SparkContext,
                   dsParams: Map[String, String],
                   query: Query): RDD[SimpleFeature] = {
    val ds = DataStoreFinder.getDataStore(dsParams)
    val fr = ds.getFeatureReader(query, Transaction.AUTO_COMMIT)
    val rdd = sc.parallelize(fr.toIterator.toSeq)
    ds.dispose()
    rdd
  }

  /**
    * Writes this RDD to a GeoMesa data store.
    * The type must exist in the data store, and all of the features in the RDD must be of this type.
    *
    * @param rdd
    * @param writeDataStoreParams
    * @param writeTypeName
    */
  override def save(rdd: RDD[SimpleFeature], writeDataStoreParams: Map[String, String], writeTypeName: String): Unit = {
    val ds = DataStoreFinder.getDataStore(writeDataStoreParams)

    try {
      require(ds.getSchema(writeTypeName) != null,
        "Feature type must exist before calling save.  Call createSchema on the DataStore first.")
    } finally {
      ds.dispose()
    }

    rdd.foreachPartition { iter =>
      val ds = DataStoreFinder.getDataStore(writeDataStoreParams)
      val featureWriter = ds.getFeatureWriterAppend(writeTypeName, Transaction.AUTO_COMMIT)
      try {
        iter.foreach {
          FeatureUtils.copyToWriter(featureWriter, _)
        }
      } finally {
        CloseQuietly(featureWriter)
        ds.dispose()
      }
    }
  }
}
