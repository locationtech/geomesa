/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.spark

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.geotools.data.{Query, Transaction}
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.planning.QueryPlanner
import org.locationtech.geomesa.kudu.data.{KuduDataStore, KuduDataStoreFactory}
import org.locationtech.geomesa.spark.{DataStoreConnector, SpatialRDD, SpatialRDDProvider}
import org.locationtech.geomesa.utils.geotools.FeatureUtils
import org.locationtech.geomesa.utils.io.CloseQuietly
import org.opengis.feature.simple.SimpleFeature

class KuduSpatialRDDProvider extends SpatialRDDProvider {

  override def canProcess(params: java.util.Map[String, java.io.Serializable]): Boolean =
    KuduDataStoreFactory.canProcess(params)

  override def rdd(conf: Configuration, sc: SparkContext, params: Map[String, String], query: Query): SpatialRDD = {

    import org.locationtech.geomesa.index.conf.QueryHints.RichHints

    val ds = DataStoreConnector.loadingMap.get(params).asInstanceOf[KuduDataStore]
    // force loose bbox to be false
    query.getHints.put(QueryHints.LOOSE_BBOX, false)

    // get the query plan to set up the iterators, ranges, etc
    lazy val sft = ds.getSchema(query.getTypeName)
    lazy val transform = {
      QueryPlanner.setQueryTransforms(query, sft)
      query.getHints.getTransformSchema
    }

    if (ds == null || sft == null) {
      SpatialRDD(sc.emptyRDD[SimpleFeature], transform.getOrElse(sft))
    } else {
      val jobConf = new JobConf(conf)
      GeoMesaKuduInputFormat.configure(jobConf, params, query)
      GeoMesaKuduInputFormat.addCredentials(jobConf, ds.client)
      val rdd = sc.newAPIHadoopRDD(jobConf, classOf[GeoMesaKuduInputFormat],
        classOf[NullWritable], classOf[SimpleFeature]).map(_._2)
      SpatialRDD(rdd, transform.getOrElse(sft))
    }
  }

  override def save(rdd: RDD[SimpleFeature], params: Map[String, String], typeName: String): Unit = {
    val ds = DataStoreConnector.loadingMap.get(params).asInstanceOf[KuduDataStore]
    try {
      require(ds.getSchema(typeName) != null,
        "Feature type must exist before calling save. Call `createSchema` on the DataStore first.")
    } finally {
      ds.dispose()
    }

    unsafeSave(rdd, params, typeName)
  }

  /**
    * Writes this RDD to a GeoMesa table.
    * The type must exist in the data store, and all of the features in the RDD must be of this type.
    * This method assumes that the schema exists.
    *
    * @param rdd rdd
    * @param params data store connection parameters
    * @param typeName feature type name
    */
  def unsafeSave(rdd: RDD[SimpleFeature], params: Map[String, String], typeName: String): Unit = {
    rdd.foreachPartition { iter =>
      val ds = DataStoreConnector.loadingMap.get(params).asInstanceOf[KuduDataStore]
      val featureWriter = ds.getFeatureWriterAppend(typeName, Transaction.AUTO_COMMIT)
      try {
        iter.foreach { rawFeature =>
          FeatureUtils.copyToWriter(featureWriter, rawFeature, useProvidedFid = true)
          featureWriter.write()
        }
      } finally {
        CloseQuietly(featureWriter)
        ds.dispose()
      }
    }
  }
}
