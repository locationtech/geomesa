/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Get, Scan}
import org.apache.hadoop.hbase.mapreduce.MultiTableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Base64
import org.apache.hadoop.io.Text
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.geotools.data.{Query, Transaction}
import org.locationtech.geomesa.hbase.data.HBaseQueryPlan.{EmptyPlan, ScanPlan}
import org.locationtech.geomesa.hbase.data._
import org.locationtech.geomesa.hbase.jobs.GeoMesaHBaseInputFormat
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.jobs.GeoMesaConfigurator
import org.locationtech.geomesa.spark.{DataStoreConnector, SpatialRDD, SpatialRDDProvider}
import org.locationtech.geomesa.utils.geotools.FeatureUtils
import org.locationtech.geomesa.utils.io.CloseQuietly
import org.opengis.feature.simple.SimpleFeature

class HBaseSpatialRDDProvider extends SpatialRDDProvider {

  import org.locationtech.geomesa.index.conf.QueryHints._

  override def canProcess(params: java.util.Map[String, java.io.Serializable]): Boolean =
    HBaseDataStoreFactory.canProcess(params)

  def rdd(conf: Configuration,
          sc: SparkContext,
          dsParams: Map[String, String],
          origQuery: Query): SpatialRDD = {

    val ds = DataStoreConnector.loadingMap.get(dsParams).asInstanceOf[HBaseDataStore]

    // get the query plan to set up the iterators, ranges, etc
    lazy val sft = ds.getSchema(origQuery.getTypeName)
    lazy val qps = {
      // force loose bbox to be false
      origQuery.getHints.put(QueryHints.LOOSE_BBOX, false)
      ds.getQueryPlan(origQuery)
    }
    // note: make sure to access this after qps, so that hints are set
    lazy val transform = origQuery.getHints.getTransformSchema

    def queryPlanToRDD(qp: HBaseQueryPlan, conf: Configuration): RDD[SimpleFeature] = {
      if (qp.isInstanceOf[EmptyPlan]) {
        sc.emptyRDD[SimpleFeature]
      } else {
        GeoMesaConfigurator.setSchema(conf, sft)
        GeoMesaConfigurator.setSerialization(conf)
        GeoMesaConfigurator.setIndexIn(conf, qp.filter.index)
        // note: we've ensured there is only one table per query plan, below
        GeoMesaConfigurator.setTable(conf, qp.tables.head.getNameAsString)
        transform.foreach(GeoMesaConfigurator.setTransformSchema(conf, _))
        // note: secondary filter is handled by scan push-down filter
        val scans = qp.scans.map { scan =>
          // need to set the table name in each scan
          scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, qp.tables.head.getName)
          convertScanToString(scan)
        }
        conf.setStrings(MultiTableInputFormat.SCANS, scans: _*)

        sc.newAPIHadoopRDD(conf, classOf[GeoMesaHBaseInputFormat], classOf[Text], classOf[SimpleFeature]).map(_._2)
      }
    }

    try {
      if (ds == null || sft == null || qps.isEmpty || qps.forall(_.isInstanceOf[EmptyPlan])) {
        SpatialRDD(sc.emptyRDD[SimpleFeature], origQuery.getHints.getTransformSchema.getOrElse(sft))
      } else {
        // can return a union of the RDDs because the query planner *should*
        // be rewriting ORs to make them logically disjoint
        // e.g. "A OR B OR C" -> "A OR (B NOT A) OR ((C NOT A) NOT B)"
        val rdd = if (qps.lengthCompare(1) == 0 && qps.head.tables.lengthCompare(1) == 0) {
          queryPlanToRDD(qps.head, conf) // no union needed for single query plan
        } else {
          // flatten and duplicate the query plans so each one only has a single table
          val expanded = qps.flatMap {
            case qp: ScanPlan => qp.tables.map(t => qp.copy(tables = Seq(t)))
            case qp: EmptyPlan => Seq(qp)
            case qp => throw new NotImplementedError(s"Unexpected query plan type: $qp")
          }
          sc.union(expanded.map(queryPlanToRDD(_, new Configuration(conf))))
        }
        SpatialRDD(rdd, transform.getOrElse(sft))
      }
    } finally {
      if (ds != null) {
        ds.dispose()
      }
    }
  }

  private def convertScanToString(scan: org.apache.hadoop.hbase.client.Query): String = scan match {
    case g: Get  => Base64.encodeBytes(ProtobufUtil.toGet(g).toByteArray)
    case s: Scan => Base64.encodeBytes(ProtobufUtil.toScan(s).toByteArray)
  }

  /**
    * Writes this RDD to a GeoMesa table.
    * The type must exist in the data store, and all of the features in the RDD must be of this type.
    *
    * @param rdd
    * @param writeDataStoreParams
    * @param writeTypeName
    */
  def save(rdd: RDD[SimpleFeature], writeDataStoreParams: Map[String, String], writeTypeName: String): Unit = {
    val ds = DataStoreConnector.loadingMap.get(writeDataStoreParams).asInstanceOf[HBaseDataStore]
    try {
      require(ds.getSchema(writeTypeName) != null,
        "Feature type must exist before calling save.  Call createSchema on the DataStore first.")
    } finally {
      ds.dispose()
    }

    unsafeSave(rdd, writeDataStoreParams, writeTypeName)
  }

  /**
    * Writes this RDD to a GeoMesa table.
    * The type must exist in the data store, and all of the features in the RDD must be of this type.
    * This method assumes that the schema exists.
    *
    * @param rdd
    * @param writeDataStoreParams
    * @param writeTypeName
    */
  def unsafeSave(rdd: RDD[SimpleFeature], writeDataStoreParams: Map[String, String], writeTypeName: String): Unit = {
    rdd.foreachPartition { iter =>
      val ds = DataStoreConnector.loadingMap.get(writeDataStoreParams).asInstanceOf[HBaseDataStore]
      val featureWriter = ds.getFeatureWriterAppend(writeTypeName, Transaction.AUTO_COMMIT)
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
