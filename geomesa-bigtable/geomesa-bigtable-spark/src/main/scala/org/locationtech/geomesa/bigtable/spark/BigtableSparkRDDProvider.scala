/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.bigtable.spark

import com.google.cloud.bigtable.hbase.BigtableExtendedScan
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.io.Text
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.geotools.data.Query
import org.locationtech.geomesa.bigtable.data.BigtableDataStoreFactory
import org.locationtech.geomesa.hbase.data.HBaseDataStore
import org.locationtech.geomesa.hbase.data.HBaseQueryPlan.ScanPlan
import org.locationtech.geomesa.hbase.jobs.HBaseJobUtils
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.jobs.GeoMesaConfigurator
import org.locationtech.geomesa.spark.SpatialRDD
import org.locationtech.geomesa.spark.hbase.HBaseSpatialRDDProvider
import org.locationtech.geomesa.utils.io.WithStore
import org.opengis.feature.simple.SimpleFeature

class BigtableSparkRDDProvider extends HBaseSpatialRDDProvider {

  import org.locationtech.geomesa.index.conf.QueryHints.RichHints

  override def canProcess(params: java.util.Map[String, _ <: java.io.Serializable]): Boolean =
    BigtableDataStoreFactory.canProcess(params)

  override def rdd(
      conf: Configuration,
      sc: SparkContext,
      dsParams: Map[String, String],
      origQuery: Query): SpatialRDD = {

    WithStore[HBaseDataStore](dsParams) { ds =>
      // get the query plan to set up the iterators, ranges, etc
      lazy val sft = ds.getSchema(origQuery.getTypeName)
      lazy val qps = {
        // force loose bbox to be false
        origQuery.getHints.put(QueryHints.LOOSE_BBOX, false)
        // flatten and duplicate the query plans so each one only has a single table
        HBaseJobUtils.getMultiScanPlans(ds, origQuery)
      }
      // note: only access this after getting the query plans so that the hint is set
      lazy val rddSft = origQuery.getHints.getTransformSchema.getOrElse(sft)

      def queryPlanToRdd(qp: ScanPlan): RDD[SimpleFeature] = {
        val config = new Configuration(conf)
        // note: we've ensured there is only one table per query plan, below
        config.set(TableInputFormat.INPUT_TABLE, qp.scans.head.table.getNameAsString)
        val scans = qp.scans.head.scans.map {
          case scan: BigtableExtendedScan =>
            // need to set the table name in each scan
            scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, qp.scans.head.table.getName)
            BigtableInputFormatBase.scanToString(scan)

          case scan: org.apache.hadoop.hbase.client.Scan =>
            val bes = new BigtableExtendedScan()
            bes.addRange(scan.getStartRow, scan.getStopRow)
            bes.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, qp.scans.head.table.getName)
            BigtableInputFormatBase.scanToString(bes)
        }
        config.setStrings(BigtableInputFormat.SCANS, scans: _*)

        GeoMesaConfigurator.setResultsToFeatures(config, qp.resultsToFeatures)

        // note: simple reducers (e.g. filter/transform) can be handled by the input format,
        // more complex ones should be done in the job reducers
        qp.reducer.foreach(GeoMesaConfigurator.setReducer(config, _))
        // note: sorting has to be handled in the job reducers
        qp.sort.foreach(GeoMesaConfigurator.setSorting(config, _))
        qp.projection.foreach(GeoMesaConfigurator.setProjection(config, _))

        sc.newAPIHadoopRDD(config, classOf[GeoMesaBigtableInputFormat], classOf[Text], classOf[SimpleFeature]).map(_._2)
      }

      if (ds == null || sft == null || qps.isEmpty) {
        SpatialRDD(sc.emptyRDD[SimpleFeature], rddSft)
      } else {
        // can return a union of the RDDs because the query planner rewrites ORs to make them logically disjoint
        // e.g. "A OR B OR C" -> "A OR (B NOT A) OR ((C NOT A) NOT B)"
        val rdd = qps.map(queryPlanToRdd) match {
          case Seq(head) => head // no need to union a single rdd
          case seq       => sc.union(seq)
        }
        SpatialRDD(rdd, rddSft)
      }
    }
  }
}
