/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.cassandra

import org.apache.cassandra.hadoop.ConfigHelper
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.geotools.data.{Query, Transaction}
import org.locationtech.geomesa.cassandra.data._
import org.locationtech.geomesa.cassandra.jobs.CassandraJobUtils
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.spark.{DataStoreConnector, SpatialRDD, SpatialRDDProvider}
import org.locationtech.geomesa.utils.geotools.FeatureUtils
import org.locationtech.geomesa.utils.io.WithClose
import org.apache.hadoop.io.Text
import org.opengis.feature.simple.SimpleFeature
import org.locationtech.geomesa.index.conf.QueryHints._

class CassandraSpatialRDDProvider extends SpatialRDDProvider {

  override def canProcess(params: java.util.Map[String, _ <: java.io.Serializable]): Boolean =
    CassandraDataStoreFactory.canProcess(params)

  def rdd(
      conf: Configuration,
      sc: SparkContext,
      dsParams: Map[String, String],
      origQuery: Query): SpatialRDD = {

    val ds = DataStoreConnector[CassandraDataStore](dsParams)
    println(origQuery.getTypeName)
    // get the query plan to set up the iterators, ranges, etc
    lazy val sft = ds.getSchema(origQuery.getTypeName)
    lazy val qps = {
      // force loose bbox to be false
      origQuery.getHints.put(QueryHints.LOOSE_BBOX, false)
      // flatten and duplicate the query plans so each one only has a single table
      CassandraJobUtils.getMultiScanPlans(ds, origQuery)
    }
    // note: only access this after getting the query plans so that the hint is set
    lazy val rddSft = origQuery.getHints.getTransformSchema.getOrElse(sft)


    def queryPlanToRdd(qp: StatementPlan): RDD[SimpleFeature] = {
       val config = new Configuration(conf)

      ConfigHelper.setInputInitialAddress(config,dsParams("cassandra.input.address"));
      ConfigHelper.setInputPartitioner(config,dsParams("cassandra.input.partitioner"))

      GeoMesaCassandraInputFormat.configure(config, qp)
      sc.newAPIHadoopRDD(config, classOf[GeoMesaCassandraInputFormat], classOf[Text], classOf[SimpleFeature]).map(_._2)
    }

    if (ds == null || sft == null || qps.isEmpty) {
      SpatialRDD(sc.emptyRDD[SimpleFeature], rddSft)
    }
       else {
      // can return a union of the RDDs because the query planner rewrites ORs to make them logically disjoint
      // e.g. "A OR B OR C" -> "A OR (B NOT A) OR ((C NOT A) NOT B)"
      val rdd = qps.map(queryPlanToRdd) match {
        case Seq(head) => head // no need to union a single rdd
        case seq       => sc.union(seq)
      }
      SpatialRDD(rdd, rddSft)
    }
  }

  /**
    * Writes this RDD to a GeoMesa table.
    * The type must exist in the data store, and all of the features in the RDD must be of this type.
    *
    * @param rdd rdd
    * @param writeDataStoreParams params
    * @param writeTypeName type name
    */
  def save(rdd: RDD[SimpleFeature], writeDataStoreParams: Map[String, String], writeTypeName: String): Unit = {
    val ds = DataStoreConnector[CassandraDataStore](writeDataStoreParams)
    require(ds.getSchema(writeTypeName) != null,
      "Feature type must exist before calling save.  Call createSchema on the DataStore first.")
    unsafeSave(rdd, writeDataStoreParams, writeTypeName)
  }

  /**
    * Writes this RDD to a GeoMesa table.
    * The type must exist in the data store, and all of the features in the RDD must be of this type.
    * This method assumes that the schema exists.
    *
    * @param rdd rdd
    * @param writeDataStoreParams params
    * @param writeTypeName type name
    */
  def unsafeSave(rdd: RDD[SimpleFeature], writeDataStoreParams: Map[String, String], writeTypeName: String): Unit = {
    rdd.foreachPartition { iter =>
      val ds = DataStoreConnector[CassandraDataStore](writeDataStoreParams)
      WithClose(ds.getFeatureWriterAppend(writeTypeName, Transaction.AUTO_COMMIT)) { writer =>
        iter.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
      }
    }
  }
}
