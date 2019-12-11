/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.cassandra.spark

import com.datastax.driver.core.RegularStatement
import com.typesafe.scalalogging.LazyLogging
import org.apache.cassandra.hadoop.ConfigHelper
import org.apache.cassandra.hadoop.cql3.CqlConfigHelper
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.geotools.data.{Query, Transaction}
import org.opengis.feature.simple.SimpleFeature
import org.locationtech.geomesa.cassandra.data.{CassandraDataStore, CassandraDataStoreFactory, CassandraQueryPlan, EmptyPlan}
import org.locationtech.geomesa.cassandra.data.CassandraDataStoreFactory.Params
import org.locationtech.geomesa.cassandra.jobs.CassandraJobUtils
import org.locationtech.geomesa.index.conf.QueryHints._
import org.locationtech.geomesa.jobs.GeoMesaConfigurator
import org.locationtech.geomesa.spark.{DataStoreConnector, SpatialRDD, SpatialRDDProvider}
import org.locationtech.geomesa.utils.geotools.FeatureUtils
import org.locationtech.geomesa.utils.io.{WithClose, WithStore}

class CassandraSpatialRDDProvider extends SpatialRDDProvider with LazyLogging {

  override def canProcess(params: java.util.Map[String, _ <: java.io.Serializable]): Boolean = {
    CassandraDataStoreFactory.canProcess(params)
  }

  def rdd(
      conf: Configuration,
      sc: SparkContext,
      dsParams: Map[String, String],
      origQuery: Query): SpatialRDD = {

    val ds = DataStoreConnector[CassandraDataStore](dsParams)
    lazy val sft = ds.getSchema(origQuery.getTypeName)

    def queryPlanToRdd(qp: CassandraQueryPlan) : RDD[SimpleFeature] = {
      /*
       * Map function from query plan to RDD.
       */
      val config = new Configuration(conf)
      if (ds == null || sft == null || qp.isInstanceOf[EmptyPlan]) {
        sc.emptyRDD[SimpleFeature]
      }

      // BEGIN: Attempting to set CQL
      // We attempt to set Cql from the ranges field from the query plan. However, the CQL from the
      // query plan is not in the format expected by the delegate of the GeoMesaCassandraInputFormat.
      // See CassandraRecordReader in GeoMesaCassandraInputFormat for more information.
      //
      // Commenting out this section will allow the CassandraSparkProviderTest to pass but we are
      // unsure if the implementation will generalize to all queries.
      if (qp.ranges.size == 1) {
        qp.ranges.head match {
          case stmt: RegularStatement => CqlConfigHelper.setInputCql(
            config,
            stmt.getQueryString
          )
          case _ => throw new IllegalArgumentException
        }
      } else {
        throw new IndexOutOfBoundsException
      }
      // END: Attempting to set CQL

      ConfigHelper.setInputInitialAddress(config, dsParams("geomesa.cassandra.host"))
      ConfigHelper.setInputColumnFamily(config, dsParams(Params.KeySpaceParam.getName), qp.tables.head)
      ConfigHelper.setInputPartitioner(config, "Murmur3Partitioner")

      GeoMesaConfigurator.setFilter(config, qp.filter.toString)
      GeoMesaConfigurator.setResultsToFeatures(config, qp.resultsToFeatures)

      qp.reducer.foreach(GeoMesaConfigurator.setReducer(config,_))
      qp.projection.foreach(GeoMesaConfigurator.setProjection(config,_))
      qp.sort.foreach(GeoMesaConfigurator.setSorting(config,_))

      sc.newAPIHadoopRDD(config, classOf[GeoMesaCassandraInputFormat], classOf[Text], classOf[SimpleFeature]).map(_._2)
    }

    try {
      lazy val rddSft = origQuery.getHints.getTransformSchema.getOrElse(sft)

      lazy val qps = {
        CassandraJobUtils.getMultiStatementPlans(ds, origQuery)
      }

      if (ds == null || sft == null || qps.isEmpty) {
        SpatialRDD(sc.emptyRDD[SimpleFeature], rddSft)
      } else {
        val rdd = qps.map(queryPlanToRdd) match {
          case Seq(head) => head
          case seq => sc.union(seq)
        }
        SpatialRDD(rdd, rddSft)
      }

    } finally {
      if (ds != null) {
        ds.dispose()
      }
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

    // Base implementation from GeoToolsSpatialRDDProvider
    val params = writeDataStoreParams
    val typeName = writeTypeName
    WithStore[CassandraDataStore](params) { ds =>
      require(ds != null, "Could not load data store with the provided parameters")
      require(ds.getSchema(typeName) != null, "Schema must exist before calling save - use `DataStore.createSchema`")
    }

    rdd.foreachPartition { iter =>
      WithStore[CassandraDataStore](params) { ds =>
        WithClose(ds.getFeatureWriterAppend(typeName, Transaction.AUTO_COMMIT)) { writer =>
          iter.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
        }
      }
    }
  }
}
