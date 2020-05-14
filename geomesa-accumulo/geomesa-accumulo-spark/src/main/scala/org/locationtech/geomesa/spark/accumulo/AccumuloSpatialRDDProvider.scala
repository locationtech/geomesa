/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * Portions Crown Copyright (c) 2017-2020 Dstl
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.accumulo

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.{NewHadoopRDD, RDD}
import org.geotools.data.{DataStoreFinder, Query, Transaction}
import org.locationtech.geomesa.accumulo.data.AccumuloQueryPlan.{BatchScanPlan, EmptyPlan}
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloDataStoreFactory, AccumuloQueryPlan}
import org.locationtech.geomesa.index.conf.QueryHints._
import org.locationtech.geomesa.jobs.accumulo.AccumuloJobUtils
import org.locationtech.geomesa.jobs.mapreduce._
import org.locationtech.geomesa.spark.{SpatialRDD, SpatialRDDProvider}
import org.locationtech.geomesa.utils.geotools.FeatureUtils
import org.locationtech.geomesa.utils.io.{WithClose, WithStore}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class AccumuloSpatialRDDProvider extends SpatialRDDProvider with LazyLogging {

  import scala.collection.JavaConverters._

  override def canProcess(params: java.util.Map[String, _ <: java.io.Serializable]): Boolean =
    AccumuloDataStoreFactory.canProcess(params)

  override def rdd(conf: Configuration,
                   sc: SparkContext,
                   params: Map[String, String],
                   query: Query): SpatialRDD = {
    val paramsAsJava = params.asJava
    val ds = DataStoreFinder.getDataStore(paramsAsJava).asInstanceOf[AccumuloDataStore]

    lazy val transform = query.getHints.getTransformSchema

    def queryPlanToRDD(sft: SimpleFeatureType, qp: AccumuloQueryPlan): RDD[SimpleFeature] = {
      if (ds == null || sft == null || qp.isInstanceOf[EmptyPlan]) {
        sc.emptyRDD[SimpleFeature]
      } else {
        val job = new Job(conf)
        GeoMesaAccumuloInputFormat.configure(job, paramsAsJava, qp)
        // We soon want to call this
        // sc.newAPIHadoopRDD(conf, classOf[GeoMesaAccumuloInputFormat], classOf[Text], classOf[SimpleFeature]).map(U => U._2)
        // But we need access to the JobConf that this creates internally and doesn't expose, so we repeat (most of) the code here

        // From sc.newAPIHadoopRDD, but can't implement this here
        // assertNotStopped()

        // From sc.newAPIHadoopRDD
        // Add necessary security credentials to the JobConf. Required to access secure HDFS.
        val jconf = job.getConfiguration.asInstanceOf[JobConf]
        SparkHadoopUtil.get.addCredentials(jconf)

        // Iterate over tokens in credentials and add the Accumulo one to the configuration directly
        // This is because the credentials seem to disappear between here and the YARN executor
        // See https://stackoverflow.com/questions/44525351/delegation-tokens-with-accumulo-spark
        val tokens = jconf.getCredentials.getAllTokens.iterator()
        var hasNext = tokens.hasNext
        while (hasNext) {
          val token = tokens.next()
          if (token.getKind.toString == "ACCUMULO_AUTH_TOKEN") {
            logger.info("Adding ACCUMULO_AUTH_TOKEN to configuration")
            jconf.set("org.locationtech.geomesa.token", token.encodeToUrlString())
            hasNext = false
          } else {
            hasNext = tokens.hasNext
          }
        }

        // From sc.newAPIHadoopRDD
        new NewHadoopRDD(sc, classOf[GeoMesaAccumuloInputFormat], classOf[Text], classOf[SimpleFeature], jconf).map(_._2)
      }
    }

    try {
      // get the query plan to set up the iterators, ranges, etc
      // getMultipleQueryPlan will return the fallback if any
      // element of the plan is a JoinPlan
      val sft = ds.getSchema(query.getTypeName)
      val qps = AccumuloJobUtils.getMultipleQueryPlan(ds, query)

      // can return a union of the RDDs because the query planner *should*
      // be rewriting ORs to make them logically disjoint
      // e.g. "A OR B OR C" -> "A OR (B NOT A) OR ((C NOT A) NOT B)"
      val sfrdd = if (qps.lengthCompare(1) == 0 && qps.head.tables.lengthCompare(1) == 0) {
        queryPlanToRDD(sft, qps.head) // no union needed for single query plan
      } else {
        // flatten and duplicate the query plans so each one only has a single table
        val expanded = qps.flatMap {
          case qp: BatchScanPlan => qp.tables.map(t => qp.copy(tables = Seq(t)))
          case qp: EmptyPlan => Seq(qp)
          case qp => throw new NotImplementedError(s"Unexpected query plan type: $qp")
        }
        sc.union(expanded.map(queryPlanToRDD(sft, _)))
      }
      SpatialRDD(sfrdd, transform.getOrElse(sft))
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
    * @param params params
    * @param typeName type name
    */
  def save(rdd: RDD[SimpleFeature], params: Map[String, String], typeName: String): Unit = {
    WithStore[AccumuloDataStore](params) { ds =>
      require(ds.getSchema(typeName) != null,
        "Feature type must exist before calling save. Call createSchema on the DataStore first.")
    }

    rdd.foreachPartition { iter =>
      WithStore[AccumuloDataStore](params) { ds =>
        WithClose(ds.getFeatureWriterAppend(typeName, Transaction.AUTO_COMMIT)) { writer =>
          iter.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
        }
      }
    }
  }
}
