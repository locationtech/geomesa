/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.cassandra

import com.typesafe.scalalogging.LazyLogging
  import org.apache.cassandra.hadoop.ConfigHelper
  import org.apache.hadoop.conf.{Configurable, Configuration}
  import org.apache.hadoop.io.Text
  import org.apache.hadoop.mapreduce._
  import org.geotools.data.Query
  import org.locationtech.geomesa.cassandra.data.{CassandraDataStore, StatementPlan}
  import org.locationtech.geomesa.cassandra.jobs.CassandraJobUtils1
  import org.locationtech.geomesa.index.api.QueryPlan.{FeatureReducer, ResultsToFeatures}
  import org.locationtech.geomesa.jobs.GeoMesaConfigurator
  import org.locationtech.geomesa.utils.collection.CloseableIterator
  import org.locationtech.geomesa.utils.io.WithStore
  import org.opengis.feature.simple.SimpleFeature
  import org.apache.cassandra.hadoop.cql3.{CqlInputFormat, CqlRecordReader}
  import GeoMesaCassandraInputFormat.GeoMesaCassandraRecordReader

  /**
    * Input format that allows processing of simple features from GeoMesa based on a CQL query
    */
  class GeoMesaCassandraInputFormat extends InputFormat[Text, SimpleFeature] with Configurable with LazyLogging {

    private val delegate = new CqlInputFormat
    /**
      * Gets splits for a job.
      */
    override def getSplits(context: JobContext): java.util.List[InputSplit] = {
      val splits = delegate.getSplits(context)
      println(s"JNH: Got ${splits.size()} Splits.  ${splits.iterator().next()}")
      logger.debug(s"Got ${splits.size()} splits")
      splits
    }

    override def createRecordReader(
                                     split: InputSplit,
                                     context: TaskAttemptContext
                                   ): RecordReader[Text, SimpleFeature] = {
      val toFeatures = GeoMesaConfigurator.getResultsToFeatures[com.datastax.driver.core.Row](context.getConfiguration)
      val reducer = GeoMesaConfigurator.getReducer(context.getConfiguration)

      val recordReader = new CqlRecordReader
      recordReader.initialize(split.asInstanceOf[InputSplit], context)

      new GeoMesaCassandraRecordReader(toFeatures, reducer, recordReader)
    }

    override def setConf(conf: Configuration): Unit = {

    }

    override def getConf: Configuration = null
  }

  object GeoMesaCassandraInputFormat {

    /**
      * Configure the input format based on a query
      *
      * @param job job to configure
      * @param params data store parameters
      * @param query query
      */
    def configure(job: Job, params: java.util.Map[String, _], query: Query): Unit = {
      // get the query plan to set up the iterators, ranges, etc
      val plan = WithStore[CassandraDataStore](params) { ds =>
        assert(ds != null, "Invalid data store parameters")
        CassandraJobUtils1.getSingleScanPlan(ds, query)
      }
      configure(job, plan)
    }

    /**
      * Configure the input format based on a query plan
      *
      * @param job job to configure
      * @param plan query plan
      */
    def configure(job: Job, plan: StatementPlan): Unit = {
      job.setInputFormatClass(classOf[GeoMesaCassandraInputFormat])
      configure(job.getConfiguration, plan)
    }

    /**
      * Configure the input format based on a query plan
      *
      * @param conf conf
      * @param plan query plan
      */
    def configure(conf: Configuration, plan: StatementPlan): Unit = {
      if (plan.tables.lengthCompare(1) != 0) {
        throw new IllegalArgumentException(s"Query requires multiple tables: ${plan.tables.mkString(", ")}")
      }
      //ConfigHelper.setInputColumnFamily(conf,"gsc_tmo" ,"gsc_raw_5fdrops_z3_geom_msdate_v6" );

      plan.ranges.map { scan =>
        // need to set the table name in each scan
         ConfigHelper.setInputColumnFamily(conf,scan.getKeyspace() ,plan.tables.head);
      }

      GeoMesaConfigurator.setResultsToFeatures(conf, plan.resultsToFeatures)
      // note: reduce and sorting have to be handled in the job reducers
      plan.reducer.foreach(GeoMesaConfigurator.setReducer(conf, _))
      plan.sort.foreach(GeoMesaConfigurator.setSorting(conf, _))
      plan.projection.foreach(GeoMesaConfigurator.setProjection(conf, _))
    }

    /**
      * Record reader for simple features
      *
      * @param toFeatures converts results to features
      * @param reducer feature reducer, if any
      * @param reader underlying cassandra reader
      */
    class GeoMesaCassandraRecordReader(
                                    toFeatures: ResultsToFeatures[com.datastax.driver.core.Row],
                                    reducer: Option[FeatureReducer],
                                    reader: CqlRecordReader
                                  ) extends RecordReader[Text, SimpleFeature] with LazyLogging {

      private val features = {
        val base = new RecordReaderIterator(reader, toFeatures)
        reducer match {
          case None => base
          case Some(reduce) => reduce(base)
        }
      }

      private val key = new Text()
      private var value: SimpleFeature = _

      override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = reader.initialize(split, context)

      override def getProgress: Float = reader.getProgress

      override def nextKeyValue(): Boolean = {
        if (features.hasNext) {
          value = features.next
          key.set(value.getID)
          true
        } else {
          false
        }
      }

      override def getCurrentKey: Text = key

      override def getCurrentValue: SimpleFeature = value

      override def close(): Unit = features.close()
    }

    private class RecordReaderIterator(
                                        reader: CqlRecordReader,
                                        toFeatures: ResultsToFeatures[com.datastax.driver.core.Row]
                                      ) extends CloseableIterator[SimpleFeature] {

      private var staged: SimpleFeature = _

      override def hasNext: Boolean = {
        staged != null || {
          if (reader.nextKeyValue()) {
            staged = toFeatures(reader.getCurrentValue)
            true
          } else {
            false
          }
        }
      }

      override def next(): SimpleFeature = {
        val res = staged
        staged = null
        res
      }

      override def close(): Unit = reader.close()
    }
  }
