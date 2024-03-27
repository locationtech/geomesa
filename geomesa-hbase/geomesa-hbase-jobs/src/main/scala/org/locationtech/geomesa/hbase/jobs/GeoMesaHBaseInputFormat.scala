/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.jobs

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{MultiTableInputFormat, TableInputFormat, TableMapReduceUtil}
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce._
import org.geotools.api.data.Query
import org.geotools.api.feature.simple.SimpleFeature
import org.locationtech.geomesa.hbase.data.HBaseQueryPlan.ScanPlan
import org.locationtech.geomesa.hbase.data.{HBaseConnectionPool, HBaseDataStore}
import org.locationtech.geomesa.hbase.jobs.GeoMesaHBaseInputFormat.GeoMesaHBaseRecordReader
import org.locationtech.geomesa.index.api.QueryPlan.{FeatureReducer, ResultsToFeatures}
import org.locationtech.geomesa.jobs.GeoMesaConfigurator
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.WithStore

/**
  * Input format that allows processing of simple features from GeoMesa based on a CQL query
  */
class GeoMesaHBaseInputFormat extends InputFormat[Text, SimpleFeature] with Configurable with LazyLogging {

  private val delegate = new MultiTableInputFormat

  /**
    * Gets splits for a job.
    */
  override def getSplits(context: JobContext): java.util.List[InputSplit] = {
    val splits = Security.doAuthorized(context.getConfiguration) {
      delegate.getSplits(context)
    }
    logger.debug(s"Got ${splits.size()} splits")
    splits
  }

  override def createRecordReader(
      split: InputSplit,
      context: TaskAttemptContext
    ): RecordReader[Text, SimpleFeature] = {
    val toFeatures = GeoMesaConfigurator.getResultsToFeatures[Result](context.getConfiguration)
    val reducer = GeoMesaConfigurator.getReducer(context.getConfiguration)
    Security.doAuthorized(context.getConfiguration) {
      new GeoMesaHBaseRecordReader(toFeatures, reducer, delegate.createRecordReader(split, context))
    }
  }

  override def setConf(conf: Configuration): Unit = {
    delegate.setConf(conf)
    // configurations aren't thread safe - if multiple input formats are configured at once,
    // updating it could cause ConcurrentModificationExceptions
    conf.synchronized {
      // see TableMapReduceUtil.java
      HBaseConfiguration.merge(conf, HBaseConfiguration.create(conf))
      HBaseConnectionPool.configureSecurity(conf)
    }
  }

  override def getConf: Configuration = delegate.getConf
}

object GeoMesaHBaseInputFormat {

  /**
    * Configure the input format based on a query
    *
    * @param job job to configure
    * @param params data store parameters
    * @param query query
    */
  def configure(job: Job, params: java.util.Map[String, _], query: Query): Unit = {
    // get the query plan to set up the iterators, ranges, etc
    val plan = WithStore[HBaseDataStore](params) { ds =>
      assert(ds != null, "Invalid data store parameters")
      HBaseJobUtils.getSingleScanPlan(ds, query)
    }
    configure(job, plan)
  }

  /**
    * Configure the input format based on a query plan
    *
    * @param job job to configure
    * @param plan query plan
    */
  def configure(job: Job, plan: ScanPlan): Unit = {
    job.setInputFormatClass(classOf[GeoMesaHBaseInputFormat])
    configure(job.getConfiguration, plan)
  }

  /**
    * Configure the input format based on a query plan
    *
    * @param conf conf
    * @param plan query plan
    */
  def configure(conf: Configuration, plan: ScanPlan): Unit = {
    if (plan.scans.lengthCompare(1) != 0) {
      throw new IllegalArgumentException(s"Query requires multiple tables: ${plan.scans.map(_.table).mkString(", ")}")
    }
    conf.set(TableInputFormat.INPUT_TABLE, plan.scans.head.table.getNameAsString)
    // note: secondary filter is handled by scan push-down filter
    val scans = plan.scans.head.scans.map { scan =>
      // need to set the table name in each scan
      scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, plan.scans.head.table.getName)
      TableMapReduceUtil.convertScanToString(scan)
    }
    conf.setStrings(MultiTableInputFormat.SCANS, scans: _*)

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
    * @param reader underlying hbase reader
    */
  class GeoMesaHBaseRecordReader(
      toFeatures: ResultsToFeatures[Result],
      reducer: Option[FeatureReducer],
      reader: RecordReader[ImmutableBytesWritable, Result]
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
      reader: RecordReader[ImmutableBytesWritable, Result],
      toFeatures: ResultsToFeatures[Result]
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
