/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
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
import org.apache.hadoop.hbase.mapreduce.{MultiTableInputFormat, TableInputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Base64
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce._
import org.geotools.data.Query
import org.locationtech.geomesa.hbase.data.HBaseQueryPlan.ScanPlan
import org.locationtech.geomesa.hbase.data.{HBaseConnectionPool, HBaseDataStore}
import org.locationtech.geomesa.hbase.jobs.GeoMesaHBaseInputFormat.GeoMesaHBaseRecordReader
import org.locationtech.geomesa.index.api.QueryPlan.ResultsToFeatures
import org.locationtech.geomesa.jobs.GeoMesaConfigurator
import org.locationtech.geomesa.utils.io.WithStore
import org.opengis.feature.simple.SimpleFeature

/**
  * Input format that allows processing of simple features from GeoMesa based on a CQL query
  */
class GeoMesaHBaseInputFormat extends InputFormat[Text, SimpleFeature] with Configurable with LazyLogging {

  private val delegate = new MultiTableInputFormat

  /**
    * Gets splits for a job.
    */
  override def getSplits(context: JobContext): java.util.List[InputSplit] = {
    val splits = delegate.getSplits(context)
    logger.debug(s"Got ${splits.size()} splits")
    splits
  }

  override def createRecordReader(
      split: InputSplit,
      context: TaskAttemptContext): RecordReader[Text, SimpleFeature] = {
    val toFeatures = GeoMesaConfigurator.getResultsToFeatures[Result](context.getConfiguration)
    val rr = delegate.createRecordReader(split, context)
    // TODO GEOMESA-2300 support local filtering
    new GeoMesaHBaseRecordReader(toFeatures, rr)
  }

  override def setConf(conf: Configuration): Unit = {
    delegate.setConf(conf)
    // see TableMapReduceUtil.java
    HBaseConfiguration.merge(conf, HBaseConfiguration.create(conf))
    HBaseConnectionPool.configureSecurity(conf)
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
    if (plan.tables.lengthCompare(1) != 0) {
      throw new IllegalArgumentException(s"Query requires multiple tables: ${plan.tables.mkString(", ")}")
    }
    conf.set(TableInputFormat.INPUT_TABLE, plan.tables.head.getNameAsString)
    // note: secondary filter is handled by scan push-down filter
    val scans = plan.scans.map { scan =>
      // need to set the table name in each scan
      scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, plan.tables.head.getName)
      Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray)
    }
    conf.setStrings(MultiTableInputFormat.SCANS, scans: _*)

    GeoMesaConfigurator.setResultsToFeatures(conf, plan.resultsToFeatures)
    // note: reduce and sorting have to be handled in the job reducers
    plan.reducer.foreach(GeoMesaConfigurator.setReducer(conf, _))
    plan.sort.foreach(GeoMesaConfigurator.setSorting(conf, _))
    plan.projection.foreach(GeoMesaConfigurator.setProjection(conf, _))
  }

  class GeoMesaHBaseRecordReader(
      toFeatures: ResultsToFeatures[Result],
      reader: RecordReader[ImmutableBytesWritable, Result]
    ) extends RecordReader[Text, SimpleFeature] with LazyLogging {

    private val key = new Text()
    private var staged: SimpleFeature = _

    override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = reader.initialize(split, context)

    override def getProgress: Float = reader.getProgress

    override def nextKeyValue(): Boolean = {
      if (reader.nextKeyValue()) {
        staged = toFeatures(reader.getCurrentValue)
        key.set(staged.getID)
        true
      } else {
        false
      }
    }

    override def getCurrentValue: SimpleFeature = staged

    override def getCurrentKey: Text = key

    override def close(): Unit = reader.close()
  }
}
