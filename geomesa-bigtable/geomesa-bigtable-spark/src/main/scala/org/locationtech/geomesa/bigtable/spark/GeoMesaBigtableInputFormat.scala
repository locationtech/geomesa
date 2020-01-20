/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.bigtable.spark

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce._
import org.locationtech.geomesa.hbase.jobs.GeoMesaHBaseInputFormat.GeoMesaHBaseRecordReader
import org.locationtech.geomesa.jobs.GeoMesaConfigurator
import org.opengis.feature.simple.SimpleFeature

class GeoMesaBigtableInputFormat extends InputFormat[Text, SimpleFeature] with Configurable with LazyLogging {

  private val delegate = new BigtableInputFormat

  private var conf: Configuration = _

  override def getSplits(context: JobContext): java.util.List[InputSplit] = {
    val splits = delegate.getSplits(context)
    logger.debug(s"Got ${splits.size()} splits")
    splits
  }

  override def createRecordReader(
      split: InputSplit,
      context: TaskAttemptContext): RecordReader[Text, SimpleFeature] = {
    val toFeatures = GeoMesaConfigurator.getResultsToFeatures[Result](context.getConfiguration)
    val reducer = GeoMesaConfigurator.getReducer(context.getConfiguration)
    new GeoMesaHBaseRecordReader(toFeatures, reducer, delegate.createRecordReader(split, context))
  }

  override def setConf(conf: Configuration): Unit = {
    this.conf = conf
    delegate.setConf(conf)
    // see TableMapReduceUtil.java
    HBaseConfiguration.merge(conf, HBaseConfiguration.create(conf))
  }

  override def getConf: Configuration = conf
}
