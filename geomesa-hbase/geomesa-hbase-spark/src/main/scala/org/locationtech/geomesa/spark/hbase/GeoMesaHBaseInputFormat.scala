/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.hbase

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{MultiTableInputFormat, TableInputFormat}
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce._
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.filter.text.ecql.ECQL
import org.geotools.process.vector.TransformProcess
import org.locationtech.geomesa.hbase.index.HBaseFeatureIndex
import org.locationtech.geomesa.jobs.GeoMesaConfigurator
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.collection.JavaConversions._

/**
  * Input format that allows processing of simple features from GeoMesa based on a CQL query
  */
class GeoMesaHBaseInputFormat extends InputFormat[Text, SimpleFeature] with LazyLogging {

  val delegate = new MultiTableInputFormat

  var sft: SimpleFeatureType = _
  var table: HBaseFeatureIndex = _

  private def init(conf: Configuration) = if (sft == null) {
    sft = GeoMesaConfigurator.getSchema(conf)
    table = HBaseFeatureIndex.index(GeoMesaConfigurator.getIndexIn(conf))
    delegate.setConf(conf)
    // see TableMapReduceUtil.java
    HBaseConfiguration.merge(conf, HBaseConfiguration.create(conf))
    conf.set(TableInputFormat.INPUT_TABLE, GeoMesaConfigurator.getTable(conf))
  }

  /**
    * Gets splits for a job.
    */
  override def getSplits(context: JobContext): java.util.List[InputSplit] = {
    init(context.getConfiguration)
    val splits = delegate.getSplits(context)
    logger.debug(s"Got ${splits.size()} splits")
    splits
  }

  override def createRecordReader(split: InputSplit,
                                  context: TaskAttemptContext): RecordReader[Text, SimpleFeature] = {
    init(context.getConfiguration)
    val rr = delegate.createRecordReader(split, context)
    val transformSchema = GeoMesaConfigurator.getTransformSchema(context.getConfiguration)
    val q = GeoMesaConfigurator.getFilter(context.getConfiguration).map { f => ECQL.toFilter(f) }
    new HBaseGeoMesaRecordReader(sft, table, rr, q, transformSchema)
  }
}

class HBaseGeoMesaRecordReader(sft: SimpleFeatureType,
                               table: HBaseFeatureIndex,
                               reader: RecordReader[ImmutableBytesWritable, Result],
                               filterOpt: Option[Filter],
                               transformSchema: Option[SimpleFeatureType])
    extends RecordReader[Text, SimpleFeature] {

  private var staged: SimpleFeature = _

  private val nextFeature =
    (filterOpt, transformSchema) match {
      case (Some(filter), Some(ts)) =>
        val indices = ts.getAttributeDescriptors.map { ad => sft.indexOf(ad.getLocalName) }
        val fn = table.toFeaturesWithFilterTransform(sft, filter, Array.empty[TransformProcess.Definition], indices.toArray, ts)
        nextFeatureFromOptional(fn)

      case (Some(filter), None) =>
        val fn = table.toFeaturesWithFilter(sft, filter)
        nextFeatureFromOptional(fn)

      case (None, Some(ts))         =>
        val indices = ts.getAttributeDescriptors.map { ad => sft.indexOf(ad.getLocalName) }
        val fn = table.toFeaturesWithTransform(sft, Array.empty[TransformProcess.Definition], indices.toArray, ts)
        nextFeatureFromDirect(fn)

      case (None, None)         =>
        val fn = table.toFeaturesDirect(sft)
        nextFeatureFromDirect(fn)
    }

  private val getId = table.getIdFromRow(sft)

  override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = reader.initialize(split, context)

  override def getProgress: Float = reader.getProgress

  override def nextKeyValue(): Boolean = nextKeyValueInternal()

  override def getCurrentValue: SimpleFeature = staged

  override def getCurrentKey = new Text(staged.getID)

  override def close(): Unit = reader.close()

  /**
    * Get the next key value from the underlying reader, incrementing the reader when required
    */
  private def nextKeyValueInternal(): Boolean = {
    nextFeature()
    if (staged != null) {
      val row = reader.getCurrentKey
      val offset = row.getOffset
      val length = row.getLength
      staged.getIdentifier.asInstanceOf[FeatureIdImpl].setID(getId(row.get(), offset, length))
      true
    } else {
      false
    }
  }

  private def nextFeatureFromOptional(toFeature: Result => Option[SimpleFeature]) = () => {
    staged = null
    while (reader.nextKeyValue() && staged == null) {
      toFeature(reader.getCurrentValue) match {
        case Some(feature) => staged = feature
        case None => staged = null
      }
    }
  }

  private def nextFeatureFromDirect(toFeature: Result => SimpleFeature) = () => {
    staged = null
    while (reader.nextKeyValue() && staged == null) {
      staged = toFeature(reader.getCurrentValue)
    }
  }
}