/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.parquet.jobs

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import org.apache.parquet.hadoop.ParquetInputFormat
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.parquet.jobs.ParquetSimpleFeatureInputFormat.GTFilteringRR
import org.opengis.feature.simple.SimpleFeature

class ParquetSimpleFeatureInputFormat extends ParquetInputFormat[SimpleFeature] {
  /**
    * {@inheritDoc }
    */
  @throws[IOException]
  @throws[InterruptedException]
  override def createRecordReader(inputSplit: InputSplit, taskAttemptContext: TaskAttemptContext): RecordReader[Void, SimpleFeature] =
    new GTFilteringRR(super.createRecordReader(inputSplit, taskAttemptContext))

}

object ParquetSimpleFeatureInputFormat {

  val GeoToolsFilterKey = "geomesa.fs.residual.filter"

  def setGeoToolsFilter(conf: Configuration, filter: org.opengis.filter.Filter): Unit =
    conf.set(GeoToolsFilterKey, ECQL.toCQL(filter))
  def getGeoToolsFilter(conf: Configuration): org.opengis.filter.Filter = ECQL.toFilter(conf.get(GeoToolsFilterKey))

  class GTFilteringRR(rr: RecordReader[Void, SimpleFeature]) extends RecordReader[Void, SimpleFeature] {

    private var cur: SimpleFeature = _
    private var filter: org.opengis.filter.Filter = _

    override def getProgress: Float = rr.getProgress

    override def nextKeyValue(): Boolean = {
      cur = null
      while (cur == null && rr.nextKeyValue()) {
        val next = rr.getCurrentValue
        if (filter.evaluate(next)) {
          cur = next
        }
      }
      cur != null
    }

    override def getCurrentValue: SimpleFeature = cur

    override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {
      rr.initialize(split, context)
      filter = ParquetSimpleFeatureInputFormat.getGeoToolsFilter(context.getConfiguration)
    }

    override def getCurrentKey: Void = null

    override def close(): Unit = rr.close()
  }
}
