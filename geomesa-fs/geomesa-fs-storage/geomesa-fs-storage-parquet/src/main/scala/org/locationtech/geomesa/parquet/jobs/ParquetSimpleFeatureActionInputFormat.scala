/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.parquet.jobs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.StorageFileAction.StorageFileAction
import org.locationtech.geomesa.fs.storage.common.jobs.StorageConfiguration
import org.locationtech.geomesa.fs.storage.common.jobs.StorageConfiguration.SimpleFeatureAction
import org.locationtech.geomesa.parquet.jobs.ParquetSimpleFeatureInputFormat.{ParquetSimpleFeatureInputFormatBase, ParquetSimpleFeatureRecordReaderBase, ParquetSimpleFeatureTransformRecordReaderBase}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

/**
  * Input format for parquet files that tracks the action and timestamp associated with each feature
  */
class ParquetSimpleFeatureActionInputFormat extends ParquetSimpleFeatureInputFormatBase[SimpleFeatureAction] {

  override protected def createRecordReader(
      delegate: RecordReader[Void, SimpleFeature],
      conf: Configuration,
      split: FileSplit,
      sft: SimpleFeatureType,
      filter: Option[Filter],
      transform: Option[(String, SimpleFeatureType)]): RecordReader[SimpleFeatureAction, SimpleFeature] = {
    val (timestamp, action) = StorageConfiguration.getPathAction(conf, split.getPath)
    transform match {
      case None => new ParquetSimpleFeatureActionRecordReader(delegate, filter, timestamp, action)
      case Some((tdefs, tsft)) =>
        new ParquetSimpleFeatureActionTransformRecordReader(delegate, filter, sft, tsft, tdefs, timestamp, action)
    }
  }

  class ParquetSimpleFeatureActionRecordReader(
      delegate: RecordReader[Void, SimpleFeature],
      filter: Option[Filter],
      timestamp: Long,
      action: StorageFileAction
    ) extends ParquetSimpleFeatureRecordReaderBase[SimpleFeatureAction](delegate, filter) {
    override def getCurrentKey: SimpleFeatureAction =
      new SimpleFeatureAction(getCurrentValue.getID, timestamp, action)
  }

  class ParquetSimpleFeatureActionTransformRecordReader(
      delegate: RecordReader[Void, SimpleFeature],
      filter: Option[Filter],
      sft: SimpleFeatureType,
      tsft: SimpleFeatureType,
      tdefs: String,
      timestamp: Long,
      action: StorageFileAction
    ) extends ParquetSimpleFeatureTransformRecordReaderBase[SimpleFeatureAction](delegate, filter, sft, tsft, tdefs) {
    override def getCurrentKey: SimpleFeatureAction =
      new SimpleFeatureAction(getCurrentValue.getID, timestamp, action)
  }
}
