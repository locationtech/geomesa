/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.orc.jobs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.orc.mapred.OrcStruct
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.StorageFileAction.StorageFileAction
import org.locationtech.geomesa.fs.storage.common.jobs.StorageConfiguration
import org.locationtech.geomesa.fs.storage.common.jobs.StorageConfiguration.SimpleFeatureAction
import org.locationtech.geomesa.fs.storage.orc.jobs.OrcSimpleFeatureInputFormat.{OrcSimpleFeatureInputFormatBase, OrcSimpleFeatureRecordReaderBase}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

/**
  * Input format for orc files that tracks the action and timestamp associated with each feature
  */
class OrcSimpleFeatureActionInputFormat extends OrcSimpleFeatureInputFormatBase[SimpleFeatureAction] {

  override protected def createRecordReader(
      delegate: RecordReader[NullWritable, OrcStruct],
      split: FileSplit,
      conf: Configuration,
      sft: SimpleFeatureType,
      filter: Option[Filter],
      transform: Option[(String, SimpleFeatureType)],
      columns: Option[Set[Int]]): RecordReader[SimpleFeatureAction, SimpleFeature] = {
    val (timestamp, action) = StorageConfiguration.getPathAction(conf, split.getPath)
    new OrcSimpleFeatureActionRecordReader(delegate, sft, filter, transform, columns, timestamp, action)
  }

  class OrcSimpleFeatureActionRecordReader(
      delegate: RecordReader[NullWritable, OrcStruct],
      sft: SimpleFeatureType,
      filter: Option[Filter],
      transform: Option[(String, SimpleFeatureType)],
      columns: Option[Set[Int]],
      timestamp: Long,
      action: StorageFileAction
    ) extends OrcSimpleFeatureRecordReaderBase[SimpleFeatureAction](delegate, sft, filter, transform, columns) {
    override def getCurrentKey: SimpleFeatureAction =
      new SimpleFeatureAction(getCurrentValue.getID, timestamp, action)
  }
}
