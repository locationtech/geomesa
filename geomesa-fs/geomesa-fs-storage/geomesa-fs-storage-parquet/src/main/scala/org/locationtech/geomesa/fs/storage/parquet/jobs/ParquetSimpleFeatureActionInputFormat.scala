/***********************************************************************
<<<<<<< HEAD
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
=======
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 1463162d60 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9f430502b2 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> dce8c58b44 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> b727e40f7c (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 3515f7f054 (GEOMESA-3254 Add Bloop build support)
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.parquet.jobs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.StorageFileAction.StorageFileAction
import org.locationtech.geomesa.fs.storage.common.jobs.StorageConfiguration
import org.locationtech.geomesa.fs.storage.common.jobs.StorageConfiguration.SimpleFeatureAction
import org.locationtech.geomesa.fs.storage.parquet.jobs.ParquetSimpleFeatureInputFormat.{ParquetSimpleFeatureInputFormatBase, ParquetSimpleFeatureRecordReaderBase, ParquetSimpleFeatureTransformRecordReaderBase}

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
