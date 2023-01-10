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
>>>>>>> b727e40f7c (GEOMESA-3254 Add Bloop build support)
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.parquet.jobs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, FileSplit}
import org.apache.hadoop.mapreduce.{InputSplit, JobContext, RecordReader, TaskAttemptContext}
import org.apache.parquet.hadoop.ParquetInputFormat
import org.geotools.api.data.Query
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.features.TransformSimpleFeature
import org.locationtech.geomesa.fs.storage.common.jobs.StorageConfiguration
import org.locationtech.geomesa.fs.storage.parquet.io.SimpleFeatureReadSupport
import org.locationtech.geomesa.fs.storage.parquet.jobs.ParquetSimpleFeatureInputFormat.{ParquetSimpleFeatureInputFormatBase, ParquetSimpleFeatureRecordReaderBase, ParquetSimpleFeatureTransformRecordReaderBase}
import org.locationtech.geomesa.fs.storage.parquet.{ReadFilter, ReadSchema}
import org.locationtech.geomesa.index.planning.QueryRunner

/**
  * Input format for parquet files
  */
class ParquetSimpleFeatureInputFormat extends ParquetSimpleFeatureInputFormatBase[Void] {

  override protected def createRecordReader(
      delegate: RecordReader[Void, SimpleFeature],
      conf: Configuration,
      split: FileSplit,
      sft: SimpleFeatureType,
      filter: Option[Filter],
      transform: Option[(String, SimpleFeatureType)]): RecordReader[Void, SimpleFeature] = {
    transform match {
      case None => new ParquetSimpleFeatureRecordReader(delegate, filter)
      case Some((tdefs, tsft)) => new ParquetSimpleFeatureTransformRecordReader(delegate, filter, sft, tsft, tdefs)
    }
  }

  class ParquetSimpleFeatureRecordReader(delegate: RecordReader[Void, SimpleFeature], filter: Option[Filter])
      extends ParquetSimpleFeatureRecordReaderBase[Void](delegate, filter) {
    override def getCurrentKey: Void = null
  }

  class ParquetSimpleFeatureTransformRecordReader(
      delegate: RecordReader[Void, SimpleFeature],
      filter: Option[Filter],
      sft: SimpleFeatureType,
      tsft: SimpleFeatureType,
      tdefs: String
    ) extends ParquetSimpleFeatureTransformRecordReaderBase[Void](delegate, filter, sft, tsft, tdefs) {
    override def getCurrentKey: Void = null
  }
}

object ParquetSimpleFeatureInputFormat {

  import org.locationtech.geomesa.index.conf.QueryHints._

  /**
    * Configure the input format
    *
    * @param conf conf
    * @param sft simple feature type
    * @param query query
    */
  def configure(conf: Configuration, sft: SimpleFeatureType, query: Query): Unit = {
    val q = QueryRunner.configureDefaultQuery(sft, query)
    val filter = Option(q.getFilter).filter(_ != Filter.INCLUDE)

    // Parquet read schema and final transform
    val ReadSchema(readSft, readTransform) = ReadSchema(sft, filter, q.getHints.getTransform)
    // push-down Parquet predicates and remaining gt-filter
    val ReadFilter(parquetFilter, residualFilter) = ReadFilter(readSft, filter)

    // set the parquet push-down filter
    parquetFilter.foreach(ParquetInputFormat.setFilterPredicate(conf, _))

    // set our residual filters and transforms
    StorageConfiguration.setSft(conf, readSft)
    residualFilter.foreach(StorageConfiguration.setFilter(conf, _))
    readTransform.foreach(StorageConfiguration.setTransforms(conf, _))

    // need this for query planning
    conf.set("parquet.filter.dictionary.enabled", "true")

    // @see org.apache.parquet.hadoop.ParquetInputFormat.setReadSupportClass(org.apache.hadoop.mapred.JobConf, java.lang.Class<?>)
    conf.set(ParquetInputFormat.READ_SUPPORT_CLASS, classOf[SimpleFeatureReadSupport].getName)

    // replicates parquet input format strategy of always recursively listing directories
    conf.set(FileInputFormat.INPUT_DIR_RECURSIVE, "true")
  }

  abstract class ParquetSimpleFeatureInputFormatBase[T] extends FileInputFormat[T, SimpleFeature] {

    private val delegate = new ParquetInputFormat[SimpleFeature]()

    override def createRecordReader(
        split: InputSplit,
        context: TaskAttemptContext): RecordReader[T, SimpleFeature] = {
      val rr = delegate.createRecordReader(split, context)

      val conf = context.getConfiguration
      val sft = StorageConfiguration.getSft(conf)
      val filter = StorageConfiguration.getFilter(conf, sft)
      val transform = StorageConfiguration.getTransforms(conf)

      createRecordReader(rr, conf, split.asInstanceOf[FileSplit], sft, filter, transform)
    }

    override def getSplits(context: JobContext): java.util.List[InputSplit] = delegate.getSplits(context)

    override protected def isSplitable(context: JobContext, filename: Path): Boolean =
      context.getConfiguration.getBoolean(ParquetInputFormat.SPLIT_FILES, true)

    protected def createRecordReader(
        delegate: RecordReader[Void, SimpleFeature],
        conf: Configuration,
        split: FileSplit,
        sft: SimpleFeatureType,
        filter: Option[Filter],
        transform: Option[(String, SimpleFeatureType)]): RecordReader[T, SimpleFeature]
  }

  abstract class ParquetSimpleFeatureRecordReaderBase[T](
      delegate: RecordReader[Void, SimpleFeature],
      filter: Option[Filter]
    ) extends RecordReader[T, SimpleFeature] {

    private var current: SimpleFeature = _

    override def initialize(split: InputSplit, context: TaskAttemptContext): Unit =
      delegate.initialize(split, context)

    override def getProgress: Float = delegate.getProgress

    override def nextKeyValue(): Boolean = {
      while (delegate.nextKeyValue()) {
        current = delegate.getCurrentValue
        if (filter.forall(_.evaluate(current))) {
           return true
        }
      }
      false
    }

    override def getCurrentValue: SimpleFeature = current

    override def close(): Unit = delegate.close()
  }

  abstract class ParquetSimpleFeatureTransformRecordReaderBase[T](
      delegate: RecordReader[Void, SimpleFeature],
      filter: Option[Filter],
      sft: SimpleFeatureType,
      tsft: SimpleFeatureType,
      tdefs: String
    ) extends ParquetSimpleFeatureRecordReaderBase[T](delegate, filter) {

    private val current = TransformSimpleFeature(sft, tsft, tdefs)

    override def getCurrentValue: SimpleFeature = {
      current.setFeature(super.getCurrentValue)
      current
    }
  }
}
