/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.orc.jobs

import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, FileSplit}
import org.apache.orc.mapred.OrcStruct
import org.apache.orc.mapreduce.OrcMapreduceRecordReader
import org.apache.orc.{OrcConf, OrcFile}
import org.geotools.data.Query
import org.locationtech.geomesa.features.{ScalaSimpleFeature, TransformSimpleFeature}
import org.locationtech.geomesa.fs.storage.common.jobs.StorageConfiguration
import org.locationtech.geomesa.fs.storage.orc.OrcFileSystemReader
import org.locationtech.geomesa.fs.storage.orc.OrcFileSystemReader.OrcReadOptions
import org.locationtech.geomesa.fs.storage.orc.jobs.OrcSimpleFeatureInputFormat.{OrcSimpleFeatureInputFormatBase, OrcSimpleFeatureRecordReaderBase}
import org.locationtech.geomesa.fs.storage.orc.utils.OrcInputFormatReader
import org.locationtech.geomesa.index.planning.QueryRunner
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

/**
  * Input format for orc files
  */
class OrcSimpleFeatureInputFormat extends OrcSimpleFeatureInputFormatBase[Void] {

  override protected def createRecordReader(
      delegate: RecordReader[NullWritable, OrcStruct],
      split: FileSplit,
      conf: Configuration,
      sft: SimpleFeatureType,
      filter: Option[Filter],
      transform: Option[(String, SimpleFeatureType)],
      columns: Option[Set[Int]]): RecordReader[Void, SimpleFeature] = {
    new OrcSimpleFeatureRecordReader(delegate, sft, filter, transform, columns)
  }

  class OrcSimpleFeatureRecordReader(
      delegate: RecordReader[NullWritable, OrcStruct],
      sft: SimpleFeatureType,
      filter: Option[Filter],
      transform: Option[(String, SimpleFeatureType)],
      columns: Option[Set[Int]]
    ) extends OrcSimpleFeatureRecordReaderBase[Void](delegate, sft, filter, transform, columns) {
    override def getCurrentKey: Void = null
  }
}

object OrcSimpleFeatureInputFormat {

  /**
    * Configure an input format
    *
    * @param conf hadoop configuration
    * @param sft simple feature type being read
    * @param filter ECQL filter
    * @param transforms result transform
    */
  def configure(
      conf: Configuration,
      sft: SimpleFeatureType,
      filter: Filter,
      transforms: Array[String] = null): Unit = {
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints

    StorageConfiguration.setSft(conf, sft)
    val q = QueryRunner.configureDefaultQuery(sft, new Query(sft.getTypeName, filter, transforms))
    Option(q.getFilter).filter(_ != Filter.INCLUDE).foreach(StorageConfiguration.setFilter(conf, _))
    q.getHints.getTransform.foreach(StorageConfiguration.setTransforms(conf, _))
    // replicates orc input format strategy of always recursively listing directories
    conf.set(FileInputFormat.INPUT_DIR_RECURSIVE, "true")
  }

  /**
    * Abstract base class for reading Orc simple features without a key.
    *
    * This class is based on OrcInputFormat, but sets UTC reader options (which aren't exposed in OrcInputFormat)
    * and doesn't use kryo serialization for configuration (which conflicts with spark kryo versions)
    *
    * @tparam T key type
    */
  abstract class OrcSimpleFeatureInputFormatBase[T] extends FileInputFormat[T, SimpleFeature] {

    override def createRecordReader(
        inputSplit: InputSplit,
        context: TaskAttemptContext): RecordReader[T, SimpleFeature] = {
      val split = inputSplit.asInstanceOf[FileSplit]
      val conf = context.getConfiguration

      val reader = {
        val maxLength = OrcConf.MAX_FILE_LENGTH.getLong(conf)
        val opts = OrcFile.readerOptions(conf).maxLength(maxLength).useUTCTimestamp(true)
        OrcFile.createReader(split.getPath, opts)
      }

      val options =
        reader.options()
            .range(split.getStart, split.getLength)
            .useZeroCopy(OrcConf.USE_ZEROCOPY.getBoolean(conf))
            .skipCorruptRecords(OrcConf.SKIP_CORRUPT_DATA.getBoolean(conf))
            .tolerateMissingSchema(OrcConf.TOLERATE_MISSING_SCHEMA.getBoolean(conf))

      val sft = StorageConfiguration.getSft(conf)
      val filter = StorageConfiguration.getFilter(conf, sft)
      val transform = StorageConfiguration.getTransforms(conf)

      val OrcReadOptions(columns, pushDown) = OrcFileSystemReader.readOptions(sft, filter, transform)
      columns.foreach(cols => options.include(OrcFileSystemReader.include(sft, cols)))
      pushDown.foreach { case (sargs, names) => options.searchArgument(sargs, names) }

      val delegate = new OrcMapreduceRecordReader[OrcStruct](reader, options)

      createRecordReader(delegate, split, conf, sft, filter, transform, columns)
    }

    override protected def listStatus(job: JobContext): util.List[FileStatus] = {
      val complete = super.listStatus(job)
      val result = new java.util.ArrayList[FileStatus](complete.size)
      val iter = complete.iterator
      while (iter.hasNext) {
        val stat = iter.next
        if (stat.getLen != 0) {
          result.add(stat)
        }
      }
      result
    }

    protected def createRecordReader(
        delegate: RecordReader[NullWritable, OrcStruct],
        split: FileSplit,
        conf: Configuration,
        sft: SimpleFeatureType,
        filter: Option[Filter],
        transform: Option[(String, SimpleFeatureType)],
        columns: Option[Set[Int]]): RecordReader[T, SimpleFeature]
  }

  /**
    * Abstract base class for reading orc simple feature records, without a key
    *
    * @param delegate primitive orc record reader
    * @param sft simple feature type
    * @param filter cql filter
    * @param transform relational transform
    * @param columns read columns
    * @tparam T key type
    */
  abstract class OrcSimpleFeatureRecordReaderBase[T](
      delegate: RecordReader[NullWritable, OrcStruct],
      sft: SimpleFeatureType,
      filter: Option[Filter],
      transform: Option[(String, SimpleFeatureType)],
      columns: Option[Set[Int]]
    ) extends RecordReader[T, SimpleFeature] {

    private val feature = new ScalaSimpleFeature(sft, "")

    private val setAttributes = OrcInputFormatReader(sft, columns)

    private val current: SimpleFeature = transform match {
      case Some((tdefs, tsft)) => TransformSimpleFeature(sft, tsft, tdefs).setFeature(feature)
      case None => feature
    }

    override def initialize(split: InputSplit, context: TaskAttemptContext): Unit =
      delegate.initialize(split, context)

    override def nextKeyValue(): Boolean = {
      while (delegate.nextKeyValue()) {
        setAttributes(delegate.getCurrentValue, feature)
        if (filter.forall(_.evaluate(feature))) {
          return true
        }
      }
      false
    }

    override def getCurrentValue: SimpleFeature = current

    override def getProgress: Float = delegate.getProgress

    override def close(): Unit = delegate.close()
  }
}
