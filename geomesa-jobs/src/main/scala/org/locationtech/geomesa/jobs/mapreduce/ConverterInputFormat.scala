/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.jobs.mapreduce

import java.io.{Closeable, InputStream}
import java.util.Locale

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.compress.archivers.ArchiveStreamFactory
import org.apache.commons.compress.archivers.zip.ZipFile
import org.apache.commons.compress.utils.SeekableInMemoryByteChannel
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, Seekable}
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.{CombineFileInputFormat, CombineFileRecordReader, CombineFileRecordReaderWrapper, CombineFileSplit}
import org.geotools.data.ReTypeFeatureReader
import org.geotools.data.simple.DelegateSimpleFeatureReader
import org.geotools.feature.collection.DelegateSimpleFeatureIterator
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.convert2.SimpleFeatureConverter
import org.locationtech.geomesa.jobs.GeoMesaConfigurator
import org.locationtech.geomesa.jobs.mapreduce.ConverterInputFormat.{ConverterKey, RetypeKey}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.fs.{ArchiveFileIterator, ZipFileIterator}
import org.locationtech.geomesa.utils.io.{CloseWithLogging, PathUtils}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
 * Input format for Converters gives us access to the entire file as a byte stream
 * via the record reader.
 */
class ConverterInputFormat extends FileStreamInputFormat {
  override def createRecordReader(): FileStreamRecordReader = new ConverterRecordReader
}

object ConverterInputFormat {

  // note: we can get away with a single instance b/c m/r doesn't end up sharing it
  lazy private [mapreduce] val instance = new ConverterInputFormat

  object Counters {
    val Group     = "org.locationtech.geomesa.jobs.convert"
    val Converted = "converted"
    val Failed    = "failed"
  }

  val ConverterKey  = "org.locationtech.geomesa.jobs.ingest.converter"
  val RetypeKey = "org.locationtech.geomesa.jobs.ingest.retype"

  def setConverterConfig(job: Job, config: String): Unit = setConverterConfig(job.getConfiguration, config)
  def setConverterConfig(conf: Configuration, config: String): Unit = conf.set(ConverterKey, config)

  def setSft(job: Job, sft: SimpleFeatureType): Unit = FileStreamInputFormat.setSft(job, sft)
  def setSft(conf: Configuration, sft: SimpleFeatureType): Unit = FileStreamInputFormat.setSft(conf, sft)

  def setRetypeSft(job: Job, sft: SimpleFeatureType): Unit = setRetypeSft(job.getConfiguration, sft)
  def setRetypeSft(conf: Configuration, sft: SimpleFeatureType): Unit = conf.set(RetypeKey, SimpleFeatureTypes.encodeType(sft))

  def setFilter(job: Job, ecql: String): Unit = setFilter(job.getConfiguration, ecql)
  def setFilter(conf: Configuration, ecql: String): Unit = GeoMesaConfigurator.setFilter(conf, ecql)
}

class ConverterCombineInputFormat extends CombineFileInputFormat[LongWritable, SimpleFeature] {

  override protected def isSplitable(context: JobContext, filename: Path): Boolean = false

  override def createRecordReader(split: InputSplit, context: TaskAttemptContext) =
    new CombineFileRecordReader(split.asInstanceOf[CombineFileSplit], context, classOf[CombineFileStreamRecordReaderWrapper])
}

class CombineFileStreamRecordReaderWrapper(split: CombineFileSplit,
                                           ctx: TaskAttemptContext,
                                           idx: java.lang.Integer)
  extends CombineFileRecordReaderWrapper[LongWritable, SimpleFeature](ConverterInputFormat.instance, split, ctx, idx)

class ConverterRecordReader extends FileStreamRecordReader with LazyLogging {

  import scala.collection.JavaConverters._

  override def createIterator(
      stream: InputStream with Seekable,
      filePath: Path,
      context: TaskAttemptContext): Iterator[SimpleFeature] with Closeable = {

    val confStr   = context.getConfiguration.get(ConverterKey)
    val conf      = ConfigFactory.parseString(confStr)
    val sft       = FileStreamInputFormat.getSft(context.getConfiguration)
    val converter = SimpleFeatureConverter(sft, conf)
    val filter    = GeoMesaConfigurator.getFilter(context.getConfiguration).map(ECQL.toFilter)
    val retypedSpec = context.getConfiguration.get(RetypeKey)

    class MapReduceCounter extends org.locationtech.geomesa.convert.Counter {
      import ConverterInputFormat.{Counters => C}

      // Global counters for the entire job
      override def incSuccess(i: Long): Unit   = context.getCounter(C.Group, C.Converted).increment(i)
      override def getSuccess: Long            = context.getCounter(C.Group, C.Converted).getValue

      override def incFailure(i: Long): Unit   = context.getCounter(C.Group, C.Failed).increment(i)
      override def getFailure: Long            = context.getCounter(C.Group, C.Failed).getValue

      // Line counts are local to file not global
      private var c: Long = 0
      override def incLineCount(i: Long = 1): Unit = c += i
      override def getLineCount: Long              = c
      override def setLineCount(i: Long): Unit     = c = i
    }

    val ec = {
      val params = EvaluationContext.inputFileParam(filePath.toString)
      converter.createEvaluationContext(params, counter = new MapReduceCounter)
    }

    val streams: CloseableIterator[(Option[String], InputStream)] =
      PathUtils.getUncompressedExtension(filePath.getName).toLowerCase(Locale.US) match {
        case ArchiveStreamFactory.TAR =>
          val archive = new ArchiveStreamFactory().createArchiveInputStream(ArchiveStreamFactory.TAR, stream)
          new ArchiveFileIterator(archive, filePath.toString)

        case ArchiveStreamFactory.ZIP | ArchiveStreamFactory.JAR =>
          // we have to read the bytes into memory to get random access reads
          // note: stream is closed in super class
          val bytes = new SeekableInMemoryByteChannel(IOUtils.toByteArray(stream))
          new ZipFileIterator(new ZipFile(bytes), filePath.toString)

        case _ =>
          CloseableIterator.single(None -> stream, stream.close())
      }

    val all = streams.flatMap { case (name, is) =>
      ec.setInputFilePath(name.getOrElse(filePath.toString))
      converter.process(is, ec)
    }
    val iter = filter match {
      case Some(f) => all.filter(f.evaluate)
      case None    => all
    }

    val featureReader = if (retypedSpec != null) {
      val retypedSft = SimpleFeatureTypes.createType(sft.getTypeName, retypedSpec)
      val reader = new DelegateSimpleFeatureReader(sft, new DelegateSimpleFeatureIterator(iter.asJava))
      new ReTypeFeatureReader(reader, retypedSft)
    } else {
      new DelegateSimpleFeatureReader(sft, new DelegateSimpleFeatureIterator(iter.asJava))
    }

    logger.info(s"Initialized record reader on split ${filePath.toString} with " +
      s"type name ${sft.getTypeName} and convert conf $confStr")

    new Iterator[SimpleFeature] with Closeable {
      override def hasNext: Boolean = featureReader.hasNext
      override def next(): SimpleFeature = featureReader.next
      override def close(): Unit = {
        CloseWithLogging(featureReader)
        CloseWithLogging(iter)
        CloseWithLogging(converter)
      }
    }
  }
}
