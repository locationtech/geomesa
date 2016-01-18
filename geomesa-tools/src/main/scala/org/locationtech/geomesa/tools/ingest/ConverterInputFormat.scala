/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.ingest

import java.io.InputStream

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{FileSystem, Path, Seekable}
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.compress.{CodecPool, CompressionCodecFactory, Decompressor}
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, FileSplit}
import org.locationtech.geomesa.convert.Transformers.EvaluationContext
import org.locationtech.geomesa.convert.{SimpleFeatureConverter, SimpleFeatureConverters}
import org.locationtech.geomesa.tools.ingest.ConverterInputFormat._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
 * Input format for Converters gives us access to the entire file as a byte stream
 * via the record reader. In the future we can use this to implement the processing
 * of InputStreams from the Converter API so that we don't have to load the entire
 * file into memory (esp for multi-line xml, json, avro).
 */
class ConverterInputFormat extends FileInputFormat[LongWritable, SimpleFeature] with LazyLogging {
  override protected def isSplitable(context: JobContext, filename: Path): Boolean = false

  override def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[LongWritable, SimpleFeature] = {
    new ConverterRecordReader()
  }
}

object ConverterInputFormat {

  object Counters {
    val Group  = "org.locationtech.geomesa.jobs.convert"
    val Success = "success"
    val Failure = "failure"
    val Written = "written"
  }

  val ConverterKey  = "org.locationtech.geomesa.jobs.ingest.converter"
  val SftKey        = "org.locationtech.geomesa.jobs.ingest.sft"
  val TypeNameKey   = "org.locationtech.geomesa.jobs.ingest.sft.name"

  def setConverterConfig(job: Job, config: String) = {
    job.getConfiguration.set(ConverterKey, config)
  }

  def setSft(job: Job, sft: SimpleFeatureType) = {
    job.getConfiguration.set(SftKey, SimpleFeatureTypes.encodeType(sft))
    job.getConfiguration.set(TypeNameKey, sft.getTypeName)
  }

}

class ConverterRecordReader() extends RecordReader[LongWritable, SimpleFeature] with LazyLogging {

  private var dec: Decompressor = null
  private var ec:  EvaluationContext = null
  private var converter: SimpleFeatureConverter[String] = null
  private var itr: Iterator[SimpleFeature] = null
  private var stream: InputStream with Seekable = null
  private var length: Float = 0

  private val curKey = new LongWritable(0)
  private var curValue: SimpleFeature = null

  override def getProgress: Float = {
    if (length == 0) {
      0.0f
    } else {
      math.min(1.0f, stream.getPos / length)
    }
  }

  override def nextKeyValue(): Boolean = {
    if (itr.hasNext) {
      curKey.set(ec.counter.getSuccess)
      curValue = itr.next()
      true
    } else {
      false
    }
  }

  override def getCurrentValue: SimpleFeature = curValue

  override def initialize(inputSplit: InputSplit, context: TaskAttemptContext): Unit = {
    val split     = inputSplit.asInstanceOf[FileSplit]
    val job       = context.getConfiguration
    val confStr   = job.get(ConverterKey)
    val conf      = ConfigFactory.parseString(confStr)
    val typeName  = job.get(TypeNameKey)
    val sft       = SimpleFeatureTypes.createType(typeName, context.getConfiguration.get(SftKey))
    converter     = SimpleFeatureConverters.build(sft, conf)

    val file = split.getPath
    val codec = new CompressionCodecFactory(job).getCodec(file)
    val fs: FileSystem = file.getFileSystem(job)
    length = split.getLength.toFloat
    stream =
      if (codec != null) {
        dec = CodecPool.getDecompressor(codec)
        codec.createInputStream(fs.open(file), dec)
      } else {
        fs.open(file)
      }

    class MapReduceCounter extends org.locationtech.geomesa.convert.Transformers.Counter {
      import ConverterInputFormat.{Counters => C}

      // Global counters for the entire job
      override def incSuccess(i: Long): Unit   = context.getCounter(C.Group, C.Success).increment(i)
      override def getSuccess: Long            = context.getCounter(C.Group, C.Success).getValue

      override def incFailure(i: Long): Unit   = context.getCounter(C.Group, C.Failure).increment(i)
      override def getFailure: Long            = context.getCounter(C.Group, C.Failure).getValue

      // Line counts are local to file not global
      private var c: Long = 0
      override def incLineCount(i: Long = 1)   = c += i
      override def getLineCount: Long          = c
      override def setLineCount(i: Long)       = c = i
    }

    ec = converter.createEvaluationContext(Map("inputFilePath" -> split.getPath.toString), new MapReduceCounter)
    itr = converter.process(stream, ec)

    logger.info(s"Initialized record reader on split ${split.getPath.toString} with type name $typeName and convert conf $confStr")
  }

  override def getCurrentKey: LongWritable = curKey

  override def close(): Unit = {
    IOUtils.closeQuietly(converter)
    IOUtils.closeQuietly(stream)
    if (dec != null) {
      CodecPool.returnDecompressor(dec)
    }
  }
}