/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.export

import java.io.File
import java.util.UUID

import com.typesafe.scalalogging.{LazyLogging, StrictLogging}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{Text, WritableComparable}
import org.apache.hadoop.mapred.InvalidJobConfException
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.partition.InputSampler.{RandomSampler, Sampler, SplitSampler}
import org.apache.hadoop.mapreduce.lib.partition.{InputSampler, TotalOrderPartitioner}
import org.apache.hadoop.mapreduce.security.TokenCache
import org.geotools.data.DataUtilities
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.geoserver.ViewParams
import org.locationtech.geomesa.index.index.attribute.AttributeIndexKey
import org.locationtech.geomesa.jobs.GeoMesaConfigurator
import org.locationtech.geomesa.jobs.mapreduce.JobWithLibJars
import org.locationtech.geomesa.tools.export.ExportCommand.{ExportOptions, Exporter, FileSizeEstimator}
import org.locationtech.geomesa.tools.export.formats.{ExportFormat, FeatureExporter}
import org.locationtech.geomesa.utils.index.ByteArrays
import org.locationtech.geomesa.utils.io.{IncrementingFileName, PathUtils}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
 * Class that handles configuration and tracking of the remote job
 */
object ExportJob extends JobWithLibJars {

  def configure(
      job: Job,
      dsParams: Map[String, String],
      sft: SimpleFeatureType,
      hints: Hints,
      filename: String,
      output: Path,
      format: ExportFormat,
      headers: Boolean,
      chunks: Option[Long],
      gzip: Option[Int],
      reducers: Int,
      libjarsFiles: Seq[String],
      libjarsPaths: Iterator[() => Seq[File]]): Job = {

    val conf = job.getConfiguration

    setLibJars(job, libjarsFiles, libjarsPaths)
    conf.set("mapreduce.job.user.classpath.first", "true")

    job.setJarByClass(getClass)
    job.setMapperClass(classOf[PassThroughMapper])
    job.setOutputFormatClass(classOf[ExportOutputFormat])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[ScalaSimpleFeature])

    FileOutputFormat.setOutputPath(job, output)
    Config.setOutputFile(conf, filename)
    Config.setFormat(conf, format)
    Config.setHeaders(conf, headers)
    chunks.foreach(Config.setChunks(conf, _))
    gzip.foreach(Config.setGzip(conf, _))
    Config.setQueryHints(conf, hints)

    // note: we assume that the input format has set sorting and reduce

    if (conf.get(GeoMesaConfigurator.Keys.FeatureReducer) != null) {
      throw new IllegalArgumentException("Query requires a feature reduce step, which is not " +
          "supported in this job")
    }

    GeoMesaConfigurator.getSorting(conf) match {
      case None => job.setNumReduceTasks(0)
      case Some(sort) =>
        require(reducers > 0, "Reducers must be a positive number")
        job.setNumReduceTasks(reducers)
        job.setReducerClass(classOf[Reducer[Text, SimpleFeature, Text, SimpleFeature]])

        job.setMapperClass(classOf[SortKeyMapper]) // override the default mapper we configured earlier

        val sortBy = sort.map(_._1)
        val reverse = sort.head._2
        if (sortBy.exists(sft.indexOf(_) == -1)) {
          throw new IllegalArgumentException(s"Trying to sort by non-existing property: ${sortBy.mkString(", ")}")
        } else if (sort.exists(_._2 != reverse)) {
          throw new IllegalArgumentException(s"Sort order is required to be the same across all sort properties: " +
              QueryHints.sortReadableString(sort))
        }

        if (reverse) {
          job.setSortComparatorClass(classOf[ReverseComparator])
        }

        // configure a global sort, as by default sort is not maintained between multiple reducers
        // if there is only 1 reducer, we don't need a global sort as each reducer is already sorted
        if (reducers > 1) {
          TotalOrderPartitioner.setPartitionFile(conf,
            new Path(FileOutputFormat.getOutputPath(job), s"${UUID.randomUUID()}.partitions"))
          // calculate the global partitioning using our custom sampler
          InputSampler.writePartitionFile(job, new FallbackSampler(0.01, 1000, 100))
          job.setPartitionerClass(classOf[TotalOrderPartitioner[Text, SimpleFeature]])
        }
    }

    job.setMapSpeculativeExecution(false)
    job.setReduceSpeculativeExecution(false)
    // Ensure that the reducers don't start too early
    // (default is at 0.05 which takes all the map slots and isn't needed)
    conf.set("mapreduce.job.reduce.slowstart.completedmaps", ".90")

    job
  }

  object Counters {
    val Group   = "org.locationtech.geomesa.jobs.export"
    val Loaded  = "loaded"
    val Written = "written"

    def count(job: Job): Long = job.getCounters.findCounter(Group, Written).getValue
    def mapping(job: Job): Seq[(String, Long)] = Seq(Loaded -> job.getCounters.findCounter(Group, Loaded).getValue)
    def reducing(job: Job): Seq[(String, Long)] = Seq(Written -> count(job))
  }

  object Config {

    val FileNameKey  = "geomesa.export.filename"
    val HintsKey     = "geomesa.export.hints"
    val ChunksKey    = "geomesa.export.chunks"
    val FormatKey    = "geomesa.export.format"
    val GzipKey      = "geomesa.export.gzip"
    val HeadersKey   = "geomesa.export.headers"

    def setOutputFile(conf: Configuration, file: String): Unit = conf.set(FileNameKey, file)
    def getOutputFile(conf: Configuration): String = conf.get(FileNameKey)

    def setQueryHints(conf: Configuration, hints: Hints): Unit = conf.set(HintsKey, ViewParams.serialize(hints))
    def getQueryHints(conf: Configuration): Hints =
      Option(conf.get(HintsKey)).map(ViewParams.deserialize).getOrElse(new Hints)

    def setFormat(conf: Configuration, format: ExportFormat): Unit = conf.set(FormatKey, format.name)
    def getFormat(conf: Configuration): ExportFormat = ExportFormat(conf.get(FormatKey)).getOrElse {
      throw new IllegalArgumentException(s"Unknown export format: ${conf.get(FormatKey)}")
    }

    def setChunks(conf: Configuration, chunks: Long): Unit = conf.set(ChunksKey, chunks.toString)
    def getChunks(conf: Configuration): Option[Long] = Option(conf.get(ChunksKey)).map(_.toLong)

    def setGzip(conf: Configuration, compression: Int): Unit = conf.set(GzipKey, compression.toString)
    def getGzip(conf: Configuration): Option[Int] = Option(conf.get(GzipKey)).map(_.toInt)

    def setHeaders(conf: Configuration, headers: Boolean): Unit = conf.set(HeadersKey, headers.toString)
    def getHeaders(conf: Configuration): Boolean = conf.get(HeadersKey).toBoolean
  }

  /**
    * Sampler implementation for global sorting reducer splits.
    *
    * The random sampler can fail to return any data if the input set is small, which then causes
    * the TotalOrderPartitioner to fail.
    *
    * This class tries to use the random sampler, but falls back to first n if needed.
    */
  class FallbackSampler(frequency: Double, numSamples: Int, maxSplitsToSample: Int) extends Sampler[AnyRef, AnyRef] {

    private val random = new RandomSampler[AnyRef, AnyRef](frequency, numSamples, maxSplitsToSample)
    private val split = new SplitSampler[AnyRef, AnyRef](numSamples, maxSplitsToSample)

    override def getSample(inf: InputFormat[AnyRef, AnyRef], job: Job): Array[AnyRef] = {
      val result = random.getSample(inf, job)
      if (result.nonEmpty) { result } else {
        split.getSample(inf, job)
      }
    }
  }

  /**
    * Takes the input and writes it to the output
    */
  class PassThroughMapper extends Mapper[Text, SimpleFeature, Text, SimpleFeature] with StrictLogging {

    type Context = Mapper[Text, SimpleFeature, Text, SimpleFeature]#Context

    private var counter: Counter = _

    override protected def setup(context: Context): Unit =
      counter = context.getCounter(Counters.Group, Counters.Loaded)

    override protected def map(key: Text, sf: SimpleFeature, context: Context): Unit = {
      logger.debug(s"map key ${key.toString}, map value ${DataUtilities.encodeFeature(sf)}")
      context.write(key, sf)
      counter.increment(1L)
    }
  }

  /**
    * Takes the input and writes it to the output, with a key suitable for sorting
    */
  class SortKeyMapper extends Mapper[Text, SimpleFeature, Text, SimpleFeature] with StrictLogging {

    type Context = Mapper[Text, SimpleFeature, Text, SimpleFeature]#Context

    private val text = new Text
    private val builder = new StringBuilder()

    private var sortFields: Seq[Int] = _
    private var counter: Counter = _

    override protected def setup(context: Context): Unit = {
      val sft = GeoMesaConfigurator.getResultsToFeatures(context.getConfiguration).schema
      val sorting = GeoMesaConfigurator.getSorting(context.getConfiguration).getOrElse {
        throw new IllegalStateException("No sorting defined in configuration")
      }
      sortFields = sorting.map { case (f, _) => sft.indexOf(f) }
      counter = context.getCounter(Counters.Group, Counters.Loaded)
    }

    override protected def map(key: Text, sf: SimpleFeature, context: Context): Unit = {
      logger.debug(s"map key ${key.toString}, map value ${DataUtilities.encodeFeature(sf)}")
      builder.clear()
      sortFields.foreach { i =>
        val attribute = sf.getAttribute(i)
        val encoded = if (attribute == null) { "" } else { AttributeIndexKey.typeEncode(attribute) }
        builder.append(encoded).append(ByteArrays.ZeroByte)
      }
      text.set(builder.toString)
      context.write(text, sf)
      counter.increment(1L)
    }
  }

  /**
    * Output format for export files
    */
  class ExportOutputFormat extends FileOutputFormat[Text, SimpleFeature] with LazyLogging {

    // duplicated from FileOutputFormat, but doesn't care if the directory exists
    override def checkOutputSpecs(job: JobContext): Unit = {
      val outDir = FileOutputFormat.getOutputPath(job)
      if (outDir == null) {
        throw new InvalidJobConfException("Output directory not set")
      }
      // get delegation token for outDir's file system
      TokenCache.obtainTokensForNamenodes(job.getCredentials, Array(outDir), job.getConfiguration)
    }

    override def getRecordWriter(job: TaskAttemptContext): RecordWriter[Text, SimpleFeature] = {
      val conf = job.getConfiguration
      val file = {
        val (base, extension) = PathUtils.getBaseNameAndExtension(Config.getOutputFile(conf))
        conf.set(FileOutputFormat.BASE_OUTPUT_NAME, base) // controls result from getDefaultWorkFile
        getDefaultWorkFile(job, extension).toString
      }
      val opts = ExportOptions(Config.getFormat(conf), Some(file), Config.getGzip(conf), Config.getHeaders(conf))
      val hints = Config.getQueryHints(conf)
      lazy val names = new IncrementingFileName(file)
      Config.getChunks(conf) match {
        case None => new ExportRecordWriter(job, opts, hints)
        case Some(c) if opts.format.countable => new ExportChunkedRecordWriter(job, names, opts, hints, c)
        case Some(c) => new ExportUncountableChunkedRecordWriter(job, names, opts, hints, c)
      }
    }
  }

  /**
    * Record writer that wraps a FeatureExporter
    *
    * @param context task context
    * @param opts options
    * @param hints query hints
    */
  class ExportRecordWriter(context: TaskAttemptContext, opts: ExportOptions, hints: Hints)
      extends RecordWriter[Text, SimpleFeature] {

    private val counter = context.getCounter(Counters.Group, Counters.Written)

    private var exporter: FeatureExporter = _

    override def write(key: Text, value: SimpleFeature): Unit = {
      if (exporter == null) {
        exporter = new Exporter(opts, hints, Map.empty)
        exporter.start(value.getFeatureType)
      }
      exporter.export(Iterator.single(value))
      counter.increment(1L) // since aggregation is disabled, this will always be 1 to 1 with features
    }

    override def close(context: TaskAttemptContext): Unit = if (exporter != null) { exporter.close() }
  }

  /**
    * Record writer that wraps a chunked countable FeatureExporter. Because the exporter is countable,
    * the bytes written should be fairly accurate after every feature
    *
    * @param context task context
    * @param files source for file names
    * @param opts options
    * @param hints query hints
    * @param chunks chunk size, in bytes
    */
  class ExportChunkedRecordWriter(
      context: TaskAttemptContext,
      files: Iterator[String],
      opts: ExportOptions,
      hints: Hints,
      chunks: Long
    ) extends RecordWriter[Text, SimpleFeature] with LazyLogging {

    private val counter = context.getCounter(Counters.Group, Counters.Written)

    private var exporter: FeatureExporter = _
    private var estimator: FileSizeEstimator = _

    override def write(key: Text, value: SimpleFeature): Unit = {
      if (exporter == null) {
        exporter = new Exporter(opts.copy(file = Some(files.next)), hints, Map.empty)
        exporter.start(value.getFeatureType)
        if (estimator == null) {
          val bytesPerFeature = opts.format.bytesPerFeature(value.getFeatureType)
          estimator = new FileSizeEstimator(chunks, 0.01f, bytesPerFeature) // 1% error threshold
        }
      }

      exporter.export(Iterator.single(value))
      counter.increment(1L) // since aggregation is disabled, this will always be 1 to 1 with features

      // bytes on a streaming exporter should be cheap to check
      if (estimator.done(exporter.bytes)) {
        exporter.close()
        exporter = null
      }
    }

    override def close(context: TaskAttemptContext): Unit = if (exporter != null) { exporter.close() }
  }

  /**
    * Record writer that wraps a chunked non-countable FeatureExporter. Because the exporter is not
    * countable, the bytes written will generally only be accurate after the exporter is closed
    *
    * @param context task context
    * @param files source for file names
    * @param opts options
    * @param hints query hints
    * @param chunks chunk size, in bytes
    */
  class ExportUncountableChunkedRecordWriter(
      context: TaskAttemptContext,
      files: Iterator[String],
      opts: ExportOptions,
      hints: Hints,
      chunks: Long
    ) extends RecordWriter[Text, SimpleFeature] with LazyLogging {

    private val counter = context.getCounter(Counters.Group, Counters.Written)

    private var exporter: FeatureExporter = _
    private var estimator: FileSizeEstimator = _
    private var estimate = 0L // estimated number of features to write to hit our chunk size
    private var count = 0 // current number of features written since the last estimate

    override def write(key: Text, value: SimpleFeature): Unit = {
      if (exporter == null) {
        exporter = new Exporter(opts.copy(file = Some(files.next)), hints, Map.empty)
        exporter.start(value.getFeatureType)
        if (estimator == null) {
          val bytesPerFeature = opts.format.bytesPerFeature(value.getFeatureType)
          estimator = new FileSizeEstimator(chunks, 0.05f, bytesPerFeature) // 5% error threshold
        }
        estimate = estimator.estimate(0L)
      }

      exporter.export(Iterator.single(value))
      counter.increment(1L) // since aggregation is disabled, this will always be 1 to 1 with features
      count += 1

      if (count >= estimate) {
        // since the bytes aren't generally available until after closing the writer,
        // we have to go with our initial estimate and adjust after the first chunk
        exporter.close()
        // update the estimator so that it's more accurate on the next run
        estimator.update(exporter.bytes, count)
        exporter = null
        count = 0
      }
    }

    override def close(context: TaskAttemptContext): Unit = if (exporter != null) { exporter.close() }
  }

  /**
    * Used for reverse (descending) sorting
    */
  class ReverseComparator extends org.apache.hadoop.io.Text.Comparator {
    override def compare(b1: Array[Byte], s1: Int, l1: Int, b2: Array[Byte], s2: Int, l2: Int): Int =
      super.compare(b1, s1, l1, b2, s2, l2) * -1
    override def compare(o1: WritableComparable[_], o2: WritableComparable[_]): Int = super.compare(o1, o2) * -1
  }
}
