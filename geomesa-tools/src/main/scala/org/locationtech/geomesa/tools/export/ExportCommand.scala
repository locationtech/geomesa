/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.export

import java.io._
import java.util.Collections
import java.util.zip.GZIPOutputStream

import com.beust.jcommander.validators.PositiveInteger
import com.beust.jcommander.{Parameter, ParameterException}
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileContext, Path}
import org.apache.hadoop.mapreduce.Job
import org.geotools.data.{DataStore, FileDataStore, Query}
import org.geotools.filter.text.ecql.ECQL
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.geoserver.ViewParams
import org.locationtech.geomesa.index.iterators.BinAggregatingScan
import org.locationtech.geomesa.index.planning.QueryRunner
import org.locationtech.geomesa.jobs.GeoMesaConfigurator
import org.locationtech.geomesa.tools.DistributedRunParam.RunModes
import org.locationtech.geomesa.tools._
import org.locationtech.geomesa.tools.export.ExportCommand.{ChunkedExporter, ExportOptions, ExportParams, Exporter}
import org.locationtech.geomesa.tools.export.formats.FeatureExporter.OutputStreamCounter
import org.locationtech.geomesa.tools.export.formats.FileSystemExporter.{OrcFileSystemExporter, ParquetFileSystemExporter}
import org.locationtech.geomesa.tools.export.formats._
import org.locationtech.geomesa.tools.utils.ParameterConverters.{BytesConverter, ExportFormatConverter}
import org.locationtech.geomesa.tools.utils.{JobRunner, NoopParameterSplitter, Prompt, StatusCallback}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.fs.FileSystemDelegate.CreateMode
import org.locationtech.geomesa.utils.io.{IncrementingFileName, PathUtils, WithClose}
import org.locationtech.geomesa.utils.stats.MethodProfiling
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter
import org.opengis.filter.sort.SortOrder

import scala.annotation.tailrec
import scala.util.control.NonFatal

trait ExportCommand[DS <: DataStore] extends DataStoreCommand[DS]
    with DistributedCommand with InteractiveCommand with MethodProfiling {

  override val name = "export"
  override def params: ExportParams

  override def libjarsFiles: Seq[String] = Seq("org/locationtech/geomesa/tools/export-libjars.list")

  override def execute(): Unit = {
    def complete(fileAndCount: (String, Option[Long]), time: Long): Unit = {
      val (file, countOption) = fileAndCount
      val count = countOption.map(c => s" for $c features").getOrElse("")
      Command.user.info(s"Feature export complete to $file in ${time}ms$count")
    }

    profile(complete _)(withDataStore(export))
  }

  private def export(ds: DS): (String, Option[Long]) = {
    // for file data stores, handle setting the default type name so the user doesn't have to
    for {
      p <- Option(params).collect { case p: ProvidedTypeNameParam => p }
      f <- Option(ds).collect { case f: FileDataStore => f }
    } { p.featureName = f.getSchema.getTypeName }

    val options = ExportOptions(params)
    val remote = options.file.exists(PathUtils.isRemote)
    val reducers = Option(params.reducers).map(_.intValue()).getOrElse(-1)
    val mode = params.mode.getOrElse(if (reducers == -1) { RunModes.Local } else { RunModes.Distributed })
    val chunks = Option(params.chunkSize).map(_.longValue())

    // do some validation up front
    if (options.file.isEmpty && !options.format.streaming) {
      throw new ParameterException(s"Format '${options.format}' requires file-based output, please use --output")
    } else if (remote && options.format == ExportFormat.Shp) {
      throw new ParameterException("Shape file export is not supported for distributed file systems")
    } else if (!remote && mode == RunModes.Distributed) {
      throw new ParameterException("Distributed export requires an output file in a distributed file system")
    } else if (mode == RunModes.Distributed && params.maxFeatures != null) {
      throw new ParameterException("Distributed export does not support --max-features")
    }

    val sft = ds.getSchema(params.featureName)
    if (sft == null) {
      throw new ParameterException(s"Schema '${params.featureName}' does not exist in the store")
    }

    val query = ExportCommand.createQuery(sft, params)

    mode match {
      case RunModes.Local =>
        options.file.map(PathUtils.getHandle).foreach { file =>
          if (file.exists) {
            if (params.force) {
              Command.user.warn(s"Output file '${file.path}' exists - deleting it")
            } else if (!Prompt.confirm(s"WARNING Output file '${file.path}' exists, delete it and continue (y/n)? ")) {
              throw new ParameterException(s"Output file '${file.path}' exists")
            }
            file.delete()
          }
        }
        lazy val dictionaries = ArrowExporter.queryDictionaries(ds, query)
        val exporter = chunks match {
          case None    => new Exporter(options, query.getHints, dictionaries)
          case Some(c) => new ChunkedExporter(options, query.getHints, dictionaries, c)
        }
        val count = try { export(ds, query, exporter) } finally { exporter.close() }
        val outFile = options.file match {
          case None => "standard out"
          case Some(f) if chunks.isDefined => PathUtils.getBaseNameAndExtension(f).productIterator.mkString("_*")
          case Some(f) => f
        }
        (outFile, count)

      case RunModes.Distributed =>
        val job = Job.getInstance(new Configuration, "GeoMesa Tools Export")

        // for remote jobs, don't push down format transforms, to enable counting and global sorting
        val hints = new Hints(query.getHints)
        ExportCommand.disableAggregation(sft, query.getHints)

        configure(job, ds, query) // note: do this first so that input format is set for the TotalOrderPartitioner

        // note: these confs should be set by the input format
        val reduce = Seq(GeoMesaConfigurator.Keys.FeatureReducer, GeoMesaConfigurator.Keys.Sorting).filter { key =>
          job.getConfiguration.get(key) != null
        }

        if (reducers < 1 && reduce.nonEmpty) {
          if (reduce.contains(GeoMesaConfigurator.Keys.Sorting)) {
            throw new ParameterException("Distributed export sorting requires --num-reducers")
          } else {
            throw new ParameterException(s"Distributed export format '${options.format}' requires --num-reducers")
          }
        }

        // must be some due to our remote check
        val file = options.file.getOrElse(throw new IllegalStateException("file should be Some"))
        val output = new Path(PathUtils.getUrl(file).toURI).getParent

        // delete the output dir before configuring the job, as it may write the partition file there
        val context = FileContext.getFileContext(output.toUri, job.getConfiguration)
        if (context.util.exists(output)) {
          if (params.force) {
            Command.user.warn(s"Output directory '$output' exists - deleting it")
          } else if (!Prompt.confirm(s"WARNING Output directory '$output' exists, delete it and continue (y/n)? ")) {
            throw new ParameterException(s"Output directory '$output' exists")
          }
          context.delete(output, true)
        }

        val filename = FilenameUtils.getName(file)

        // note: use our original hints, which have the aggregating keys
        ExportJob.configure(job, connection, sft, hints, filename, output, options.format, options.headers,
          chunks, options.gzip, reducers, libjars(options.format), libjarsPaths)

        JobRunner.run(job, StatusCallback(), ExportJob.Counters.mapping(job), ExportJob.Counters.reducing(job))

        (output.toString, Some(ExportJob.Counters.count(job)))

      case _ => throw new NotImplementedError() // someone added a run mode and didn't implement it here...
    }
  }

  protected def export(ds: DS, query: Query, exporter: FeatureExporter): Option[Long] = {
    try {
      Command.user.info("Running export - please wait...")
      val features = ds.getFeatureSource(query.getTypeName).getFeatures(query)
      exporter.start(features.getSchema)
      WithClose(CloseableIterator(features.features()))(exporter.export)
    } catch {
      case NonFatal(e) =>
        throw new RuntimeException("Could not execute export query. Please ensure " +
            "that all arguments are correct", e)
    }
  }

  /**
    * Configure an export job, with the appropriate input format for this particular data store. Must be
    * overridden to provide distributed export support
    *
    * @return
    */
  protected def configure(job: Job, ds: DS, query: Query): Unit =
    throw new ParameterException("Distributed export is not supported by this store, please use --run-mode local")

  /**
    * Get the list of libjars files for a given format
    *
    * @param format export format
    * @return
    */
  private def libjars(format: ExportFormat): Seq[String] = {
    val path = s"org/locationtech/geomesa/tools/export-libjars-$format.list"
    Seq(path).filter(getClass.getClassLoader.getResource(_) != null) ++ libjarsFiles
  }
}

object ExportCommand extends LazyLogging {

  import org.locationtech.geomesa.index.conf.QueryHints.RichHints
  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  import scala.collection.JavaConverters._

  /**
    * Create the query to execute
    *
    * @param sft simple feature type
    * @param params parameters
    * @return
    */
  def createQuery(sft: SimpleFeatureType, params: ExportParams): Query = {
    val typeName = Option(params).collect { case p: TypeNameParam => p.featureName }.orNull
    val filter = Option(params.cqlFilter).getOrElse(Filter.INCLUDE)

    val query = new Query(typeName, filter)
    val hints = query.getHints
    Option(params.maxFeatures).map(Int.unbox).foreach(query.setMaxFeatures)
    Option(params).collect { case p: OptionalIndexParam => p }.foreach { p =>
      Option(p.index).foreach { index =>
        logger.debug(s"Using index $index")
        hints.put(QueryHints.QUERY_INDEX, index)
      }
    }

    Option(params.hints).foreach { hintStrings =>
      hints.put(Hints.VIRTUAL_TABLE_PARAMETERS, hintStrings)
      ViewParams.setHints(query)
    }

    if (params.outputFormat == ExportFormat.Arrow) {
      hints.put(QueryHints.ARROW_ENCODE, java.lang.Boolean.TRUE)
    } else if (params.outputFormat == ExportFormat.Bin) {
      // if not specified in hints, set it here to trigger the bin query
      if (!hints.containsKey(QueryHints.BIN_TRACK)) {
        hints.put(QueryHints.BIN_TRACK, "id")
      }
      if (!hints.containsKey(QueryHints.BIN_DTG)) {
        sft.getDtgField.foreach(hints.put(QueryHints.BIN_DTG, _))
      }
    } else if (params.outputFormat == ExportFormat.Leaflet) {
      if (!LeafletMapExporter.configure(params)) {
        throw new ParameterException("Terminating execution")
      }
    }

    val attributes: Array[String] = {
      val combined = params.attributes.asScala ++ params.transforms.asScala
      if (combined.nonEmpty) {
        val (id, attributes) = combined.partition(_.equalsIgnoreCase("id"))
        if (id.isEmpty && !hints.containsKey(QueryHints.ARROW_INCLUDE_FID)) {
          // note: we also use this hint for delimited text export
          hints.put(QueryHints.ARROW_INCLUDE_FID, java.lang.Boolean.FALSE)
        }
        attributes.toArray
      } else if (params.outputFormat == ExportFormat.Bin) {
        BinAggregatingScan.propertyNames(hints, sft).toArray
      } else {
        null // all props
      }
    }
    query.setPropertyNames(attributes)

    if (!params.sortFields.isEmpty) {
      val fields = params.sortFields.asScala
      if (fields.exists(a => sft.indexOf(a) == -1)) {
        val errors = fields.filter(a => sft.indexOf(a) == -1)
        throw new ParameterException(s"Invalid sort attribute${if (errors.lengthCompare(1) == 0) "" else "s"}: " +
            errors.mkString(", "))
      }
      val order = if (params.sortDescending) { SortOrder.DESCENDING } else { SortOrder.ASCENDING }
      query.setSortBy(fields.map(f => org.locationtech.geomesa.filter.ff.sort(f, order)).toArray)
    } else if (hints.isArrowQuery) {
      hints.getArrowSort.foreach { case (f, r) =>
        val order = if (r) { SortOrder.DESCENDING } else { SortOrder.ASCENDING }
        query.setSortBy(Array(org.locationtech.geomesa.filter.ff.sort(f, order)))
      }
    }

    logger.debug(s"Applying CQL filter ${ECQL.toCQL(filter)}")
    logger.debug(s"Applying transform ${Option(attributes).map(_.mkString(",")).orNull}")

    QueryRunner.configureDefaultQuery(sft, query)
  }

  /**
    * Disable hints for aggregating scans by removing them, and moving any aggregating sort hints to
    * regular sort hints
    *
    * @param sft simple feature type
    * @param hints hints
    * @return
    */
  def disableAggregation(sft: SimpleFeatureType, hints: Hints): Unit = {
    if (hints.isArrowQuery) {
      hints.remove(QueryHints.ARROW_ENCODE)
      val sort = hints.remove(QueryHints.ARROW_SORT_FIELD).asInstanceOf[String]
      if (sort != null) {
        val reverse = Option(hints.remove(QueryHints.ARROW_SORT_REVERSE).asInstanceOf[java.lang.Boolean])
        val order = if (reverse.exists(_.booleanValue)) { SortOrder.DESCENDING } else { SortOrder.ASCENDING }
        val hint = org.locationtech.geomesa.filter.ff.sort(sort, order)
        hints.put(QueryHints.Internal.SORT_FIELDS, QueryHints.Internal.toSortHint(Array(hint)))
      }
    } else if (hints.isBinQuery) {
      hints.remove(QueryHints.BIN_TRACK)
      if (hints.isBinSorting) {
        hints.getBinDtgField.orElse(sft.getDtgField).foreach { dtg =>
          val hint = org.locationtech.geomesa.filter.ff.sort(dtg, SortOrder.ASCENDING)
          hints.put(QueryHints.Internal.SORT_FIELDS, QueryHints.Internal.toSortHint(Array(hint)))
        }
      }
    }
  }

  /**
    * Options from the input params, in a more convenient format
    *
    * @param format output format
    * @param file file path (or stdout)
    * @param gzip compression
    * @param headers headers (for delimited text only)
    */
  case class ExportOptions(format: ExportFormat, file: Option[String], gzip: Option[Int], headers: Boolean)

  object ExportOptions {
    def apply(params: ExportParams): ExportOptions =
      ExportOptions(params.outputFormat, Option(params.file), Option(params.gzip).map(_.intValue), !params.noHeader)
  }

  /**
    * Single exporter that handles the command options and delegates to the correct implementation
    *
    * @param options options
    * @param hints query hints
    * @param dictionaries lazily evaluated arrow dictionaries
    */
  class Exporter(options: ExportOptions, hints: Hints, dictionaries: => Map[String, Array[AnyRef]])
      extends FeatureExporter {

    private val name = options.file.orNull

    // lowest level - keep track of the bytes we write
    // do this before any compression, buffering, etc so we get an accurate count
    private lazy val counter = {
      val base = if (name == null) { System.out } else {
        PathUtils.getHandle(name).write(CreateMode.Create, createParents = true)
      }
      new OutputStreamCounter(base)
    }

    private lazy val stream = {
      val base = counter.stream
      // disable compressing the output stream for avro, as it's handled by the avro writer
      val compressed = options.gzip.filter(_ => options.format != ExportFormat.Avro) match {
        case None => base
        case Some(c) => new GZIPOutputStream(base) { `def`.setLevel(c) } // hack to access the protected deflate level
      }
      new BufferedOutputStream(compressed)
    }

    // noinspection ComparingUnrelatedTypes
    private lazy val fids = !Option(hints.get(QueryHints.ARROW_INCLUDE_FID)).contains(java.lang.Boolean.FALSE)

    private val exporter = options.format match {
      case ExportFormat.Arrow   => new ArrowExporter(stream, counter, hints, dictionaries)
      case ExportFormat.Avro    => new AvroExporter(stream, counter, options.gzip)
      case ExportFormat.Bin     => new BinExporter(stream, counter, hints)
      case ExportFormat.Csv     => DelimitedExporter.csv(stream, counter, options.headers, fids)
      case ExportFormat.Gml2    => GmlExporter.gml2(stream, counter)
      case ExportFormat.Gml3    => GmlExporter(stream, counter)
      case ExportFormat.Json    => new GeoJsonExporter(stream, counter)
      case ExportFormat.Leaflet => new LeafletMapExporter(stream, counter)
      case ExportFormat.Null    => NullExporter
      case ExportFormat.Orc     => new OrcFileSystemExporter(name)
      case ExportFormat.Parquet => new ParquetFileSystemExporter(name)
      case ExportFormat.Shp     => new ShapefileExporter(new File(name))
      case ExportFormat.Tsv     => DelimitedExporter.tsv(stream, counter, options.headers, fids)
      // shouldn't happen unless someone adds a new format and doesn't implement it here
      case _ => throw new NotImplementedError(s"Export for '${options.format}' is not implemented")
    }

    override def start(sft: SimpleFeatureType): Unit = exporter.start(sft)

    override def export(features: Iterator[SimpleFeature]): Option[Long] = exporter.export(features)

    override def bytes: Long = exporter.bytes

    override def close(): Unit = exporter.close()
  }

  /**
    * Feature exporter that handles chunking output into multiple files
    *
    * @param options export options
    * @param hints query hints
    * @param dictionaries arrow dictionaries (lazily evaluated)
    * @param chunks number of bytes to write per file
    */
  class ChunkedExporter(
      options: ExportOptions,
      hints: Hints,
      dictionaries: => Map[String, Array[AnyRef]],
      chunks: Long
    ) extends FeatureExporter with LazyLogging {

    private val names = options.file match {
      case None    => Iterator.continually(None)
      case Some(f) => new IncrementingFileName(f).map(Option.apply)
    }

    private lazy val queriedDictionaries = dictionaries // only evaluate once, even if we have multiple chunks

    private var sft: SimpleFeatureType = _
    private var exporter: FeatureExporter = _
    private var estimator: FileSizeEstimator = _
    private var count = 0L // number of features written
    private var total = 0L // sum size of all finished chunks

    override def start(sft: SimpleFeatureType): Unit = {
      this.sft = sft
      estimator = new FileSizeEstimator(chunks, 0.05f, options.format.bytesPerFeature(sft)) // 5% error threshold
      nextChunk()
    }

    override def export(features: Iterator[SimpleFeature]): Option[Long] = export(features, None)

    override def bytes: Long = if (exporter == null) { total } else { total + exporter.bytes }

    override def close(): Unit = if (exporter != null) { exporter.close() }

    private def nextChunk(): Unit = {
      if (exporter != null) {
        exporter.close()
        // adjust our estimate to account for the actual bytes written
        // do this after closing the exporter to account for footers, batching, etc
        val written = exporter.bytes
        estimator.update(written, count)
        total += written
      }
      exporter = new Exporter(options.copy(file = names.next), hints, queriedDictionaries)
      exporter.start(sft)
      count = 0
    }

    @tailrec
    private def export(features: Iterator[SimpleFeature], result: Option[Long]): Option[Long] = {
      val counter = features.take(estimator.estimate(exporter.bytes)).map { f => count += 1; f }
      val exported = exporter.export(counter) match {
        case None    => result
        case Some(c) => result.map(_ + c).orElse(Some(c))
      }
      if (features.isEmpty) {
        exported
      } else {
        // if it's a countable format, the bytes should be available now and we can compare to our chunk size
        // otherwise, the bytes aren't generally available until after closing the writer,
        // so we have to go with our initial estimate and adjust after the first chunk
        if (options.format.countable) {
          val bytes = exporter.bytes
          if (estimator.done(bytes)) {
            nextChunk()
          } else {
            estimator.update(bytes, count)
          }
        } else {
          nextChunk()
        }
        export(features, exported)
      }
    }
  }

  /**
    * Estimates how many features to write to create a file of a target size
    *
    * @param target target file size, in bytes
    * @param error acceptable percent error for file size, in bytes
    * @param estimatedBytesPerFeature initial estimate for bytes per feature
    */
  class FileSizeEstimator(target: Long, error: Float, estimatedBytesPerFeature: Float) extends LazyLogging {

    require(error >= 0 && error < 1f, "Error must be a percentage between [0,1)")

    private val threshold = math.round(target * error.toDouble)
    private var estimate = estimatedBytesPerFeature.toDouble

    /**
      * Estimate how many features to write to hit our target size
      *
      * @param written number of bytes written so far
      * @return
      */
    def estimate(written: Long): Int = {
      val e = math.round((target - written) / estimate)
      if (e < 1) { 1 } else if (e.isValidInt) { e.intValue() } else { Int.MaxValue}
    }

    /**
      * Re-evaluate the bytes per feature, based on having written out a certain number of features
      *
      * @param size size of the file created
      * @param count number of features written to the file
      */
    def update(size: Long, count: Long): Unit = {
      if (size > 0 && count > 0 && math.abs(size - target) > threshold) {
        val update = size.toDouble / count
        logger.debug(s"Updating bytesPerFeature from $estimate to $update based on writing $count features in $size bytes")
        estimate = update
      } else {
        logger.debug(s"Not updating bytesPerFeature from $estimate based on writing $count features in $size bytes")
      }
    }

    /**
      * Checks if the bytes written is (at least) within the error threshold of the desired size
      *
      * @param size size of the file created
      * @return
      */
    def done(size: Long): Boolean = size > target || math.abs(size - target) < threshold
  }

  /**
    * Export parameters
    */
  trait ExportParams extends OptionalCqlFilterParam
      with QueryHintsParams with DistributedRunParam with TypeNameParam with OptionalForceParam {
    @Parameter(names = Array("-o", "--output"), description = "Output to a file instead of std out")
    var file: String = _

    @Parameter(names = Array("--gzip"), description = "Level of gzip compression to apply to output, from 1-9")
    var gzip: Integer = _

    @Parameter(
      names = Array("--no-header"),
      description = "Export as a delimited text format (csv|tsv) without a type header")
    var noHeader: Boolean = false

    @Parameter(
      names = Array("-m", "--max-features"),
      description = "Restrict the maximum number of features returned")
    var maxFeatures: java.lang.Integer = _

    @Parameter(
      names = Array("--attribute"),
      description = "Attributes or derived expressions to export, or 'id' to include the feature ID",
      splitter = classOf[NoopParameterSplitter])
    var transforms: java.util.List[String] = Collections.emptyList()

    @Parameter(
      names = Array("-a", "--attributes"),
      description = "Comma-separated attributes to export, or 'id' to include the feature ID")
    var attributes: java.util.List[String] = Collections.emptyList()

    @Parameter(names = Array("--sort-by"), description = "Sort by the specified attributes (comma-delimited)")
    var sortFields: java.util.List[String] = Collections.emptyList()

    @Parameter(
      names = Array("--sort-descending"),
      description = "Sort in descending order, instead of ascending",
      arity = 0)
    var sortDescending: Boolean = false

    @Parameter(
      names = Array("--num-reducers"),
      description = "Number of reducers to use when sorting or merging (for distributed export)",
      validateWith = classOf[PositiveInteger])
    var reducers: java.lang.Integer = _

    @Parameter(
      names = Array("--chunk-size"),
      description = "Split the output into multiple files, by specifying the rough number of bytes to store per file",
      converter = classOf[BytesConverter])
    var chunkSize: java.lang.Long = _

    @Parameter(
      names = Array("-F", "--output-format"),
      description = "File format of output files (csv|tsv|gml|json|shp|avro|leaflet|orc|parquet|arrow)",
      converter = classOf[ExportFormatConverter])
    var explicitOutputFormat: ExportFormat = _

    lazy val outputFormat: ExportFormat = {
      if (explicitOutputFormat != null) { explicitOutputFormat } else {
        Option(file).flatMap(f => ExportFormat(PathUtils.getUncompressedExtension(f))).getOrElse(ExportFormat.Csv)
      }
    }
  }
}
