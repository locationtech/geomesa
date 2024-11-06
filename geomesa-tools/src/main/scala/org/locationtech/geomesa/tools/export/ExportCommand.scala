/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.`export`

import com.beust.jcommander.{Parameter, ParameterException}
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.FilenameUtils
import org.apache.commons.io.output.CountingOutputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileContext, Path}
import org.apache.hadoop.mapreduce.Job
import org.geotools.api.data.{DataStore, FileDataStore, Query}
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.geotools.api.filter.sort.SortOrder
import org.geotools.filter.text.ecql.ECQL
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.features.SerializationOption
import org.locationtech.geomesa.features.exporters.FileSystemExporter.{OrcFileSystemExporter, ParquetFileSystemExporter}
import org.locationtech.geomesa.features.exporters._
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.geoserver.ViewParams
import org.locationtech.geomesa.index.iterators.BinAggregatingScan
import org.locationtech.geomesa.index.planning.QueryRunner
import org.locationtech.geomesa.jobs.JobResult.{JobFailure, JobSuccess}
import org.locationtech.geomesa.jobs.{GeoMesaConfigurator, JobResult}
import org.locationtech.geomesa.tools.Command.CommandException
import org.locationtech.geomesa.tools.DistributedRunParam.RunModes
import org.locationtech.geomesa.tools._
import org.locationtech.geomesa.tools.export.ExportCommand.{ChunkedExporter, ExportOptions, ExportParams, Exporter}
import org.locationtech.geomesa.tools.utils.ParameterConverters.{BytesConverter, ExportFormatConverter}
import org.locationtech.geomesa.tools.utils.{JobRunner, NoopParameterSplitter, Prompt, TerminalCallback}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.fs.FileSystemDelegate.{CreateMode, FileHandle}
import org.locationtech.geomesa.utils.io.fs.LocalDelegate.StdInHandle
import org.locationtech.geomesa.utils.io.{FileSizeEstimator, IncrementingFileName, PathUtils, WithClose}
import org.locationtech.geomesa.utils.stats.MethodProfiling

import java.io._
import java.util.Collections
import java.util.zip.GZIPOutputStream
import scala.annotation.tailrec
import scala.util.control.NonFatal

trait ExportCommand[DS <: DataStore] extends DataStoreCommand[DS]
    with DistributedCommand with InteractiveCommand with MethodProfiling {

  import ExportCommand.CountKey

  override val name = "export"
  override def params: ExportParams

  override def libjarsFiles: Seq[String] = Seq("org/locationtech/geomesa/tools/export-libjars.list")

  override def execute(): Unit = {
    def complete(result: JobResult, time: Long): Unit = {
      result match {
        case JobSuccess(message, counts) =>
          val count = counts.get(CountKey).map(c => s" for $c features").getOrElse("")
          Command.user.info(s"$message$count in ${time}ms")

        case JobFailure(message) =>
          Command.user.info(s"Feature export failed in ${time}ms: $message")
          throw new CommandException(message) // propagate out and return an exit code error
      }
    }

    profile(complete _)(withDataStore(export))
  }

  private def export(ds: DS): JobResult = {
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
        val exporter = chunks match {
          case None    => new Exporter(options, query.getHints)
          case Some(c) => new ChunkedExporter(options, query.getHints, c)
        }
        val count = try { export(ds, query, exporter, !params.suppressEmpty) } finally { exporter.close() }
        val outFile = options.file match {
          case None => "standard out"
          case Some(f) if chunks.isDefined => PathUtils.getBaseNameAndExtension(f).productIterator.mkString("_*")
          case Some(f) => f
        }

        JobSuccess(s"Feature export complete to $outFile", count.map(CountKey -> _).toMap)

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

        // file output format doesn't generally let you write to an existing directory
        val context = FileContext.getFileContext(output.toUri, job.getConfiguration)
        if (context.util.exists(output)) {
          val warning = s"Output directory '$output' exists - files may be overwritten"
          if (params.force) {
            Command.user.warn(warning)
          } else if (!Prompt.confirm(s"WARNING $warning. Continue anyway (y/n)? ")) {
            if (Prompt.confirm("WARNING DATA MAY BE LOST - delete directory and proceed with export (y/n)? ")) {
              context.delete(output, true)
            } else {
              throw new ParameterException(s"Output directory '$output' exists")
            }
          }
        }

        val filename = FilenameUtils.getName(file)

        // note: use our original hints, which have the aggregating keys
        ExportJob.configure(job, connection, sft, hints, filename, output, options.format, options.headers,
          chunks, options.gzip, reducers, libjars(options.format), libjarsPaths)

        val reporter = TerminalCallback()
        JobRunner.run(job, reporter, ExportJob.Counters.mapping(job), ExportJob.Counters.reducing(job)).merge {
          Some(JobSuccess(s"Feature export complete to $output", Map(CountKey -> ExportJob.Counters.count(job))))
        }

      case _ => throw new NotImplementedError() // someone added a run mode and didn't implement it here...
    }
  }

  /**
   * Hook for overriding export
   *
   * @param ds data store
   * @param query query
   * @param exporter exporter
   * @param writeEmptyFiles export empty files or no
   * @return
   */
  protected def export(ds: DS, query: Query, exporter: FeatureExporter, writeEmptyFiles: Boolean): Option[Long] = {
    try {
      Command.user.info("Running export - please wait...")
      val features = ds.getFeatureSource(query.getTypeName).getFeatures(query)
      WithClose(CloseableIterator(features.features())) { iter =>
        if (writeEmptyFiles || iter.hasNext) {
          exporter.start(features.getSchema)
          exporter.export(iter)
        } else {
          Some(0L)
        }
      }
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

  private val CountKey = "count"

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
      configureLeafletExport(params)
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
    query.setPropertyNames(attributes: _*)

    if (!params.sortFields.isEmpty) {
      val fields = params.sortFields.asScala.toSeq
      if (fields.exists(a => sft.indexOf(a) == -1)) {
        val errors = fields.filter(a => sft.indexOf(a) == -1)
        throw new ParameterException(s"Invalid sort attribute${if (errors.lengthCompare(1) == 0) "" else "s"}: " +
            errors.mkString(", "))
      }
      val order = if (params.sortDescending) { SortOrder.DESCENDING } else { SortOrder.ASCENDING }
      query.setSortBy(fields.map(f => org.locationtech.geomesa.filter.ff.sort(f, order)): _*)
    } else if (hints.isArrowQuery) {
      hints.getArrowSort.foreach { case (f, r) =>
        val order = if (r) { SortOrder.DESCENDING } else { SortOrder.ASCENDING }
        query.setSortBy(org.locationtech.geomesa.filter.ff.sort(f, order))
      }
    }

    logger.debug(s"Applying CQL filter ${ECQL.toCQL(filter)}")
    logger.debug(s"Applying transform ${Option(attributes).map(_.mkString(",")).orNull}")

    QueryRunner.configureDefaultQuery(sft, query)
  }

  /**
   * Configure parameters for a leaflet export
   *
   * @param params params
   * @throws ParameterException if params are not valid for a leaflet export
   */
  @throws(classOf[ParameterException])
  private def configureLeafletExport(params: ExportParams): Unit = {
    val large = "The Leaflet map may exhibit performance issues when displaying large numbers of features. For a " +
        "more robust solution, consider using GeoServer."
    if (params.maxFeatures == null) {
      Command.user.warn(large)
      Command.user.warn(s"Limiting max features to ${LeafletMapExporter.MaxFeatures}. To override, " +
          "please use --max-features")
      params.maxFeatures = LeafletMapExporter.MaxFeatures
    } else if (params.maxFeatures > LeafletMapExporter.MaxFeatures) {
      if (params.force) {
        Command.user.warn(large)
      } else if (!Prompt.confirm(s"$large Would you like to continue anyway (y/n)? ")) {
        throw new ParameterException("Terminating execution")
      }
    }

    if (params.gzip != null) {
      Command.user.warn("Ignoring gzip parameter for Leaflet export")
      params.gzip = null
    }

    if (params.file == null) {
      params.file = sys.props("user.dir")
    }
    if (PathUtils.isRemote(params.file)) {
      if (params.file.endsWith("/")) {
        params.file = s"${params.file}index.html"
      } else if (FilenameUtils.indexOfExtension(params.file) == -1) {
        params.file = s"${params.file}/index.html"
      }
    } else {
      val file = new File(params.file)
      val destination = if (file.isDirectory || (!file.exists && file.getName.indexOf(".") == -1)) {
        new File(file, "index.html")
      } else {
        file
      }
      params.file = destination.getAbsolutePath
    }
  }
  /**
    * Disable hints for aggregating scans by removing them, and moving any aggregating sort hints to
    * regular sort hints
    *
    * @param sft simple feature type
    * @param hints hints
    * @return
    */
  private def disableAggregation(sft: SimpleFeatureType, hints: Hints): Unit = {
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
   * @param writeEmptyFiles write empty files (i.e. headers, etc), or suppress all output
   */
  case class ExportOptions(
    format: ExportFormat,
    file: Option[String],
    gzip: Option[Int],
    headers: Boolean,
    writeEmptyFiles: Boolean
  )

  object ExportOptions {
    def apply(params: ExportParams): ExportOptions =
      ExportOptions(params.outputFormat, Option(params.file), Option(params.gzip).map(_.intValue), !params.noHeader, !params.suppressEmpty)
  }

  /**
    * Single exporter that handles the command options and delegates to the correct implementation
    *
    * @param options options
    * @param hints query hints
    */
  class Exporter(options: ExportOptions, hints: Hints) extends FeatureExporter {

    // used only for streaming export formats
    private lazy val stream: ByteCounterStream = {
      // avro compression is handled differently, see AvroExporter below
      val gzip = options.gzip.filter(_ => options.format != ExportFormat.Avro && options.format != ExportFormat.AvroNative)
      val handle = options.file match {
        case None => StdInHandle.get() // writes to std out
        case Some(f) => PathUtils.getHandle(f)
      }
      if (options.writeEmptyFiles) {
        new ExportStream(handle, gzip)
      } else {
        new LazyExportStream(handle, gzip)
      }
    }

    // used only for file-based export formats
    private lazy val name = options.file.getOrElse {
      // should have been validated already...
      throw new IllegalStateException("Export format requires a file but none was specified")
    }

    // noinspection ComparingUnrelatedTypes
    private lazy val fids = !Option(hints.get(QueryHints.ARROW_INCLUDE_FID)).contains(java.lang.Boolean.FALSE)

    private val exporter = options.format match {
      case ExportFormat.Arrow      => new ArrowExporter(stream, hints)
      case ExportFormat.Avro       => new AvroExporter(stream, options.gzip)
      case ExportFormat.AvroNative => new AvroExporter(stream, options.gzip, Set(SerializationOption.NativeCollections))
      case ExportFormat.Bin        => new BinExporter(stream, hints)
      case ExportFormat.Csv        => DelimitedExporter.csv(stream, options.headers, fids)
      case ExportFormat.Gml2       => GmlExporter.gml2(stream)
      case ExportFormat.Gml3       => GmlExporter(stream)
      case ExportFormat.Json       => new GeoJsonExporter(stream)
      case ExportFormat.Leaflet    => new LeafletMapExporter(stream)
      case ExportFormat.Null       => NullExporter
      case ExportFormat.Orc        => new OrcFileSystemExporter(name)
      case ExportFormat.Parquet    => new ParquetFileSystemExporter(name)
      case ExportFormat.Shp        => new ShapefileExporter(new File(name))
      case ExportFormat.Tsv        => DelimitedExporter.tsv(stream, options.headers, fids)
      // shouldn't happen unless someone adds a new format and doesn't implement it here
      case _ => throw new NotImplementedError(s"Export for '${options.format}' is not implemented")
    }

    override def start(sft: SimpleFeatureType): Unit = exporter.start(sft)

    override def export(features: Iterator[SimpleFeature]): Option[Long] = exporter.export(features)

    def bytes: Long = {
      if (options.format.streaming) {
        stream.bytes
      } else {
        exporter match {
          case e: ShapefileExporter => e.bytes
          case _ => PathUtils.getHandle(name).length
        }
      }
    }

    override def close(): Unit = exporter.close()
  }

  /**
    * Feature exporter that handles chunking output into multiple files
    *
    * @param options export options
    * @param hints query hints
    * @param chunks number of bytes to write per file
    */
  class ChunkedExporter(options: ExportOptions, hints: Hints, chunks: Long)
      extends FeatureExporter with LazyLogging {

    private val names = options.file match {
      case None    => Iterator.continually(None)
      case Some(f) => new IncrementingFileName(f).map(Option.apply)
    }

    private var sft: SimpleFeatureType = _
    private var exporter: Exporter = _
    private var estimator: FileSizeEstimator = _
    private var count = 0L // number of features written
    private var total = 0L // sum size of all finished chunks

    override def start(sft: SimpleFeatureType): Unit = {
      this.sft = sft
      estimator = new FileSizeEstimator(chunks, 0.05f, options.format.bytesPerFeature(sft)) // 5% error threshold
      nextChunk()
    }

    override def export(features: Iterator[SimpleFeature]): Option[Long] = export(features, None)

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
      exporter = new Exporter(options.copy(file = names.next), hints)
      exporter.start(sft)
      count = 0L
    }

    @tailrec
    private def export(features: Iterator[SimpleFeature], result: Option[Long]): Option[Long] = {
      var estimate = estimator.estimate(exporter.bytes)
      val counter = features.takeWhile { _ => count += 1; estimate -= 1; estimate >= 0 }
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
   * Export output stream, lazily instantiated
   *
   * @param out file handle
   * @param gzip gzip
   */
  class LazyExportStream(out: FileHandle, gzip: Option[Int] = None) extends ByteCounterStream {

    private var stream: ExportStream = _

    private def ensureStream(): OutputStream = {
      if (stream == null) {
        stream = new ExportStream(out, gzip)
      }
      stream
    }

    def bytes: Long = if (stream == null) { 0L } else { stream.bytes }

    override def write(b: Array[Byte]): Unit = ensureStream().write(b)
    override def write(b: Array[Byte], off: Int, len: Int): Unit = ensureStream().write(b, off, len)
    override def write(b: Int): Unit = ensureStream().write(b)

    override def flush(): Unit = if (stream != null) { stream.flush() }
    override def close(): Unit = if (stream != null) { stream.close() }
  }

  /**
   * Export output stream
   *
   * @param out file handle
   * @param gzip gzip
   */
  class ExportStream(out: FileHandle, gzip: Option[Int] = None) extends ByteCounterStream {

    // lowest level - keep track of the bytes we write
    // do this before any compression, buffering, etc so we get an accurate count
    private val counter = new CountingOutputStream(out.write(CreateMode.Create))
    private val stream = {
      val compressed = gzip match {
        case None => counter
        case Some(c) => new GZIPOutputStream(counter) { `def`.setLevel(c) } // hack to access the protected deflate level
      }
      new BufferedOutputStream(compressed)
    }

    override def bytes: Long = counter.getByteCount

    override def write(b: Array[Byte]): Unit = stream.write(b)
    override def write(b: Array[Byte], off: Int, len: Int): Unit = stream.write(b, off, len)
    override def write(b: Int): Unit = stream.write(b)

    override def flush(): Unit = stream.flush()
    override def close(): Unit = stream.close()
  }

  trait ByteCounterStream extends OutputStream {
    def bytes: Long
  }

  /**
    * Export parameters
    */
  trait ExportParams extends OptionalCqlFilterParam with QueryHintsParams
      with DistributedRunParam with TypeNameParam with NumReducersParam with OptionalForceParam {
    @Parameter(names = Array("-o", "--output"), description = "Output to a file instead of std out")
    var file: String = _

    @Parameter(names = Array("--gzip"), description = "Level of gzip compression to apply to output, from 1-9")
    var gzip: Integer = _

    @Parameter(
      names = Array("--no-header"),
      description = "Export as a delimited text format (csv|tsv) without a type header")
    var noHeader: Boolean = false

    @Parameter(
      names = Array("--suppress-empty"),
      description = "Suppress all output (headers, etc) if there are no features exported")
    var suppressEmpty: Boolean = false

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
      names = Array("--chunk-size"),
      description = "Split the output into multiple files, by specifying the rough number of bytes to store per file",
      converter = classOf[BytesConverter])
    var chunkSize: java.lang.Long = _

    @Parameter(
      names = Array("-F", "--output-format"),
      description = "File format of output files (csv|tsv|gml|json|shp|avro|avro-native|leaflet|orc|parquet|arrow)",
      converter = classOf[ExportFormatConverter])
    var explicitOutputFormat: ExportFormat = _

    lazy val outputFormat: ExportFormat = {
      if (explicitOutputFormat != null) { explicitOutputFormat } else {
        Option(file).flatMap(f => ExportFormat(PathUtils.getUncompressedExtension(f))).getOrElse(ExportFormat.Csv)
      }
    }
  }
}
