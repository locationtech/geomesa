/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.ingest

import com.beust.jcommander.{Parameter, ParameterException}
import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions, ConfigValueFactory}
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.{FilenameUtils, IOUtils}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.geotools.api.data.DataStore
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.convert.all.TypeAwareInference
import org.locationtech.geomesa.convert.{ConverterConfigLoader, EvaluationContext}
import org.locationtech.geomesa.convert2.SimpleFeatureConverter
import org.locationtech.geomesa.jobs.Awaitable
import org.locationtech.geomesa.jobs.JobResult.{JobFailure, JobSuccess}
import org.locationtech.geomesa.jobs.mapreduce.ConverterCombineInputFormat
import org.locationtech.geomesa.tools.Command.CommandException
import org.locationtech.geomesa.tools.DistributedRunParam.RunModes
import org.locationtech.geomesa.tools.DistributedRunParam.RunModes.RunMode
import org.locationtech.geomesa.tools._
import org.locationtech.geomesa.tools.data.CreateSchemaCommand.SchemaOptionsCommand
import org.locationtech.geomesa.tools.ingest.IngestCommand.{IngestCounters, IngestParams, Inputs}
import org.locationtech.geomesa.tools.utils.{CLArgResolver, Prompt, TerminalCallback}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypeComparator.TypeComparison
import org.locationtech.geomesa.utils.geotools.{ConfigSftParsing, SimpleFeatureTypeComparator, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.fs.FileSystemDelegate.FileHandle
import org.locationtech.geomesa.utils.io.fs.LocalDelegate.{CachingStdInHandle, StdInHandle}
import org.locationtech.geomesa.utils.io.{PathUtils, WithClose}
import org.locationtech.geomesa.utils.text.TextTools

import java.io.{File, FileWriter, InputStream, PrintWriter}
import java.nio.charset.StandardCharsets
import java.util.{Collections, Locale}
import scala.collection.mutable.ListBuffer
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

trait IngestCommand[DS <: DataStore]
    extends DataStoreCommand[DS] with DistributedCommand with InteractiveCommand with SchemaOptionsCommand {

  import scala.collection.JavaConverters._

  override val name = "ingest"
  override def params: IngestParams

  override def libjarsFiles: Seq[String] = Seq("org/locationtech/geomesa/tools/ingest-libjars.list")

  override def execute(): Unit = {
    val inputs: Inputs = {
      val files = Inputs(params.files.asScala.toSeq)
      if (files.stdin && !StdInHandle.isAvailable) {
        if (files.paths.isEmpty) {
          throw new ParameterException("Missing option: <files>... is required, or use `-` to ingest from standard in")
        } else {
          Command.user.info("Waiting for input...")
          while (!StdInHandle.isAvailable) {
            Thread.sleep(10)
          }
        }
      }
      if (params.srcList) { files.asSourceList } else { files }
    }

    val format = IngestCommand.getDataFormat(params, inputs.paths)
    val remote = inputs.paths.exists(PathUtils.isRemote)

    if (remote) {
      // If we have a remote file, make sure they are all the same FS
      val prefix = inputs.paths.head.split("/")(0).toLowerCase
      if (!inputs.paths.drop(1).forall(_.toLowerCase.startsWith(prefix))) {
        throw new ParameterException(s"Files must all be on the same file system: ($prefix) or all be local")
      }
    }

    val mode = if (format.contains("shp")) {
      // shapefiles have to be ingested locally, as we need access to the related files
      if (params.mode.exists(_ != RunModes.Local)) {
        Command.user.warn("Forcing run mode to local for shapefile ingestion")
      }
      RunModes.Local
    } else if (remote) {
      params.mode.getOrElse(RunModes.Distributed)
    } else {
      if (params.mode.exists(_ != RunModes.Local)) {
        throw new ParameterException("Input files must be in a distributed file system to run in distributed mode")
      }
      RunModes.Local
    }

    if (mode == RunModes.Local) {
      if (!params.waitForCompletion) {
        throw new ParameterException("Tracking must be enabled when running in local mode")
      }
    } else if (params.threads != 1) {
      throw new ParameterException("Threads can only be specified in local mode")
    }
    if (params.maxSplitSize != null && !params.combineInputs) {
      throw new ParameterException("--split-max-size can only be used with --combine-inputs")
    }

    withDataStore { ds =>
      // use .get to re-throw the exception if we fail
      IngestCommand.getSftAndConverter(params, inputs, format, Some(ds)).get.foreach { case (sft, converter) =>
        val start = System.currentTimeMillis()
        // create schema for the feature prior to ingest
        val existing = Try(ds.getSchema(sft.getTypeName)).getOrElse(null)
        if (existing == null) {
          Command.user.info(s"Creating schema '${sft.getTypeName}'")
          setBackendSpecificOptions(sft)
          ds.createSchema(sft)
        } else {
          // note: sft will have been loaded from the datastore if it already exists, so will match existing
          Command.user.info(s"Schema '${sft.getTypeName}' exists")
        }
        val result = startIngest(mode, ds, sft, converter, inputs)
        if (params.waitForCompletion) {
          result.await(TerminalCallback()) match {
            case JobSuccess(message, counts) =>
              Command.user.info(s"Ingestion complete in ${TextTools.getTime(start)}")
              val count = counts.getOrElse(IngestCounters.Persisted, counts.getOrElse(IngestCounters.Ingested, 0L))
              val failed = counts.getOrElse(IngestCounters.Failed, 0L)
              Command.user.info(IngestCommand.getStatInfo(count, failed, input = message))

            case JobFailure(message) =>
              Command.user.error(s"Ingestion failed in ${TextTools.getTime(start)}")
              // propagate out and return an exit code error
              throw new CommandException(message)
          }
        } else {
          Command.user.info("Job submitted, check tracking for progress and result")
        }
      }
    }
  }

  /**
   * Start the ingestion asynchronously, returning an object for reporting status
   *
   * @param mode run mode
   * @param ds data store
   * @param sft simple feature type
   * @param converter converter config
   * @param inputs input files
   * @return
   */
  protected def startIngest(
      mode: RunMode,
      ds: DS,
      sft: SimpleFeatureType,
      converter: Config,
      inputs: Inputs): Awaitable = {
    mode match {
      case RunModes.Local =>
        Command.user.info("Running ingestion in local mode")
        new LocalConverterIngest(ds, connection.asJava, sft, converter, inputs, params.threads)

      case RunModes.Distributed =>
        Command.user.info(s"Running ingestion in distributed ${if (params.combineInputs) "combine " else "" }mode")
        new ConverterIngestJob(connection, sft, converter, inputs.paths, libjarsFiles, libjarsPaths) {
          override def configureJob(job: Job): Unit = {
            super.configureJob(job)
            if (params.combineInputs) {
              job.setInputFormatClass(classOf[ConverterCombineInputFormat])
              Option(params.maxSplitSize).foreach(s => FileInputFormat.setMaxInputSplitSize(job, s.toLong))
            }
          }
        }

      case _ =>
        throw new NotImplementedError(s"Missing implementation for mode $mode")
    }
  }
}

object IngestCommand extends LazyLogging {

  val LocalBatchSize: SystemProperty = SystemProperty("geomesa.ingest.local.batch.size", "20000")

  // @Parameters(commandDescription = "Ingest/convert various file formats into GeoMesa")
  trait IngestParams extends OptionalTypeNameParam with OptionalFeatureSpecParam with OptionalForceParam
      with ConverterConfigParam with OptionalInputFormatParam with DistributedRunParam with DistributedCombineParam {

    @Parameter(names = Array("-t", "--threads"), description = "Number of threads if using local ingest")
    var threads: Integer = 1

    @Parameter(names = Array("--src-list"), description = "Input files are text files with lists of files, one per line, to ingest.")
    var srcList: Boolean = false

    @Parameter(names = Array("--no-tracking"), description = "Return immediately after submitting ingest job (distributed jobs)")
    var noWaitForCompletion: Boolean = false

    def waitForCompletion: Boolean = !noWaitForCompletion
  }

  /**
    * Determine the input file data format
    *
    * @param params params
    * @param files input files
    * @return input format, as a lower-case string
    */
  def getDataFormat(params: OptionalInputFormatParam, files: Seq[String]): Option[String] = {
    val raw = if (params.inputFormat != null) { Some(params.inputFormat) } else {
      val exts = files.iterator.flatMap(PathUtils.interpretPath).map(_.format).filter(_.nonEmpty)
      if (exts.hasNext) { Some(exts.next) } else { None }
    }
    raw.map {
      case r if r.equalsIgnoreCase("gml")     => "xml"
      case r if r.equalsIgnoreCase("geojson") => "json"
      case r => r.toLowerCase(Locale.US)
    }
  }

  /**
    * Loads and/or infers a SimpleFeatureType and Converter config based on the inputs. If not possible to load
    * or infer the values, will return a failure. If `interactive` and the user declines to use an
    * inferred result, will return a Success(None). Otherwise will return a Success(Some)
    *
    * @param params params
    * @param inputs input files
    * @param format input format
    * @param ds data store for loading schemas by name
    * @return None if user declines inferred result, otherwise the loaded/inferred result
    */
  def getSftAndConverter(
      params: TypeNameParam with FeatureSpecParam with ConverterConfigParam with OptionalForceParam,
      inputs: Inputs,
      format: Option[String],
      ds: Option[_ <: DataStore]): Try[Option[(SimpleFeatureType, Config)]] = Try {
    // try to load the sft, first check for an existing schema, then load from the params/environment
    var sft: SimpleFeatureType = loadSft(params, ds).orNull

    var converter: Config = Option(params.config).map(CLArgResolver.getConfig).orNull

    // if there is no converter passed in
    if (converter == null && (inputs.stdin || inputs.paths.nonEmpty)) {
      val errorMsg = if (inputs.stdin) { "Unable to read data from stdin" } else {
        "Parameter <files> did not evaluate to something that could be read"
      }
      // try to infer the schema from the inputs
      Command.user.info(
        s"No converter defined - will attempt to detect schema from ${if (inputs.stdin) "stdin" else "input files"}")
      val file = inputs.handles.headOption.getOrElse(throw new ParameterException(errorMsg))
      val opened = ListBuffer.empty[CloseableIterator[InputStream]]
      def open(): InputStream = {
        val is = try {
          val streams = file.open.map(_._2)
          opened += streams
          if (streams.hasNext) { streams.next } else { null }
        } catch {
          case NonFatal(e) => throw new RuntimeException(errorMsg, e)
        }
        if (is == null) {
          throw new ParameterException(errorMsg)
        } else {
          is
        }
      }

      val path = if (inputs.stdin) { Map.empty[String, AnyRef] } else { EvaluationContext.inputFileParam(file.path) }
      val opt = format match {
        case None => SimpleFeatureConverter.infer(open _, Option(sft), path)
        case Some(fmt) => TypeAwareInference.infer(fmt, open _, Option(sft), path)
      }

      val (inferredSft, inferredConverter) = opt match {
        case Success(o) => o
        case Failure(e) =>
          throw new ParameterException("Could not determine converter from inputs - please specify a converter", e)
      }

      val renderOptions = ConfigRenderOptions.concise().setFormatted(true)
      var inferredSftString: Option[String] = None

      if (sft == null) {
        val typeName = if (inputs.stdin) {
          // Throw an error if the user doesn't specify an SFT name
          Option(params.featureName).getOrElse {
            throw new ParameterException(
              "SimpleFeatureType name not specified. Please ensure the -f or --feature-name flag is set.")
          }
        } else {
          Option(params.featureName).getOrElse {
            val existing = ds.toSet[DataStore].flatMap(_.getTypeNames)
            val fileName = Option(FilenameUtils.getBaseName(file.path))
            val base = fileName.map(_.trim.replaceAll("[^A-Za-z0-9]+", "_")).filterNot(_.isEmpty).getOrElse {
              throw new RuntimeException("Unable to infer SimpleFeatureType name from file name. " +
                  "Please specify a name manually by setting the -f or --feature-name flag.")
            }
            var name = base
            var i = 0
            while (existing.contains(name)) {
              name = s"${base}_$i"
              i += 1
            }
            name
          }
        }

        sft = SimpleFeatureTypes.renameSft(inferredSft, typeName)
        inferredSftString = Some(SimpleFeatureTypes.toConfig(sft, includePrefix = false).root().render(renderOptions))
        if (!params.force) {
          Command.user.info(s"Inferred schema: $typeName identified ${SimpleFeatureTypes.encodeType(sft)}")
        }
      }
      converter = inferredConverter

      if (!params.force) {
        val converterString = inferredConverter.root().render(renderOptions)

        def persist(): Unit = if (Prompt.confirm("Persist this converter for future use (y/n)? ")) {
          writeInferredConverter(sft.getTypeName, converterString, inferredSftString)
        }

        Command.user.info(s"Inferred converter:\n$converterString")
        if (Prompt.confirm("Use inferred converter (y/n)? ")) {
          persist()
        } else {
          Command.user.info("Please re-run with a valid converter")
          persist()
          return Success(None)
        }
      }
    }

    if (sft == null) {
      throw new ParameterException("SimpleFeatureType name and/or specification argument is required")
    } else if (converter == null) {
      throw new ParameterException("Converter config argument is required")
    }

    if (params.errorMode != null) {
      val opts = ConfigValueFactory.fromMap(Collections.singletonMap("error-mode", params.errorMode.toString))
      converter = ConfigFactory.empty().withValue("options", opts).withFallback(converter)
    }

    Some((sft, converter))
  }

  /**
    * Gets status as a string
    */
  def getStatInfo(successes: Long, failures: Long, action: String = "Ingested", input: String = ""): String = {
    val failureString = if (failures == 0) {
      "with no failures"
    } else {
      s"and failed to ingest ${TextTools.getPlural(failures, "feature")}"
    }
    s"$action ${TextTools.getPlural(successes, "feature")} $failureString${TextTools.prefixSpace(input)}"
  }

  object IngestCounters {
    val Ingested  = "ingested"
    val Failed    = "failed"
    val Persisted = "persisted"
  }

  /**
   * Command inputs
   *
   * @param paths paths to files for ingest
   */
  case class Inputs(paths: Seq[String]) {

    import Inputs.StdInInputs

    import scala.collection.JavaConverters.asScalaIteratorConverter

    val stdin: Boolean = paths.isEmpty || paths == StdInInputs

    /**
     * Interprets the input paths into actual files, handling wildcards, etc
     */
    lazy val handles: List[FileHandle] = paths match {
      case Nil         => CachingStdInHandle.available().toList
      case StdInInputs => List(CachingStdInHandle.get())
      case _           => paths.flatMap(PathUtils.interpretPath).toList
    }

    /**
     * Interprets the paths as lists of source file names (instead of the files to ingest)
     *
     * @return the actual inputs to ingest
     */
    def asSourceList: Inputs = {
      val paths = handles.flatMap { file =>
        WithClose(file.open) { iter =>
          iter.flatMap { case (_, is) => IOUtils.lineIterator(is, StandardCharsets.UTF_8).asScala }.toList
        }
      }
      Inputs(paths)
    }
  }

  object Inputs {
    val StdInInputs: Seq[String] = Seq("-")
  }

  /**
    * Tries to load a feature type, first from the data store then from the params/environment
    *
    * @param params params
    * @param ds data store access
    * @return
    */
  private def loadSft(
      params: TypeNameParam with FeatureSpecParam,
      ds: Option[_ <: DataStore]): Option[SimpleFeatureType] = {
    val fromStore = for {
      d    <- ds
      name <- Option(params.featureName)
      sft  <- Try(d.getSchema(name)).filter(_ != null).toOption
    } yield {
      sft
    }
    lazy val fromEnv = Option(params.spec).map(CLArgResolver.getSft(_, params.featureName)).orElse {
      Option(params.featureName).flatMap(name => Try(CLArgResolver.getSft(name)).toOption)
    }

    if (logger.underlying.isWarnEnabled()) {
      for { fs <- fromStore; fe <- fromEnv } {
        SimpleFeatureTypeComparator.compare(fs, fe) match {
          case TypeComparison.Compatible(false, false, _) if fs.getTypeName == fe.getTypeName => // ok
          case _ =>
            logger.warn(
              "Schema from data store does not match schema from environment." +
                s"\n  From data store:  ${fs.getTypeName} identified ${SimpleFeatureTypes.encodeType(fs)}" +
                s"\n  From environment: ${fe.getTypeName} identified ${SimpleFeatureTypes.encodeType(fe)}")
        }
      }
    }

    fromStore.orElse(fromEnv)
  }

  private def writeInferredConverter(typeName: String, converterString: String, schemaString: Option[String]): Unit = {
    import scala.collection.JavaConverters._

    try {
      val conf = this.getClass.getClassLoader.getResources("reference.conf").asScala.find { u =>
        "file".equalsIgnoreCase(u.getProtocol) && u.getPath.endsWith("/conf/reference.conf")
      }
      conf match {
        case None => Command.user.error("Could not persist converter: could not find 'conf/reference.conf'")
        case Some(r) =>
          val reference = new File(r.toURI)
          val folder = reference.getParentFile
          val baseName = typeName.replaceAll("[^A-Za-z0-9_]+", "_")
          var convert = new File(folder, s"$baseName.conf")
          var i = 1
          while (convert.exists()) {
            convert = new File(folder, s"${baseName}_$i.conf")
            i += 1
          }
          WithClose(new PrintWriter(new FileWriter(convert))) { writer =>
            writer.println(s"${ConverterConfigLoader.path}.$baseName : $converterString")
            schemaString.foreach(s => writer.println(s"${ConfigSftParsing.path}.$baseName : $s"))
          }
          WithClose(new PrintWriter(new FileWriter(reference, true))) { writer =>
            writer.println(s"""include "${convert.getName}"""")
          }
          val (names, refs) = if (schemaString.isDefined) {
            ("schema and converter", s"'--spec $baseName' and '--converter $baseName'")
          } else {
            ("converter", s"'--converter $baseName'")
          }
          Command.user.info(s"Added import in reference.conf and saved inferred $names to ${convert.getAbsolutePath}")
          Command.user.info(s"In future commands, the $names may be invoked with $refs")
      }
    } catch {
      case NonFatal(e) =>
        logger.error("Error trying to persist inferred schema", e)
        Command.user.error(s"Error trying to persist inferred schema: $e")
    }
  }
}
