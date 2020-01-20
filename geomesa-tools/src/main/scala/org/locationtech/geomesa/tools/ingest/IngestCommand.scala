/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.ingest

import java.io.{File, FileWriter, InputStream, PrintWriter}
import java.nio.charset.StandardCharsets
import java.util.{Collections, Locale}

import com.beust.jcommander.{Parameter, ParameterException}
import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions, ConfigValueFactory}
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.{FilenameUtils, IOUtils}
import org.geotools.data.{DataStore, DataUtilities}
import org.locationtech.geomesa.convert.ConverterConfigLoader
import org.locationtech.geomesa.convert.all.TypeAwareInference
import org.locationtech.geomesa.convert2.SimpleFeatureConverter
import org.locationtech.geomesa.tools.DistributedRunParam.RunModes
import org.locationtech.geomesa.tools.DistributedRunParam.RunModes.RunMode
import org.locationtech.geomesa.tools._
import org.locationtech.geomesa.tools.ingest.IngestCommand.IngestParams
import org.locationtech.geomesa.tools.utils.{CLArgResolver, Prompt}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.geotools.{ConfigSftParsing, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.fs.LocalDelegate.StdInHandle
import org.locationtech.geomesa.utils.io.{CloseWithLogging, PathUtils, WithClose}
import org.locationtech.geomesa.utils.text.TextTools
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.mutable.ListBuffer
import scala.util.control.NonFatal
import scala.util.{Success, Try}

trait IngestCommand[DS <: DataStore] extends DataStoreCommand[DS] with DistributedCommand with InteractiveCommand {

  import scala.collection.JavaConverters._

  override val name = "ingest"
  override def params: IngestParams

  override def libjarsFiles: Seq[String] = Seq("org/locationtech/geomesa/tools/ingest-libjars.list")

  override def execute(): Unit = {
    if (params.files.isEmpty && !StdInHandle.isAvailable) {
      throw new ParameterException("Missing option: <files>... is required")
    }

    val inputs = if (params.srcList) {
      val lists = if (params.files.isEmpty) { StdInHandle.available().toList } else {
        params.files.asScala.flatMap(PathUtils.interpretPath).toList
      }
      lists.flatMap { file =>
        WithClose(file.open) { iter =>
          iter.flatMap { case (_, is) => IOUtils.lineIterator(is, StandardCharsets.UTF_8).asScala }.toList
        }
      }
    } else {
      params.files.asScala
    }

    val format = IngestCommand.getDataFormat(params, inputs)
    val remote = inputs.exists(PathUtils.isRemote)

    if (remote) {
      // If we have a remote file, make sure they are all the same FS
      val prefix = inputs.head.split("/")(0).toLowerCase
      if (!inputs.drop(1).forall(_.toLowerCase.startsWith(prefix))) {
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

    // use .get to re-throw the exception if we fail
    IngestCommand.getSftAndConverter(params, inputs, format, Some(this)).get.foreach {
      case (sft, converter) => createIngest(mode, sft, converter, inputs).run()
    }
  }

  protected def createIngest(mode: RunMode, sft: SimpleFeatureType, converter: Config, inputs: Seq[String]): Runnable = {
    mode match {
      case RunModes.Local =>
        new LocalConverterIngest(connection, sft, converter, inputs, params.threads)

      case RunModes.Distributed if params.combineInputs =>
        new DistributedCombineConverterIngest(connection, sft, converter, inputs, libjarsFiles, libjarsPaths,
          Option(params.maxSplitSize), params.waitForCompletion)

      case RunModes.Distributed =>
        new DistributedConverterIngest(connection, sft, converter, inputs, libjarsFiles, libjarsPaths,
          params.waitForCompletion)

      case _ =>
        throw new NotImplementedError(s"Missing implementation for mode $mode")
    }
  }
}

object IngestCommand extends LazyLogging {

  val LocalBatchSize = SystemProperty("geomesa.ingest.local.batch.size", "20000")

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
    * @param command hook to data store for loading schemas by name
    * @return None if user declines inferred result, otherwise the loaded/inferred result
    */
  def getSftAndConverter(
      params: TypeNameParam with FeatureSpecParam with ConverterConfigParam with OptionalForceParam,
      inputs: Seq[String],
      format: Option[String],
      command: Option[DataStoreCommand[_ <: DataStore]]): Try[Option[(SimpleFeatureType, Config)]] = Try {
    import org.locationtech.geomesa.utils.conversions.ScalaImplicits.RichIterator

    // try to load the sft, first check for an existing schema, then load from the params/environment
    var sft: SimpleFeatureType = loadSft(params, command).orNull

    var converter: Config = Option(params.config).map(CLArgResolver.getConfig).orNull

    if (converter == null && inputs.nonEmpty) {
      // if there is no converter passed in, try to infer the schema from the input files themselves
      Command.user.info("No converter defined - will attempt to detect schema from input files")
      val file = inputs.iterator.flatMap(PathUtils.interpretPath).headOption.getOrElse {
        throw new ParameterException("Parameter <files> did not evaluate to anything that could be read")
      }
      val opened = ListBuffer.empty[CloseableIterator[InputStream]]
      def open(): InputStream = {
        val streams = file.open.map(_._2)
        opened += streams
        if (streams.hasNext) { streams.next } else {
          throw new ParameterException("Parameter <files> did not evaluate to anything that could be read")
        }
      }
      val (inferredSft, inferredConverter) = try {
        val opt = format match {
          case None      => SimpleFeatureConverter.infer(open, Option(sft), Option(file.path))
          case Some(fmt) => TypeAwareInference.infer(fmt, open, Option(sft), Option(file.path))
        }
        opt.getOrElse {
          throw new ParameterException("Could not determine converter from inputs - please specify a converter")
        }
      } finally {
        CloseWithLogging(opened)
      }
      val renderOptions = ConfigRenderOptions.concise().setFormatted(true)
      var inferredSftString: Option[String] = None

      if (sft == null) {
        val typeName = Option(params.featureName).getOrElse {
          val existing = command.toSet[DataStoreCommand[_ <: DataStore]].flatMap(_.withDataStore(_.getTypeNames))
          val fileName = Option(FilenameUtils.getBaseName(file.path))
          val base = fileName.map(_.trim.replaceAll("[^A-Za-z0-9]+", "_")).filterNot(_.isEmpty).getOrElse("geomesa")
          var name = base
          var i = 0
          while (existing.contains(name)) {
            name = s"${base}_$i"
            i += 1
          }
          name
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
    s"$action ${TextTools.getPlural(successes, "feature")} $failureString$input"
  }

  /**
    * Tries to load a feature type, first from the data store then from the params/environment
    *
    * @param params params
    * @param command command with data store access
    * @return
    */
  private def loadSft(
      params: TypeNameParam with FeatureSpecParam,
      command: Option[DataStoreCommand[_ <: DataStore]]): Option[SimpleFeatureType] = {
    val fromStore = for {
      cmd  <- command
      name <- Option(params.featureName)
      sft  <- cmd.withDataStore(ds => Try(ds.getSchema(name)).filter(_ != null).toOption)
    } yield {
      sft
    }
    lazy val fromEnv = Option(params.spec).map(CLArgResolver.getSft(_, params.featureName)).orElse {
      Option(params.featureName).flatMap(name => Try(CLArgResolver.getSft(name)).toOption)
    }

    if (logger.underlying.isWarnEnabled()) {
      for { fs <- fromStore; fe <- fromEnv } {
        if (fs.getTypeName != fe.getTypeName || SimpleFeatureTypes.compare(fs, fe) != 0) {
          logger.warn(
            "Schema from data store does not match schema from environment." +
              s"\n  From data store:  ${fs.getTypeName} identified ${DataUtilities.encodeType(fs)}" +
              s"\n  From environment: ${fe.getTypeName} identified ${DataUtilities.encodeType(fe)}")
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
