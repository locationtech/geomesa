/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.ingest

import java.io.{File, FileWriter, PrintWriter}

import com.beust.jcommander.{Parameter, ParameterException}
import com.typesafe.config.{Config, ConfigRenderOptions}
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.FilenameUtils
import org.geotools.data.DataStore
import org.locationtech.geomesa.convert.shp.ShapefileConverterFactory
import org.locationtech.geomesa.convert.ConverterConfigLoader
import org.locationtech.geomesa.convert2.SimpleFeatureConverter
import org.locationtech.geomesa.tools.DistributedRunParam.RunModes
import org.locationtech.geomesa.tools._
import org.locationtech.geomesa.tools.utils.{CLArgResolver, DataFormats, Prompt}
import org.locationtech.geomesa.utils.geotools.{ConfigSftParsing, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.{PathUtils, WithClose}
import org.opengis.feature.simple.SimpleFeatureType

import scala.util.Try
import scala.util.control.NonFatal

trait IngestCommand[DS <: DataStore] extends DataStoreCommand[DS] with InteractiveCommand with LazyLogging {

  import scala.collection.JavaConversions._

  override val name = "ingest"
  override def params: IngestParams

  def libjarsFile: String
  def libjarsPaths: Iterator[() => Seq[File]]

  override def execute(): Unit = {
    import org.locationtech.geomesa.utils.conversions.ScalaImplicits.RichIterator

    ensureSameFs(PathUtils.RemotePrefixes)

    // try to load the sft, first check for an existing schema, then load from the params/environment
    var sft: SimpleFeatureType =
      Option(params.featureName).flatMap(n => Try(withDataStore(_.getSchema(n))).filter(_ != null).toOption)
        .orElse(Option(params.spec).flatMap(s => Option(CLArgResolver.getSft(s, params.featureName))))
        .orNull

    var converter: Config = Option(params.config).map(CLArgResolver.getConfig).orNull

    if (converter == null && params.files.nonEmpty) {
      // if there is no converter passed in, try to infer the schema from the input files themselves
      Command.user.info("No converter defined - will attempt to detect schema from input files")
      val file = params.files.iterator.flatMap(PathUtils.interpretPath).headOption.getOrElse {
        throw new ParameterException(s"<files> '${params.files.mkString(",")}' did not evaluate to anything" +
            "that could be read")
      }
      val (inferredSft, inferredConverter) = {
        val opt = if (params.fmt == DataFormats.Shp) {
          ShapefileConverterFactory.infer(file.path, Option(sft))
        } else {
          SimpleFeatureConverter.infer(() => file.open, Option(sft))
        }
        opt.getOrElse {
          throw new ParameterException("Could not determine converter from inputs - please specify a converter")
        }
      }

      val renderOptions = ConfigRenderOptions.concise().setFormatted(true)
      var inferredSftString: Option[String] = None

      if (sft == null) {
        val typeName = Option(params.featureName).getOrElse {
          val existing = withDataStore(_.getTypeNames)
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
        Command.user.info(s"Inferred schema: $typeName identified ${SimpleFeatureTypes.encodeType(sft)}")
      }
      val converterString = inferredConverter.root().render(renderOptions)
      Command.user.info(s"Inferred converter:\n$converterString")
      if (Prompt.confirm("Use inferred converter (y/n)? ")) {
        if (Prompt.confirm("Persist this converter for future use (y/n)? ")) {
          writeInferredConverter(sft.getTypeName, converterString, inferredSftString)
        }
        converter = inferredConverter
      } else {
        Command.user.info("Please re-run with a valid converter")
        return
      }
    }

    if (sft == null) {
      throw new ParameterException("SimpleFeatureType name and/or specification argument is required")
    } else if (converter == null) {
      throw new ParameterException("Converter config argument is required")
    }

    if (params.fmt == DataFormats.Shp) {
      // shapefiles have to be ingested locally, as we need access to the related files
      if (params.mode == RunModes.Distributed) {
        Command.user.warn("Forcing run mode to local for shapefile ingestion")
      }
      params.mode = RunModes.Local
    }

    createConverterIngest(sft, converter).run()
  }

  protected def createConverterIngest(sft: SimpleFeatureType, converterConfig: Config): Runnable = {
    new ConverterIngest(sft, connection, converterConfig, params.files, Option(params.mode),
      libjarsFile, libjarsPaths, params.threads)
  }

  private def ensureSameFs(prefixes: Seq[String]): Unit = {
    prefixes.foreach { pre =>
      if (params.files.exists(_.toLowerCase.startsWith(s"$pre://")) &&
        !params.files.forall(_.toLowerCase.startsWith(s"$pre://"))) {
        throw new ParameterException(s"Files must all be on the same file system: ($pre) or all be local")
      }
    }
  }

  private def writeInferredConverter(typeName: String, converterString: String, schemaString: Option[String]): Unit = {
    try {
      val conf = this.getClass.getClassLoader.getResources("reference.conf").find { u =>
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

// @Parameters(commandDescription = "Ingest/convert various file formats into GeoMesa")
trait IngestParams extends OptionalTypeNameParam with OptionalFeatureSpecParam
    with OptionalConverterConfigParam with OptionalInputFormatParam with DistributedRunParam {
  @Parameter(names = Array("-t", "--threads"), description = "Number of threads if using local ingest")
  var threads: Integer = 1
}
