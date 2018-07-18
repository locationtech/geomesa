/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.ingest

import java.io.{Console, File}

import com.beust.jcommander.{Parameter, ParameterException}
import com.typesafe.config.{Config, ConfigRenderOptions}
import org.apache.commons.io.FilenameUtils
import org.geotools.data.DataStore
import org.locationtech.geomesa.convert2.SimpleFeatureConverter
import org.locationtech.geomesa.tools._
import org.locationtech.geomesa.tools.utils.{CLArgResolver, DataFormats, Prompt}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.PathUtils
import org.opengis.feature.simple.SimpleFeatureType

import scala.util.Try

trait IngestCommand[DS <: DataStore] extends DataStoreCommand[DS] with InteractiveCommand {

  import scala.collection.JavaConversions._

  override val name = "ingest"
  override def params: IngestParams

  def libjarsFile: String
  def libjarsPaths: Iterator[() => Seq[File]]

  override def execute(): Unit = {
    import DataFormats._
    import org.locationtech.geomesa.utils.conversions.ScalaImplicits.RichIterator

    ensureSameFs(PathUtils.RemotePrefixes)

    if (params.fmt == Shp) {
      createShpIngest().run()
    } else {
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
        val inferred = SimpleFeatureConverter.infer(() => file.open, Option(sft)).getOrElse {
          throw new ParameterException("Could not determine converter from inputs - please specify a converter")
        }

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
          sft = SimpleFeatureTypes.renameSft(inferred._1, typeName)
          Command.user.info(s"Inferred schema: $typeName identified ${SimpleFeatureTypes.encodeType(sft)}")
        }
        Command.user.info("Inferred converter:\n" +
            inferred._2.root().render(ConfigRenderOptions.concise().setFormatted(true)))
        if (Prompt.confirm("Use inferred converter (y/n)? ")) {
          Command.user.info("For consistency, please use this converter definition for any future ingest")
          converter = inferred._2
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

      createConverterIngest(sft, converter).run()
    }
  }

  protected def createConverterIngest(sft: SimpleFeatureType, converterConfig: Config): Runnable = {
    new ConverterIngest(sft, connection, converterConfig, params.files, Option(params.mode),
      libjarsFile, libjarsPaths, params.threads)
  }

  protected def createShpIngest(): Runnable =
    new ShapefileIngest(connection, Option(params.featureName), params.files, params.threads)

  def ensureSameFs(prefixes: Seq[String]): Unit = {
    prefixes.foreach { pre =>
      if (params.files.exists(_.toLowerCase.startsWith(s"$pre://")) &&
        !params.files.forall(_.toLowerCase.startsWith(s"$pre://"))) {
        throw new ParameterException(s"Files must all be on the same file system: ($pre) or all be local")
      }
    }
  }
}

// @Parameters(commandDescription = "Ingest/convert various file formats into GeoMesa")
trait IngestParams extends OptionalTypeNameParam with OptionalFeatureSpecParam
    with OptionalConverterConfigParam with OptionalInputFormatParam with DistributedRunParam {
  @Parameter(names = Array("-t", "--threads"), description = "Number of threads if using local ingest")
  var threads: Integer = 1
}
