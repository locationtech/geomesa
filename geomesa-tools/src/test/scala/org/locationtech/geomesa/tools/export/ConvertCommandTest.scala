/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.`export`

import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.FileUtils
import org.geotools.api.feature.simple.SimpleFeatureType
import org.junit.runner.RunWith
import org.locationtech.geomesa.tools.export.formats.ExportFormat
import org.locationtech.geomesa.tools.ingest.IngestCommand
import org.locationtech.geomesa.tools.ingest.IngestCommand.Inputs
import org.locationtech.geomesa.utils.io.PathUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.concurrent.atomic.AtomicInteger

@RunWith(classOf[JUnitRunner])
class ConvertCommandTest extends Specification with LazyLogging {

  sequential

  val inputFilesAndConfs: Map[ExportFormat, (String, String)] = {
    val csvInput = getClass.getResource("/convert/csv-data.csv").getFile
    val csvConf  = {
      val file = new File(getClass.getResource("/convert/csv-convert.conf").getFile)
      FileUtils.readFileToString(file, StandardCharsets.UTF_8)
    }

    val tsvInput = getClass.getResource("/convert/tsv-data.tsv").getFile
    val tsvConf  = {
      val file = new File(getClass.getResource("/convert/tsv-convert.conf").getFile)
      FileUtils.readFileToString(file, StandardCharsets.UTF_8)
    }

    val jsonInput = getClass.getResource("/convert/json-data.json").getFile
    val jsonConf  = {
      val file = new File(getClass.getResource("/convert/json-convert.conf").getFile)
      FileUtils.readFileToString(file, StandardCharsets.UTF_8)
    }

    Map(
      ExportFormat.Csv  -> (csvInput  -> csvConf),
      ExportFormat.Tsv  -> (tsvInput  -> tsvConf),
      ExportFormat.Json -> (jsonInput -> jsonConf)
    )
  }

  val inFormats = Seq(ExportFormat.Csv, ExportFormat.Tsv, ExportFormat.Json)
  val outFormats = ExportFormat.Formats.filter(_ != ExportFormat.Null)

  val counter = new AtomicInteger(0)

  def createCommand(inFmt: ExportFormat, outFmt: ExportFormat): ConvertCommand = {
    val (inputFile, conf) = inputFilesAndConfs(inFmt)
    val file = new File(dir.toFile, s"${counter.getAndIncrement()}/out.${outFmt.extensions.headOption.orNull}")
    val command = new ConvertCommand
    command.params.files.add(inputFile)
    command.params.config = conf
    command.params.spec = conf
    command.params.explicitOutputFormat = outFmt
    command.params.force = true
    command.params.file = file.getAbsolutePath
    command
  }

  var dir: Path = _

  step {
    dir = Files.createTempDirectory("gm-convert-test")
  }

  "Convert Command" should {
    "get a converter" in {
      forall(inFormats) { inFmt =>
        forall(outFormats) { outFmt =>
          val command = createCommand(inFmt, outFmt)
          val sftAndConverter = IngestCommand.getSftAndConverter(command.params, Inputs(Seq.empty), None, None)
          sftAndConverter must beASuccessfulTry(beSome[(SimpleFeatureType, Config)])
        }
      }
    }

    "export data" in {
      forall(inFormats) { inFmt =>
        forall(outFormats) { outFmt =>
          logger.debug(s"Testing $inFmt to $outFmt converter")
          val command = createCommand(inFmt, outFmt)
          command.execute()
          new File(command.params.file).length() must beGreaterThan(0L)
        }
      }
    }

    "use type inference" in {
      forall(Seq(ExportFormat.Csv, ExportFormat.Tsv)) { inFmt =>
        forall(outFormats) { outFmt =>
          logger.debug(s"Testing $inFmt to $outFmt type inference")
          val command = createCommand(inFmt, outFmt)
          command.params.config = null
          command.params.spec = null
          command.execute()
          new File(command.params.file).length() must beGreaterThan(0L)
        }
      }
    }
  }

  step {
    if (dir != null) {
      PathUtils.deleteRecursively(dir)
    }
  }
}
