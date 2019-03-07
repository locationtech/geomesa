/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.export

import java.io.{File, FilenameFilter}
import java.nio.charset.StandardCharsets

import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.FileUtils
import org.geotools.factory.Hints
import org.junit.runner.RunWith
import org.locationtech.geomesa.tools.export.formats.ExportFormats
import org.locationtech.geomesa.tools.export.formats.ExportFormats.ExportFormat
import org.locationtech.geomesa.tools.ingest.IngestCommand
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.simple.SimpleFeatureType
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ConvertCommandTest extends Specification with LazyLogging {

  sequential

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

  val inFormats = Seq(ExportFormats.Csv, ExportFormats.Tsv, ExportFormats.Json)
  val outFormats = ExportFormats.values.toSeq

  for (in <- inFormats; out <- outFormats) {
    logger.debug(s"Testing $in to $out converter")
    testPair(in, out)
  }

  def getInputFileAndConf(fmt: ExportFormat): (String, String) = {
    fmt match {
      case ExportFormats.Csv  => (csvInput,  csvConf)
      case ExportFormats.Tsv  => (tsvInput,  tsvConf)
      case ExportFormats.Json => (jsonInput, jsonConf)
    }
  }

  def testPair(inFmt: ExportFormat, outFmt: ExportFormat): Unit = {
    s"Convert Command should convert $inFmt -> $outFmt" in {
      val (inputFile, conf) = getInputFileAndConf(inFmt)

      def withCommand[T](test: ConvertCommand => T): T = {
        val file = if (outFmt == ExportFormats.Leaflet) {
          File.createTempFile("convertTest", ".html")
        } else {
          File.createTempFile("convertTest", s".${outFmt.toString.toLowerCase}")
        }
        file.delete() // some output formats require that the file doesn't exist
        val command = new ConvertCommand
        command.params.files.add(inputFile)
        command.params.config = conf
        command.params.spec = conf
        command.params.outputFormat = outFmt
        command.params.force = true
        command.params.file = file.getAbsolutePath

        try {
          test(command)
        } finally {
          if (!file.delete()) {
            file.deleteOnExit()
          }
          if (outFmt == ExportFormats.Shp) {
            val root = file.getName.takeWhile(_ != '.') + "."
            file.getParentFile.listFiles(new FilenameFilter() {
              override def accept(dir: File, name: String) = name.startsWith(root)
            }).foreach { file =>
              if (!file.delete()) {
                file.deleteOnExit()
              }
            }
          }
        }
      }

      "get a Converter" in {
        withCommand { command =>
          val sftAndConverter = IngestCommand.getSftAndConverter(command.params, Seq.empty, None, None)
          sftAndConverter must beASuccessfulTry(beSome[(SimpleFeatureType, Config)])
        }
      }
      "get an Exporter" in {
        withCommand { command =>
          WithClose(ConvertCommand.getExporter(command.params, ExportFormats.Csv, new Hints()))(_ must not(beNull))
        }
      }
      "export data" in {
        withCommand { command =>
          command.execute()
          if (command.params.outputFormat == ExportFormats.Null) {
            new File(command.params.file).length() mustEqual 0
          } else {
            new File(command.params.file).length() must beGreaterThan(0L)
          }
        }
      }
      "use type inference" in {
        if (inFmt == ExportFormats.Csv || inFmt == ExportFormats.Tsv) {
          withCommand { command =>
            command.params.config = null
            command.params.spec = null
            command.execute()
            if (command.params.outputFormat == ExportFormats.Null) {
              new File(command.params.file).length() mustEqual 0
            } else {
              new File(command.params.file).length() must beGreaterThan(0L)
            }
          }
        } else {
          ok
        }
      }
    }
  }
}
