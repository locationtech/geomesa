/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools

import java.io.{File, FilenameFilter}
import java.nio.charset.StandardCharsets

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.FileUtils
import org.junit.runner.RunWith
import org.locationtech.geomesa.tools.utils.DataFormats.DataFormat
import org.locationtech.geomesa.tools.utils.{CLArgResolver, DataFormats}
import org.locationtech.geomesa.utils.io.{PathUtils, WithClose}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ConvertCommandTest extends Specification with LazyLogging {

  val csvInput = getClass.getResource("/convert/csv-data.csv").getFile
  val csvConf  = {
    val file = new File(getClass.getResource("/convert/csv-convert.conf").getFile)
    FileUtils.readFileToString(file, StandardCharsets.UTF_8)
  }

  val tsvInput = getClass.getResource("/convert/tsv-data.csv").getFile
  val tsvConf  = {
    val file = new File(getClass.getResource("/convert/tsv-convert.conf").getFile)
    FileUtils.readFileToString(file, StandardCharsets.UTF_8)
  }

  val jsonInput = getClass.getResource("/convert/json-data.json").getFile
  val jsonConf  = {
    val file = new File(getClass.getResource("/convert/json-convert.conf").getFile)
    FileUtils.readFileToString(file, StandardCharsets.UTF_8)
  }

  val inFormats = Seq(DataFormats.Csv, DataFormats.Tsv, DataFormats.Json)
  val outFormats = DataFormats.values.filter( _ != DataFormats.Null ).toSeq

  for (in <- inFormats; out <- outFormats) {
    logger.debug(s"Testing $in to $out converter")
    testPair(in, out)
  }

  def getInputFileAndConf(fmt: DataFormat): (String, String) = {
    fmt match {
      case DataFormats.Csv  => (csvInput,  csvConf)
      case DataFormats.Tsv  => (tsvInput,  tsvConf)
      case DataFormats.Json => (jsonInput, jsonConf)
    }
  }

  def testPair(inFmt: DataFormat, outFmt: DataFormat): Unit = {
    s"Convert Command should convert $inFmt -> $outFmt" in {
      val (inputFile, conf) = getInputFileAndConf(inFmt)
      val sft = CLArgResolver.getSft(conf)

      def withCommand[T](test: (ConvertCommand) => T): T = {
        val command = new ConvertCommand
        command.params.files.add(inputFile)
        command.params.config = conf
        command.params.spec = conf
        command.params.outputFormat = outFmt
        command.params.file =
          if (outFmt == DataFormats.Leaflet) {
            File.createTempFile("convertTest", s".html")
          } else {
            File.createTempFile("convertTest", s".${outFmt.toString.toLowerCase}")
          }

        try {
          test(command)
        } finally {
          if (!command.params.file.delete()) {
            command.params.file.deleteOnExit()
          }
          if (outFmt == DataFormats.Shp) {
            val root = command.params.file.getName.takeWhile(_ != '.') + "."
            command.params.file.getParentFile.listFiles(new FilenameFilter() {
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
          ConvertCommand.getConverter(command.params, sft) must not(beNull)
        }
      }
      "get an Exporter" in {
        withCommand { command =>
          WithClose(ConvertCommand.getExporter(command.params, null))(_ must not(beNull))
        }
      }
      "convert File" in {
        withCommand { command =>
          val converter = ConvertCommand.getConverter(command.params, sft)
          val ec = converter.createEvaluationContext(Map("inputFilePath" -> inputFile))
          val files = Iterator.single(inputFile).flatMap(PathUtils.interpretPath)
          val features = ConvertCommand.convertFeatures(files, converter, ec, None, None)
          features.toSeq must haveLength(3)
        }
      }
      "export data" in {
        withCommand { command =>
          command.execute()
          command.params.file.length() must beGreaterThan(0L)
        }
      }
    }
  }
}
