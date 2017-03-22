/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools

import java.io.{File, PrintWriter}

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.FileUtils
import org.geotools.feature.DefaultFeatureCollection
import org.junit.runner.RunWith
import org.locationtech.geomesa.convert.SimpleFeatureConverter
import org.locationtech.geomesa.tools.ConvertParameters.ConvertParameters
import org.locationtech.geomesa.tools.export.formats.FeatureExporter
import org.locationtech.geomesa.tools.utils.{CLArgResolver, DataFormats}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.io.Source

@RunWith(classOf[JUnitRunner])
class ConvertCommandTest extends Specification with LazyLogging {

  sequential

  val testCSV =
    """
      |ID,Name,Age,LastSeen,Friends,Talents,Lat,Lon,Vis
      |23623,Harry,20,2015-05-06,"Will, Mark, Suzan","patronus->10,expelliarmus->9",-100.236523,23,user
      |26236,Hermione,25,2015-06-07,"Edward, Bill, Harry","accio->10",40.232,-53.2356,user
      |3233,Severus,30,2015-10-23,"Tom, Riddle, Voldemort","potions->10",3,-62.23,user&admin
      |
    """.stripMargin
  val testCSVFile = File.createTempFile("ConvertCommandTestData", ".csv")
  new PrintWriter(testCSVFile) { write(testCSV); close() }
  val csvConf =
    """
      |geomesa {
      |  sfts {
      |    "example" = {
      |      attributes = [
      |        { name = "fid",      type = "Integer",         index = false                             }
      |        { name = "name",     type = "String",          index = true                              }
      |        { name = "age",      type = "Integer",         index = false                             }
      |        { name = "lastseen", type = "Date",            index = true                              }
      |        { name = "friends",  type = "List[String]",    index = true                              }
      |        { name = "talents",  type = "Map[String,Int]", index = false                             }
      |        { name = "geom",     type = "Point",           index = true, srid = 4326, default = true }
      |      ]
      |    }
      |  }
      |  converters {
      |    "example-csv" {
      |      type   = "delimited-text",
      |      format = "CSV",
      |      options {
      |        skip-lines = 1
      |      },
      |      id-field = "toString($fid)",
      |      fields = [
      |        { name = "fid",      transform = "$1::int"                     }
      |        { name = "name",     transform = "$2::string"                  }
      |        { name = "age",      transform = "$3::int"                     }
      |        { name = "lastseen", transform = "date('YYYY-MM-dd', $4)"      }
      |        { name = "friends",  transform = "parseList('String', $5)"     }
      |        { name = "talents",  transform = "parseMap('String->Int', $6)" }
      |        { name = "lon",      transform = "$7::double"                  }
      |        { name = "lat",      transform = "$8::double"                  }
      |        { name = "geom",     transform = "point($lon, $lat)"           }
      |      ]
      |    }
      |  }
      |}
    """.stripMargin

  val testTSV =
    """
      |ID	Name	Age	LastSeen	Friends Talents Lat	Lon	Vis
      |23623	Harry	20	2015-05-06	"Will, Mark, Suzan"	"patronus->10,expelliarmus->9"	-100.236523	23	user
      |26236	Hermione	25	2015-06-07	"Edward, Bill, Harry"	"accio->10"	40.232	-53.2356	user
      |3233	Severus	30	2015-10-23	"Tom, Riddle, Voldemort"	"potions->10"	3	-62.23	user&admin
      |
    """.stripMargin
  val testTSVFile = File.createTempFile("ConvertCommandTestData", ".tsv")
  new PrintWriter(testTSVFile) { write(testTSV); close() }
  val tsvConf =
    """
      |geomesa {
      |  sfts {
      |    "example" = {
      |      attributes = [
      |        { name = "fid",      type = "Integer",         index = false                             }
      |        { name = "name",     type = "String",          index = true                              }
      |        { name = "age",      type = "Integer",         index = false                             }
      |        { name = "lastseen", type = "Date",            index = true                              }
      |        { name = "friends",  type = "List[String]",    index = true                              }
      |        { name = "talents",  type = "Map[String,Int]", index = false                             }
      |        { name = "geom",     type = "Point",           index = true, srid = 4326, default = true }
      |      ]
      |    }
      |  }
      |  converters {
      |    "example-tsv" {
      |      type   = "delimited-text",
      |      format = "TSV",
      |      options {
      |        skip-lines = 1
      |      },
      |      id-field = "toString($fid)",
      |      fields = [
      |        { name = "fid",      transform = "$1::int"                     }
      |        { name = "name",     transform = "$2::string"                  }
      |        { name = "age",      transform = "$3::int"                     }
      |        { name = "lastseen", transform = "date('YYYY-MM-dd', $4)"      }
      |        { name = "friends",  transform = "parseList('String', $5)"     }
      |        { name = "talents",  transform = "parseMap('String->Int', $6)" }
      |        { name = "lon",      transform = "$7::double"                  }
      |        { name = "lat",      transform = "$8::double"                  }
      |        { name = "geom",     transform = "point($lon, $lat)"           }
      |      ]
      |    }
      |  }
      |}
    """.stripMargin

  val testJSON =
    """
      |{
      |	DataSource: { name: "example-json" },
      |	Features: [
      |		{
      |			fid: 23623,
      |			name: "Harry",
      |			age: 20,
      |			lastseen: "2015-05-06",
      |			friends: "Will, Mark, Suzan",
      |			talents: "patronus->10,expelliarmus->9",
      |			lat: -100.236523,
      |			lon: 23,
      |			vis: "user"
      |		},
      |		{
      |			fid: 26236,
      |			name: "Hermione",
      |			age: 25,
      |			lastseen: "2015-06-07",
      |			friends: "Edward, Bill, Harry",
      |			talents: "accio->10",
      |			lat: 40.232,
      |			lon: -53.2356,
      |			vis: "user"
      |		},
      |		{
      |			fid: 3233,
      |			name: "Severus",
      |			age: 30,
      |			lastseen: "2015-10-23",
      |			friends: "Tom, Riddle, Voldemort",
      |			talents: "potions->10",
      |			lat: 3,
      |			lon: -62.23,
      |			vis: "user&admin"
      |		}
      |	]
      |}
    """.stripMargin
  val testJSONFile = File.createTempFile("ConvertCommandTestData", ".json")
  new PrintWriter(testJSONFile) { write(testJSON); close() }
  val jsonConf =
    """
      |geomesa {
      |  sfts {
      |    "example-json" = {
      |      attributes = [
      |        { name = "id",   type = "Integer" }
      |        { name = "name", type = "String"  }
      |        { name = "age",  type = "Integer" }
      |        { name = "geom", type = "Point"   }
      |      ]
      |    }
      |  }
      |  converters {
      |    "example-json" = {
      |      type         = "json"
      |      id-field     = "$id"
      |      feature-path = "$.Features[*]"
      |      options {
      |        line-mode = "multi"
      |      }
      |      fields = [
      |        { name = "id",   json-type = "integer", path = "$.fid",  transform = "toString($0)"      }
      |        { name = "name", json-type = "string",  path = "$.name",                                 }
      |        { name = "age",  json-type = "integer", path = "$.age",                                  }
      |        { name = "lat",  json-type = "double",  path = "$.lat",                                  }
      |        { name = "lon",  json-type = "double",  path = "$.lon",                                  }
      |        { name = "geom",                                         transform = "point($lon, $lat)" }
      |      ]
      |    }
      |  }
      |}
    """.stripMargin

  val formats: Array[String] = DataFormats.values.filter(_ != DataFormats.Null).map(_.toString).toArray
  try {
    for (x <- formats; y <- formats) {
      logger.debug(s"Testing $x to $y converter")
      testPair(x, y)
    }
  } finally {
    FileUtils.deleteQuietly(testCSVFile)
    FileUtils.deleteQuietly(testJSONFile)
    FileUtils.deleteQuietly(testTSVFile)
    FileUtils.deleteQuietly(testCSVFile)
    FileUtils.deleteQuietly(testCSVFile)
    FileUtils.deleteQuietly(testCSVFile)
    FileUtils.deleteQuietly(testCSVFile)
    FileUtils.deleteQuietly(testCSVFile)
  }

  def getInputFileandConf(fmt: String): (String, String) = {
    fmt.toLowerCase match {
      case "avro"    => (testCSVFile.toString, csvConf)
      case "bin"     => (testCSVFile.toString, csvConf)
      case "csv"     => (testCSVFile.toString, csvConf)
      case "geojson" => (testCSVFile.toString, csvConf)
      case "gml"     => (testCSVFile.toString, csvConf)
      case "json"    => (testJSONFile.toString, jsonConf)
      case "shp"     => (testCSVFile.toString, csvConf)
      case "tsv"     => (testTSVFile.toString, tsvConf)
    }
  }

  def testPair(inFmt: String, outFmt: String): Unit ={
    s"Convert Command should convert $inFmt -> $outFmt" in {
      val params = new ConvertParameters
      val (inputFile, conf) = getInputFileandConf(inFmt)
      params.files.add(inputFile)
      params.config = conf
      params.spec = conf
      params.outputFormat = outFmt
      params.file = File.createTempFile("convertTest", s".$outFmt")
      val sft = CLArgResolver.getSft(params.spec)

      "get a Converter" in {
        val converter = ConvertCommand.getConverter(params, sft)
        converter must not beNull;
        converter must beAnInstanceOf[SimpleFeatureConverter[Any]]
      }
      "get an Exporter" in {
        val exporter = ConvertCommand.getExporter(params, sft)
        exporter must not beNull;
        exporter must beAnInstanceOf[FeatureExporter]
      }
      "convert File" in {
        val fc = new DefaultFeatureCollection(sft.getTypeName, sft)
        val converter = ConvertCommand.getConverter(params, sft)
        ConvertCommand.convertFile(testCSVFile.toString, converter, params, fc)
        fc.size() must beEqualTo(3)
      }

      "export data" in {
        val fc = new DefaultFeatureCollection(sft.getTypeName, sft)
        val converter = ConvertCommand.getConverter(params, sft)
        val exporter = ConvertCommand.getExporter(params, sft)
        ConvertCommand.convertFile(testCSVFile.toString, converter, params, fc)
        exporter.export(fc)
        val res = Source.fromFile(params.file)
        res.toString() must not beNull;
      }

      FileUtils.deleteQuietly(params.file)
    }
  }
}
