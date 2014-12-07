/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.locationtech.geomesa.tools.commands

import java.io._

import com.beust.jcommander.{Parameters, JCommander, Parameter}
import com.typesafe.scalalogging.slf4j.Logging
import org.geotools.data.Query
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.core.data.AccumuloFeatureStore
import org.locationtech.geomesa.tools.Utils.Formats
import org.locationtech.geomesa.tools.Utils.Formats._
import org.locationtech.geomesa.tools._
import org.locationtech.geomesa.tools.commands.ExportCommand.{Command, ExportParameters}
import org.opengis.filter.Filter

import scala.util.Try

class ExportCommand(parent: JCommander) extends Command with Logging {

  val params = new ExportParameters
  parent.addCommand(Command, params)

  override def execute() = {

    val fmt = params.format.toLowerCase()
    val features = getFeatureCollection(fmt)
    val exporter: FeatureExporter = fmt match {
      case CSV | TSV       => DelimitedExport(getWriter(), params)
      case SHP             => new ShapefileExport(getFile())
      case GeoJson | JSON  => new GeoJsonExport(getWriter())
      case GML             => new GmlExport(getOutputStream())
      case _ =>
        throw new IllegalArgumentException(s"Unsupported export format. Supported formats are: ${Formats.All.mkString(",")}.")
    }
    try {
      exporter.write(features)
      logger.info("Feature export complete to " + (if (usefile) params.file.getPath else "standard out"))
    } finally {
      exporter.flush()
      exporter.close()
    }
  }

  def getFeatureCollection(fmt: String): SimpleFeatureCollection = {
    fmt match {
      case SHP =>
        val schemaString =
          if (Option(params.attributes).nonEmpty) {
            params.attributes
          } else {
            val sft = new DataStoreHelper(params).ds.getSchema(params.featureName)
            ShapefileExport.modifySchema(sft)
          }
        getFeatureCollection(Some(schemaString))

      case _ => getFeatureCollection(Option(params.attributes))
    }
  }

  def getFeatureCollection(overrideAttributes: Option[String] = None): SimpleFeatureCollection = {
    val filter = Option(params.cqlFilter).map(ECQL.toFilter).getOrElse(Filter.INCLUDE)
    val q = new Query(params.featureName, filter)
    Option(params.maxFeatures).foreach(q.setMaxFeatures(_))

    // If there are override attributes given as an arg or via command line params
    // split attributes by "," meanwhile allowing to escape it by "\,".
    overrideAttributes.orElse(Option(params.attributes)).foreach { attributes =>
      q.setPropertyNames(attributes.split("""(?<!\\),""").map(_.trim.replace("\\,", ",")))
    }

    // get the feature store used to query the GeoMesa data
    val fs = new DataStoreHelper(params).ds.getFeatureSource(params.featureName).asInstanceOf[AccumuloFeatureStore]

    // and execute the query
    Try(fs.getFeatures(q)).getOrElse{
      throw new Exception("Error: Could not create a SimpleFeatureCollection to export. Please ensure " +
        "that all arguments are correct in the previous command.")
    }
  }

  def getOutputStream(): OutputStream =
    Option(params.file) match {
      case Some(file) => new FileOutputStream(file)
      case None       => System.out
    }
  
  def getWriter(): Writer = new BufferedWriter(new OutputStreamWriter(getOutputStream()))
  
  def getFile(): File = Option(params.file) match {
    case Some(file) => file
    case None       => throw new Exception("Error: --file option required")
  }

  def usefile = Option(params.file).nonEmpty
}

object ExportCommand {
  val Command = "export"

  @Parameters(commandDescription = "Export a GeoMesa feature")
  class ExportParameters extends OptionalCqlFilterParameters {
    @Parameter(names = Array("-fmt", "--format"), description = "Format to export (csv|tsv|gml|json|shp)")
    var format: String = "csv"

    @Parameter(names = Array("-max", "--maxFeatures"), description = "Maximum number of features to return. default: Long.MaxValue")
    var maxFeatures: Integer = Int.MaxValue

    @Parameter(names = Array("-at", "--attributes"), description = "Attributes from feature to export " +
      "(comma-separated)...Comma-separated expressions with each in the format " +
      "attribute[=filter_function_expression]|derived-attribute=filter_function_expression. " +
      "filter_function_expression is an expression of filter function applied to attributes, literals " +
      "and filter functions, i.e. can be nested")
    var attributes: String = null

    @Parameter(names = Array("-id", "--idAttribute"), description = "name of the id attribute to export")
    var idAttribute: String = null

    @Parameter(names = Array("-lat", "--latAttribute"), description = "name of the latitude attribute to export")
    var latAttribute: String = null

    @Parameter(names = Array("-lon", "--lonAttribute"), description = "name of the longitude attribute to export")
    var lonAttribute: String = null

    @Parameter(names = Array("-dt", "--dtAttribute"), description = "name of the date attribute to export")
    var dateAttribute: String = null

    @Parameter(names = Array("-o", "--output"), description = "name of the file to output to instead of std out")
    var file: File = null
  }
}
