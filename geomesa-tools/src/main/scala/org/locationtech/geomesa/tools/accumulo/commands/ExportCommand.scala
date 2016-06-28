/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.accumulo.commands

import java.io._
import java.util.Locale
import java.util.zip.{Deflater, GZIPOutputStream}

import com.beust.jcommander.{JCommander, Parameter, ParameterException, Parameters}
import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.Query
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.accumulo.data.AccumuloFeatureStore
import org.locationtech.geomesa.tools.accumulo.Utils.Formats
import org.locationtech.geomesa.tools.accumulo.Utils.Formats._
import org.locationtech.geomesa.tools.accumulo._
import org.locationtech.geomesa.tools.accumulo.commands.ExportCommand.ExportParameters
import org.locationtech.geomesa.tools.common.{FeatureTypeNameParam, OptionalCQLFilterParam}
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.opengis.filter.Filter

import scala.util.{Failure, Success, Try}

class ExportCommand(parent: JCommander) extends CommandWithCatalog(parent) with LazyLogging {

  override val command = "export"
  override val params = new ExportParameters

  override def execute() = {
    val fmt = Formats.withName(params.format.toLowerCase(Locale.US))
    val features = getFeatureCollection(fmt)
    lazy val avroCompression = Option(params.gzip).map(_.toInt).getOrElse(Deflater.DEFAULT_COMPRESSION)
    val exporter: FeatureExporter = fmt match {
      case CSV | TSV      => new DelimitedExport(getWriter(), fmt)
      case SHP            => new ShapefileExport(checkShpFile())
      case GeoJson | JSON => new GeoJsonExport(getWriter())
      case GML            => new GmlExport(createOutputStream())
      case BIN            => BinFileExport(createOutputStream(), params)
      case AVRO           => new AvroExport(createOutputStream(true), features.getSchema, avroCompression)
      // shouldn't happen unless someone adds a new format and doesn't implement it here
      case _              => throw new UnsupportedOperationException(s"Format $fmt can't be exported")
    }
    try {
      exporter.write(features)
      logger.info(s"Feature export complete to ${Option(params.file).map(_.getPath).getOrElse("standard out")}")
    } finally {
      exporter.flush()
      exporter.close()
      ds.dispose()
    }
  }

  def getFeatureCollection(fmt: Formats): SimpleFeatureCollection = {
    lazy val sft = ds.getSchema(params.featureName)
    fmt match {
      case SHP =>
        val schemaString =
          if (params.attributes == null) {
            ShapefileExport.modifySchema(sft)
          } else {
            ShapefileExport.replaceGeomInAttributesString(params.attributes, sft)
          }
        getFeatureCollection(Some(schemaString))
      case BIN =>
        sft.getDtgField.foreach(BinFileExport.DEFAULT_TIME = _)
        getFeatureCollection(Some(BinFileExport.getAttributeList(params)))
      case _ => getFeatureCollection()
    }
  }

  def getFeatureCollection(overrideAttributes: Option[String] = None): SimpleFeatureCollection = {
    val filter = Option(params.cqlFilter).map(ECQL.toFilter).getOrElse(Filter.INCLUDE)
    logger.debug(s"Applying CQL filter ${filter.toString}")
    val q = new Query(params.featureName, filter)
    Option(params.maxFeatures).foreach(q.setMaxFeatures(_))

    // If there are override attributes given as an arg or via command line params
    // split attributes by "," meanwhile allowing to escape it by "\,".
    overrideAttributes.orElse(Option(params.attributes)).foreach { attributes =>
      val splitAttrs = attributes.split("""(?<!\\),""").map(_.trim.replace("\\,", ","))
      logger.debug("Attributes used for query transform: " + splitAttrs.mkString("|"))
      q.setPropertyNames(splitAttrs)
    }

    // get the feature store used to query the GeoMesa data
    val fs = ds.getFeatureSource(params.featureName).asInstanceOf[AccumuloFeatureStore]

    // and execute the query
    Try(fs.getFeatures(q)) match {
      case Success(fc) => fc
      case Failure(ex) =>
        throw new Exception("Error: Could not create a SimpleFeatureCollection to export. Please ensure " +
        "that all arguments are correct in the previous command.", ex)
    }
  }

  def createOutputStream(skipCompression: Boolean = false): OutputStream = {
    val out = if (params.file == null) System.out else new FileOutputStream(params.file)
    val compressed = if (skipCompression || params.gzip == null) out else new GZIPOutputStream(out) {
      `def`.setLevel(params.gzip) // hack to access the protected deflate level
    }
    new BufferedOutputStream(compressed)
  }

  // noinspection AccessorLikeMethodIsEmptyParen
  def getWriter(): Writer = new OutputStreamWriter(createOutputStream())
  
  def checkShpFile(): File = {
    if (params.file != null) {
      params.file
    } else {
      throw new ParameterException("Error: -o or --output for file-based output is required for " +
          "shapefile export (stdout not supported for shape files)")
    }
  }
}

object ExportCommand {
  @Parameters(commandDescription = "Export a GeoMesa feature")
  class ExportParameters extends GeoMesaConnectionParams
    with FeatureTypeNameParam
    with OptionalCQLFilterParam {

    @Parameter(names = Array("-F", "--format"), description = "Format to export (csv|tsv|gml|json|shp|bin|avro)")
    var format: String = "csv"

    @Parameter(names = Array("-m", "--max-features"), description = "Maximum number of features to return. default: Unlimited")
    var maxFeatures: Integer = null

    @Parameter(names = Array("-a", "--attributes"), description = "Attributes from feature to export " +
      "(comma-separated)...Comma-separated expressions with each in the format " +
      "attribute[=filter_function_expression]|derived-attribute=filter_function_expression. " +
      "filter_function_expression is an expression of filter function applied to attributes, literals " +
      "and filter functions, i.e. can be nested")
    var attributes: String = null

    @Parameter(names = Array("--gzip"), description = "level of gzip compression to apply to output, from 1-9")
    var gzip: Integer = null

    @Parameter(names = Array("--id-attribute"), description = "name of the id attribute to export")
    var idAttribute: String = null

    @Parameter(names = Array("--lat-attribute"), description = "name of the latitude attribute to export")
    var latAttribute: String = null

    @Parameter(names = Array("--lon-attribute"), description = "name of the longitude attribute to export")
    var lonAttribute: String = null

    @Parameter(names = Array("--dt-attribute"), description = "name of the date attribute to export")
    var dateAttribute: String = null

    @Parameter(names = Array("--label-attribute"), description = "name of the attribute to use as a bin file label")
    var labelAttribute: String = null

    @Parameter(names = Array("-o", "--output"), description = "name of the file to output to instead of std out")
    var file: File = null
  }
}
