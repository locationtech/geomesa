/***********************************************************************
  * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
  * All rights reserved. This program and the accompanying materials
  * are made available under the terms of the Apache License, Version 2.0
  * which accompanies this distribution and is available at
  * http://www.opensource.org/licenses/apache2.0.php.
  *************************************************************************/

package org.locationtech.geomesa.tools.accumulo.commands

import java.io._
import java.util.zip.GZIPOutputStream

import com.beust.jcommander.{Parameter, ParameterException}
import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.Query
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloFeatureStore}
import org.locationtech.geomesa.tools.accumulo.Utils.Formats._
import org.locationtech.geomesa.tools.accumulo._
import org.locationtech.geomesa.tools.common.{FeatureTypeNameParam, OptionalCQLFilterParam}
import org.opengis.filter.Filter
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

import scala.util.{Failure, Success, Try}

trait ExportCommandTools[P <: ExportCommandToolsParam] extends LazyLogging {

  def getFeatureCollection(fmt: Formats, ds: AccumuloDataStore, params: P):
    SimpleFeatureCollection = {
    lazy val sft = ds.getSchema(params.featureName)
    fmt match {
      case SHP =>
        val schemaString =
          if (params.attributes == null) {
            ShapefileExport.modifySchema(sft)
          } else {
            ShapefileExport.replaceGeomInAttributesString(params.attributes, sft)
          }
        getFeatureCollection(Some(schemaString), ds, params)
      case BIN =>
        sft.getDtgField.foreach(BinFileExport.DEFAULT_TIME = _)
        getFeatureCollection(Some(BinFileExport.getAttributeList(params)), ds, params)
      case _ => getFeatureCollection(None, ds, params)
    }
  }

  def getFeatureCollection(overrideAttributes: Option[String] = None, ds: AccumuloDataStore, params: P):
    SimpleFeatureCollection = {
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

  def createOutputStream(skipCompression: Boolean = false, params: P): OutputStream = {
    val out = if (params.file == null) System.out else new FileOutputStream(params.file)
    val compressed = if (skipCompression || params.gzip == null) out else new GZIPOutputStream(out) {
      `def`.setLevel(params.gzip) // hack to access the protected deflate level
    }
    new BufferedOutputStream(compressed)
  }

  // noinspection AccessorLikeMethodIsEmptyParen
  def getWriter(params: P): Writer = new OutputStreamWriter(createOutputStream(false, params))

  def checkShpFile(params: P): File = {
    if (params.file != null) {
      params.file
    } else {
      throw new ParameterException("Error: -o or --output for file-based output is required for " +
        "shapefile export (stdout not supported for shape files)")
    }
  }
}

trait ExportCommandToolsParam extends GeoMesaConnectionParams
  with FeatureTypeNameParam
  with OptionalCQLFilterParam {

  @Parameter(names = Array("-m", "--max-features"), description = "Maximum number of features to return. default: Unlimited")
  var maxFeatures: Integer = null

  @Parameter(names = Array("-a", "--attributes"), description = "Attributes from feature to export " +
    "(comma-separated)...Comma-separated expressions with each in the format " +
    "attribute[=filter_function_expression]|derived-attribute=filter_function_expression. " +
    "filter_function_expression is an expression of filter function applied to attributes, literals " +
    "and filter functions, i.e. can be nested")
  var attributes: String = null

  @Parameter(names = Array("-o", "--output"), description = "name of the file to output to instead of std out")
  var file: File = null

  @Parameter(names = Array("--gzip"), description = "level of gzip compression to apply to output, from 1-9")
  var gzip: Integer = null
}
