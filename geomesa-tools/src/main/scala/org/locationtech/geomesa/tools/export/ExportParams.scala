/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.export

import java.io.File

import com.beust.jcommander.Parameter
import org.locationtech.geomesa.tools.{CatalogParam, OptionalCqlFilterParam, OptionalIndexParam, RequiredTypeNameParam, _}

trait FileExportParams {
  @Parameter(names = Array("-o", "--output"), description = "Output to a file instead of std out")
  var file: File = null

  @Parameter(names = Array("--gzip"), description = "Level of gzip compression to apply to output, from 1-9")
  var gzip: Integer = null

  @Parameter(names = Array("-F", "--output-format"), description = "File format of output files (csv|tsv|gml|json|shp|avro)")
  var outputFormat: String = "csv"

  @Parameter(names = Array("--no-header"), description = "Export as a delimited text format (csv|tsv) without a type header", required = false)
  var noHeader: Boolean = false
}

trait MaxFeaturesParam {
  @Parameter(names = Array("-m", "--max-features"), description = "Maximum number of features to return. default: Unlimited")
  var maxFeatures: Integer = null
}

trait DataExportParams extends OptionalCqlFilterParam with MaxFeaturesParam {
  @Parameter(names = Array("-a", "--attributes"), description = "Attributes from feature to export " +
    "(comma-separated)...Comma-separated expressions with each in the format " +
    "attribute[=filter_function_expression]|derived-attribute=filter_function_expression|'id'. " +
    "'id' will export the feature ID, filter_function_expression is an expression of filter function applied " +
      "to attributes, literals and filter functions, i.e. can be nested")
  var attributes: java.util.List[String] = null
}

trait BaseExportParams extends FileExportParams with DataExportParams with TypeNameParam with OptionalIndexParam

trait ExportParams extends BaseExportParams with CatalogParam with RequiredTypeNameParam

trait BaseBinExportParams {
  @Parameter(names = Array("--id-attribute"), description = "Name of the id attribute to export")
  var idAttribute: String = null

  @Parameter(names = Array("--lat-attribute"), description = "Name of the latitude attribute to export")
  var latAttribute: String = null

  @Parameter(names = Array("--lon-attribute"), description = "Name of the longitude attribute to export")
  var lonAttribute: String = null

  @Parameter(names = Array("--label-attribute"), description = "Name of the attribute to use as a bin file label")
  var labelAttribute: String = null
}

trait OptionalBinExportParams extends BaseBinExportParams {
  @Parameter(names = Array("--dt-attribute"), description = "Name of the date attribute to export")
  var dateAttribute: String = null
}

trait BinExportParams extends OptionalBinExportParams with BaseExportParams with RequiredTypeNameParam
