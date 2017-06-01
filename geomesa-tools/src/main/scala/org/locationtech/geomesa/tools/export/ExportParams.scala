/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.export

import java.io.File

import com.beust.jcommander.Parameter
import org.locationtech.geomesa.tools.utils.ParameterConverters.HintConverter
import org.locationtech.geomesa.tools.{CatalogParam, OptionalCqlFilterParam, OptionalIndexParam, RequiredTypeNameParam}

trait FileExportParams extends OptionalCqlFilterParam {
  @Parameter(names = Array("-o", "--output"), description = "Output to a file instead of std out")
  var file: File = _

  @Parameter(names = Array("--gzip"), description = "Level of gzip compression to apply to output, from 1-9")
  var gzip: Integer = _

  @Parameter(names = Array("-F", "--output-format"), description = "File format of output files (csv|tsv|gml|json|shp|avro)")
  var outputFormat: String = "csv"

  @Parameter(names = Array("--no-header"), description = "Export as a delimited text format (csv|tsv) without a type header", required = false)
  var noHeader: Boolean = false

  @Parameter(names = Array("-m", "--max-features"), description = "Maximum number of features to return. default: Unlimited")
  var maxFeatures: Integer = _

  @Parameter(names = Array("--hints"), description = "Query hints to set, in the form key1=value1;key2=value2", required = false, converter = classOf[HintConverter])
  var hints: java.util.Map[String, String] = _
}

trait DataExportParams extends FileExportParams {
  @Parameter(names = Array("-a", "--attributes"), description = "Attributes from feature to export " +
    "(comma-separated)...Comma-separated expressions with each in the format " +
    "attribute[=filter_function_expression]|derived-attribute=filter_function_expression|'id'. " +
    "'id' will export the feature ID, filter_function_expression is an expression of filter function applied " +
      "to attributes, literals and filter functions, i.e. can be nested")
  var attributes: java.util.List[String] = _
}

// export from a datastore
trait ExportParams extends DataExportParams with CatalogParam with RequiredTypeNameParam with OptionalIndexParam
