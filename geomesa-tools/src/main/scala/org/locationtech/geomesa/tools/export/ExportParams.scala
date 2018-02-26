/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.export

import java.io.File

import com.beust.jcommander.Parameter
import org.locationtech.geomesa.tools.utils.DataFormats
import org.locationtech.geomesa.tools.utils.DataFormats.DataFormat
import org.locationtech.geomesa.tools.utils.ParameterConverters.DataFormatConverter
import org.locationtech.geomesa.tools.{OptionalCqlFilterParam, QueryHintsParams}

trait AttributeParam {
  @Parameter(names = Array("-a", "--attributes"), description = "Attributes from feature to export (comma-separated)...Comma-separated expressions with each in the format attribute[=filter_function_expression]|derived-attribute=filter_function_expression|'id'. 'id' will export the feature ID, filter_function_expression is an expression of filter function applied to attributes, literals and filter functions, i.e. can be nested")
  var attributes: java.util.List[String] = _
}

// This provides evidence for ExportQueryParams that this variable will exist.
// We override this in FileExportParams and in LeafletExportCommands so we can
// provided different defaults and messages.
trait MaxFeaturesParam {
  def maxFeatures: Integer
}

trait ExportQueryParams extends MaxFeaturesParam with AttributeParam with OptionalCqlFilterParam with QueryHintsParams

trait FilePropertyParams extends MaxFeaturesParam with OptionalCqlFilterParam with QueryHintsParams {
  @Parameter(names = Array("-o", "--output"), description = "Output to a file instead of std out")
  var file: File = _

  @Parameter(names = Array("--gzip"), description = "Level of gzip compression to apply to output, from 1-9")
  var gzip: Integer = _

  @Parameter(names = Array("-F", "--output-format"), description = "File format of output files (csv|tsv|gml|json|shp|avro)", converter = classOf[DataFormatConverter])
  var outputFormat: DataFormat = DataFormats.Csv

  @Parameter(names = Array("--no-header"), description = "Export as a delimited text format (csv|tsv) without a type header", required = false)
  var noHeader: Boolean = false

  @Parameter(names = Array("-m", "--max-features"), description = "Maximum number of features to return. Default: Unlimited")
  var maxFeatures: Integer = _
}

trait FileExportParams extends FilePropertyParams with ExportQueryParams

trait LeafletExportParams extends ExportQueryParams {
  @Parameter(names = Array("-o", "--output"), description = "(Optional) Output directory to store files.")
  var file: File = _

  @Parameter(names = Array("-m", "--max-features"), description = "Maximum number of features to return. A high limit will cause performance issues, use this parameter with caution.")
  var maxFeatures: Integer = 10000
}
