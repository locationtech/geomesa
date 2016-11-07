/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.common

import java.io.File
import java.util
import java.util.regex.Pattern

import com.beust.jcommander.Parameter

/**
  * Shared parameters as individual traits
  */

trait FeatureTypeNameParam {
  @Parameter(names = Array("-f", "--feature-name"), description = "Simple Feature Type name on which to operate", required = true)
  var featureName: String = null
}

trait OptionalFeatureTypeNameParam {
  @Parameter(names = Array("-f", "--feature-name"), description = "Simple Feature Type name on which to operate")
  var featureName: String = null
}

trait OptionalPatternParam {
  @Parameter(names = Array("--pattern"), description = "Regular expression to select items to delete")
  var pattern: Pattern = null
}

trait OptionalForceParam {
  @Parameter(names = Array("--force"), description = "Force deletion without prompt")
  var force: Boolean = false
}

trait FeatureTypeSpecParam {
  @Parameter(names = Array("-s", "--spec"),
    description = "SimpleFeatureType specification as a GeoTools spec string, SFT config, or file with either", required = true)
  var spec: String = null
}

trait OptionalFeatureTypeSpecParam {
  @Parameter(names = Array("-s", "--spec"), description = "SimpleFeatureType specification as a GeoTools spec string, SFT config, or file with either")
  var spec: String = null
}

trait CQLFilterParam {
  @Parameter(names = Array("-q", "--cql"), description = "CQL predicate", required = true)
  var cqlFilter: String = null
}

trait OptionalCQLFilterParam {
  @Parameter(names = Array("-q", "--cql"), description = "CQL predicate")
  var cqlFilter: String = null
}

trait OptionalDTGParam {
  @Parameter(names = Array("--dtg"), description = "DateTime field name to use as the default dtg")
  var dtgField: String = null
}

trait OptionalAttributesParam {
  @Parameter(names = Array("-a", "--attributes"), description = "Attributes to evaluate (comma-separated)")
  var attributes: java.util.List[String] = null
}

trait AttributesParam {
  @Parameter(names = Array("-a", "--attributes"), description = "Attributes to evaluate (comma-separated)", required = true)
  var attributes: java.util.List[String] = null
}

trait ZookeepersParam {
  @Parameter(names = Array("-z", "--zookeepers"), description = "Zookeepers (host[:port], comma separated)", required = true)
  var zookeepers: String = null
}

trait OptionalZookeepersParam {
  @Parameter(names = Array("-z", "--zookeepers"), description = "Zookeepers (host[:port], comma separated)")
  var zookeepers: String = null
}

trait DateAttributeParam {
  @Parameter(names = Array("--dt-attribute"), description = "[Bin-Export] Name of the date attribute to export", required = true)
  var dateAttribute: String = null
}

trait OptionalDateAttributeParam {
  @Parameter(names = Array("--dt-attribute"), description = "[Bin-Export] Name of the date attribute to export")
  var dateAttribute: String = null
}

trait InputFileParams {
  @Parameter(names = Array("-C", "--converter"), description = "GeoMesa converter specification as a config string, file name, or name of an available converter")
  var config: String = null

  @Parameter(names = Array("--input-format"), description = "File format of input files (shp, csv, tsv, avro, etc). Optional, autodetection will be attempted.")
  var format: String = null

  @Parameter(description = "<file>...", required = true)
  var files: java.util.List[String] = new util.ArrayList[String]()
}

trait BaseExportCommands extends OptionalFeatureTypeNameParam with OptionalCQLFilterParam {
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

  @Parameter(names = Array("-F","--output-Format"), description = "File format of output files (csv|tsv|gml|json|shp|avro)")
  var outputFormat: String = "csv"

  @Parameter(names = Array("--no-header"), description = "Export as a delimited text format (csv|tsv) without a type header", required = false)
  var noHeader: Boolean = false
}

trait BaseBinaryExportParameters {
  @Parameter(names = Array("--dt-attribute"), description = "[Bin Export Only] Name of the date attribute to export. default: dtg")
  var dateAttribute: String = null

  @Parameter(names = Array("--id-attribute"), description = "[Bin Export Only] Name of the id attribute to export")
  var idAttribute: String = null

  @Parameter(names = Array("--lat-attribute"), description = "[Bin Export Only] Name of the latitude attribute to export")
  var latAttribute: String = null

  @Parameter(names = Array("--lon-attribute"), description = "[Bin Export Only] Name of the longitude attribute to export")
  var lonAttribute: String = null

  @Parameter(names = Array("--label-attribute"), description = "[Bin Export Only] Name of the attribute to use as a bin file label")
  var labelAttribute: String = null
}