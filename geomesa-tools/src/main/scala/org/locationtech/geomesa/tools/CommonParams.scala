/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools

import java.util
import java.util.regex.Pattern

import com.beust.jcommander.Parameter

/**
  * Shared parameters as individual traits
  */

trait QueryParams extends CatalogParam with RequiredTypeNameParam with CqlFilterParam with OptionalAttributesParam

trait CatalogParam {
  @Parameter(names = Array("-c", "--catalog"), description = "Catalog table for GeoMesa datastore", required = true)
  var catalog: String = null
}

trait TypeNameParam {
  def featureName: String
}

trait RequiredTypeNameParam extends TypeNameParam {
  @Parameter(names = Array("-f", "--feature-name"), description = "Simple Feature Type name on which to operate", required = true)
  var featureName: String = null
}

trait OptionalTypeNameParam extends TypeNameParam {
  @Parameter(names = Array("-f", "--feature-name"), description = "Simple Feature Type name on which to operate")
  var featureName: String = null
}

trait FeatureSpecParam {
  def spec: String
}

trait RequiredFeatureSpecParam extends FeatureSpecParam {
  @Parameter(names = Array("-s", "--spec"),
    description = "SimpleFeatureType specification as a GeoTools spec string, SFT config, or file with either", required = true)
  var spec: String = null
}

trait OptionalFeatureSpecParam extends FeatureSpecParam {
  @Parameter(names = Array("-s", "--spec"), description = "SimpleFeatureType specification as a GeoTools spec string, SFT config, or file with either")
  var spec: String = null
}

trait CqlFilterParam {
  def cqlFilter: String
}

trait RequiredCqlFilterParam extends CqlFilterParam {
  @Parameter(names = Array("-q", "--cql"), description = "CQL predicate", required = true)
  var cqlFilter: String = null
}

trait OptionalCqlFilterParam extends CqlFilterParam {
  @Parameter(names = Array("-q", "--cql"), description = "CQL predicate")
  var cqlFilter: String = null
}

trait OptionalDtgParam {
  @Parameter(names = Array("--dtg"), description = "DateTime field name to use as the default dtg")
  var dtgField: String = null
}

trait AttributesParam {
  def attributes: java.util.List[String]
}
trait OptionalAttributesParam extends AttributesParam {
  @Parameter(names = Array("-a", "--attributes"), description = "Attributes to evaluate (comma-separated)")
  var attributes: java.util.List[String] = null
}

trait RequiredAttributesParam extends AttributesParam {
  @Parameter(names = Array("-a", "--attributes"), description = "Attributes to evaluate (comma-separated)", required = true)
  var attributes: java.util.List[String] = null
}

trait OptionalSharedTablesParam {
  @Parameter(names = Array("--use-shared-tables"), description = "Use shared tables for feature storage (true/false)", arity = 1)
  var useSharedTables: Boolean = true //default to true in line with datastore
}

trait OptionalForceParam {
  @Parameter(names = Array("--force"), description = "Force execution without prompt")
  var force: Boolean = false
}

trait OptionalPatternParam {
  @Parameter(names = Array("--pattern"), description = "Regular expression for simple feature type names")
  var pattern: Pattern = null
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