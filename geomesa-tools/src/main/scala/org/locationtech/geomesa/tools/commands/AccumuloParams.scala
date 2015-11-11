/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/
package org.locationtech.geomesa.tools.commands

import java.util.regex.Pattern

import com.beust.jcommander.Parameter

class AccumuloParams {
  @Parameter(names = Array("-u", "--user"), description = "Accumulo user name", required = true)
  var user: String = null

  @Parameter(names = Array("-p", "--password"), description = "Accumulo password (will prompt if not supplied)")
  var password: String = null

  @Parameter(names = Array("-i", "--instance"), description = "Accumulo instance name")
  var instance: String = null

  @Parameter(names = Array("-z", "--zookeepers"), description = "Zookeepers (host[:port], comma separated)")
  var zookeepers: String = null

  @Parameter(names = Array("-a", "--auths"), description = "Accumulo authorizations")
  var auths: String = null

  @Parameter(names = Array("-v", "--visibilities"), description = "Accumulo scan visibilities")
  var visibilities: String = null

  @Parameter(names = Array("-mc", "--mock"), description = "Run everything with a mock accumulo instance instead of a real one (true/false)", arity = 1)
  var useMock: Boolean = false
}

class GeoMesaParams extends AccumuloParams {
  @Parameter(names = Array("-c", "--catalog"), description = "Catalog table name for GeoMesa", required = true)
  var catalog: String = null
}

class FeatureParams extends GeoMesaParams {
  @Parameter(names = Array("-fn", "--feature-name"), description = "Simple Feature Type name on which to operate", required = true)
  var featureName: String = null
}

class OptionalFeatureParams extends GeoMesaParams {
  @Parameter(names = Array("-fn", "--feature-name"), description = "Simple Feature Type name on which to operate", required = false)
  var featureName: String = null
}

class RequiredCqlFilterParameters extends FeatureParams {
  @Parameter(names = Array("-q", "--cql"), description = "CQL predicate", required = true)
  var cqlFilter: String = null
}

class OptionalCqlFilterParameters extends FeatureParams {
  @Parameter(names = Array("-q", "--cql"), description = "CQL predicate")
  var cqlFilter: String = null
}

class CreateFeatureParams extends FeatureParams {
  @Parameter(names = Array("-s", "--spec"), description = "SimpleFeatureType specification as a GeoTools spec string, SFT config, or file with either")
  var spec: String = null

  @Parameter(names = Array("-dt", "--dtg"), description = "DateTime field name to use as the default dtg")
  var dtgField: String = null

  @Parameter(names = Array("-st", "--use-shared-tables"), description = "Use shared tables in Accumulo for feature storage (true/false)", arity = 1)
  var useSharedTables: Boolean = true //default to true in line with datastore
}

class ForceParams {
  @Parameter(names = Array("-f", "--force"), description = "Force deletion without prompt", required = false)
  var force: Boolean = false
}

class PatternParams {
  @Parameter(names = Array("-pt", "--pattern"), description = "Regular expression to select items to delete", required = false)
  var pattern: Pattern = null
}

class RasterParams extends AccumuloParams {
  @Parameter(names = Array("-t", "--raster-table"), description = "Accumulo table for storing raster data", required = true)
  var table: String = null
}

class CreateRasterParams extends RasterParams {
  @Parameter(names = Array("-wm", "--write-memory"), description = "Memory allocation for ingestion operation")
  var writeMemory: String = null

  @Parameter(names = Array("-wt", "--write-threads"), description = "Threads for writing raster data")
  var writeThreads: Integer = null

  @Parameter(names = Array("-qt", "--query-threads"), description = "Threads for quering raster data")
  var queryThreads: Integer = null
}
