/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.common.commands

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

trait ZookeepersParam {
  @Parameter(names = Array("-z", "--zookeepers"), description = "Zookeepers (host[:port], comma separated)", required = true)
  var zookeepers: String = null
}

trait OptionalZookeepersParam {
  @Parameter(names = Array("-z", "--zookeepers"), description = "Zookeepers (host[:port], comma separated)")
  var zookeepers: String = null
}