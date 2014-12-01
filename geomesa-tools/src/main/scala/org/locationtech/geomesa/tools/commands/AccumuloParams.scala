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

  @Parameter(names = Array("--mock"), description = "Run everything with a mock accumulo instance instead of a real one")
  var useMock: Boolean = false
}

class GeoMesaParams extends AccumuloParams {
  @Parameter(names = Array("-c", "--catalog"), description = "Catalog table name for GeoMesa", required = true)
  var catalog: String = null
}

class FeatureParams extends GeoMesaParams{
  @Parameter(names = Array("-f", "--feature-name"), description = "Simple Feature Type name on which to operate", required = true)
  var featureName: String = null
}

class RequiredCqlFilterParameters extends FeatureParams{
  @Parameter(names = Array("-q", "--cql"), description = "CQL predicate", required = true)
  var cqlFilter: String = null
}

class OptionalCqlFilterParameters extends FeatureParams{
  @Parameter(names = Array("-q", "--cql"), description = "CQL predicate")
  var cqlFilter: String = null
}

class CreateFeatureParams extends FeatureParams {
  @Parameter(names = Array("-s", "--spec"), description = "SimpleFeatureType specification", required = true)
  var spec: String = null

  @Parameter(names = Array("--dtg"), description = "DateTime field name to use as the default dtg")
  var dtgField: String = null

  @Parameter(names = Array("--useSharedTables"), description = "Use shared tables in Accumulo for feature storage")
  var useSharedTables: Boolean = false

  @Parameter(names = Array("--shards"), description = "Number of shards to use for the storage tables (defaults to number of tservers)")
  var numShards: Integer = null
}