/*
 * Copyright 2013 Commonwealth Computer Research, Inc.
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

package geomesa

package object core {

  // This first string is used as a SimpleFeature attribute name.
  //  Since we would like to be able to use ECQL filters, we are restricted to letters, numbers, and _'s.
  val DEFAULT_GEOMETRY_PROPERTY_NAME = "SF_PROPERTY_GEOMETRY"
  val DEFAULT_DTG_PROPERTY_NAME = "dtg"
  val DEFAULT_DTG_END_PROPERTY_NAME = "dtg_end_time"

  val DEFAULT_FEATURE_TYPE = "geomesa.feature.type"
  val DEFAULT_SCHEMA_NAME  = "geomesa.index.schema"
  val DEFAULT_FEATURE_NAME = "geomesa.index.feature"

  val INGEST_TABLE_NAME                      = "geomesa.ingest.table"
  val DEFAULT_FILTER_PROPERTY_NAME           = "geomesa.index.filter"
  val DEFAULT_INTERVAL_PROPERTY_NAME         = "geomesa.index.interval"
  val DEFAULT_ATTRIBUTE_NAMES                = "geomesa.index.shapefile.attribute-names"
  val DEFAULT_CACHE_SIZE_NAME                = "geomesa.index.cache-size"
  val DEFAULT_CACHE_TABLE_NAME               = "geomesa.index.cache-table"
  val DEFAULT_AGGREGATOR_CLASS_PROPERTY_NAME = "geomesa.iterators.aggregator-class"

  val GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE = "geomesa.iterators.aggregator-types"
  val GEOMESA_ITERATORS_ECQL_FILTER         = "geomesa.iterators.ecql-filter"
  val GEOMESA_ITERATORS_TRANSFORM           = "geomesa.iterators.transform"
  val GEOMESA_ITERATORS_TRANSFORM_SCHEMA    = "geomesa.iterators.transform.schema"

}
