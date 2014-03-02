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

package geomesa.core

import org.apache.accumulo.core.data.{Value, Key}
import org.geotools.filter.identity.FeatureIdImpl
import org.joda.time.DateTime
import org.opengis.filter.identity.FeatureId
import org.geotools.data.DataUtilities

/**
 * These are package-wide constants.
 */
package object index {
  val MIN_DATE = new DateTime(Long.MinValue)
  val MAX_DATE = new DateTime(Long.MaxValue)

  val SF_PROPERTY_GEOMETRY = "geomesa_index_geometry"
  val SF_PROPERTY_START_TIME = "geomesa_index_start_time"
  val SF_PROPERTY_END_TIME   = "geomesa_index_end_time"

  val spec = "geomesa_index_geometry:Geometry:srid=4326,geomesa_index_start_time:Date,geomesa_index_end_time:Date"
  val indexSFT = DataUtilities.createType("geomesa-idx", spec)

  implicit def string2id(s: String): FeatureId = new FeatureIdImpl(s)

  type KeyValuePair = (Key, Value)
}

