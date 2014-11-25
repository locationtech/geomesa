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

package org.locationtech.geomesa.raster

import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes

package object index {
  val rasterSpec = "geom:Geometry:srid=4326,dtg:Date,dtg_end_time:Date"
  val rasterIndexSFT = SimpleFeatureTypes.createType("geomesa-raster-idx", rasterSpec)
}
