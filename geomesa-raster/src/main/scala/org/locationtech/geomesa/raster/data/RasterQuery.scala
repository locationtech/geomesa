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

package org.locationtech.geomesa.raster.data

import org.joda.time.DateTime
import org.locationtech.geomesa.utils.geohash.BoundingBox

/**
 * This class contains parameters needed to create query to
 * retrieve raster chunks from Accumulo table.
 *
 * @param bbox Bounding box defines geometric area of desired raster
 * @param resolution Desired resolution of grid
 * @param startTime Optional earliest ingestion time of rasters
 * @param endTime Optional latest ingestion time of rasters
 */
case class RasterQuery(bbox: BoundingBox,
                       resolution: String,
                       startTime: Option[DateTime],
                       endTime: Option[DateTime]) {
  def encodeBbox(): String =
    s"${bbox.getMinX},${bbox.getMinY},${bbox.getMaxX},${bbox.getMaxY}"

  def encodeTime(time: Option[DateTime]): String = time match {
    case Some(t) => s"${dateToAccTimestamp(t)}"
    case _ => ""
  }

  private def dateToAccTimestamp(dt: DateTime): Long =  dt.getMillis / 1000
}
