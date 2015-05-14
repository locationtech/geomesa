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

package org.locationtech.geomesa.core.util

import java.util.{Collection => JCollection}

import org.apache.accumulo.core.data.{Key, PartialKey, Range => AccRange}
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.utils.geohash.BoundingBox

import scala.collection.JavaConversions._

/**
 *    This object holds any Accumulo functions related to Geohashes.
 *
 */
object BoundingBoxUtil {
  /**
   * returns a list of AccRange objects which may be used in a accumulo query.
   *
   * @param bbox
   * @return
   */
  def getRanges(bbox: BoundingBox): JCollection[AccRange] =
    getRanges(BoundingBox.getGeoHashesFromBoundingBox(bbox, 32), 0, Long.MaxValue / 2)

  /**
   * as above but includes time.
   *
   * @param geohashes
   * @param startTime
   * @param endTime
   * @return
   */
  def getRanges(geohashes: List[String], startTime: Long, endTime: Long): Iterable[AccRange] = {
    def getRange(startHash: String, endHash: String) = {
      val List(b, e) = getKeys(startHash, endHash)
      b.setTimestamp(startTime)
      e.setTimestamp(endTime)
      // it expects ranges, but the precision is maxed out, so fake it
      if (b.equals(e, PartialKey.ROW_COLFAM_COLQUAL)) {
        new AccRange(b, new Key(e.getRow, e.getColumnFamily, new Text(e.getColumnQualifier + "~")))
      } else {
        new AccRange(b, e)
      }
    }
    def getKeys(startHash: String, endHash: String) = {
      List(startHash, endHash) map (h => new Key(h.substring(0, 4),
        h.substring(4, 6),
        h.substring(6, 8)))
    }
    val geoBegin = (geohashes map (geohash => geohash.padTo(8, "!").mkString))
    val geoEnd = (geohashes map (geohash => geohash.padTo(8, "~").mkString))
    (geoBegin, geoEnd).zipped.map((b, e) => getRange(b, e)).toIterable
  }

  def getRangesByRow(geohashes: List[String], startTime: Long = 0 , endTime: Long = Long.MaxValue/2): Iterable[AccRange] = {
    def getRange(startHash: String, endHash: String) = {
      val List(b, e) = getKeys(startHash, endHash)
      b.setTimestamp(startTime)
      e.setTimestamp(endTime)
      // it expects ranges, but the precision is maxed out, so fake it
      if (b.equals(e, PartialKey.ROW)) {
        new AccRange(b, new Key(e.getRow + "~", e.getTimestamp))
      } else {
        new AccRange(b, e)
      }
    }
    def getKeys(startHash: String, endHash: String) = {
      List(startHash, endHash) map (h => new Key(h.substring(0, 8)))
    }
    val geoBegin = (geohashes map (geohash => geohash.padTo(8, "!").mkString))
    val geoEnd = (geohashes map (geohash => geohash.padTo(8, "~").mkString))
    (geoBegin, geoEnd).zipped.map((b, e) => getRange(b, e)).toIterable
  }

  implicit def string2Text(s: String): Text = new Text(s)
}
