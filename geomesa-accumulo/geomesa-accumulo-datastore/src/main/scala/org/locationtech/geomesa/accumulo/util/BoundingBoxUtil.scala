/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.util

import java.util.{Collection => JCollection}

import org.apache.accumulo.core.data.{Key, PartialKey, Range => AccRange}
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.utils.geohash.BoundingBox

import scala.collection.JavaConversions._

/**
  *    This object holds any Accumulo functions related to Geohashes.
  *
  */
@deprecated("will be removed with no replacement")
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
