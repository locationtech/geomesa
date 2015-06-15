/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index

import org.apache.hadoop.io.Text
import org.joda.time.DateTime
import org.locationtech.geomesa.accumulo.data.tables.SpatioTemporalTable
import org.locationtech.geomesa.utils.geohash.GeoHash
import org.opengis.feature.simple.SimpleFeature
import scala.util.hashing.MurmurHash3

trait TextFormatter {
  def format(gh: GeoHash, dt: DateTime, sf: SimpleFeature, isIndex: Boolean = false): Text
  def numBits: Int
}

abstract class BaseTextFormatter() extends TextFormatter {
  def format(gh: GeoHash, dt: DateTime, sf: SimpleFeature, isIndex: Boolean = false): Text =
    new Text(formatString(gh, dt, sf, isIndex))
  def formatString(gh: GeoHash, dt: DateTime, sf: SimpleFeature, isIndex: Boolean = false): String
}
/**
 * These GeoHash strings are padded to 7 characters with a period.  This is
 * done for a few reasons:
 * 1.  with the addition of GeoHash decomposition for non-point data, some
 *     of the GeoHashes will be at fewer than 35 bits (but always on 5-bit
 *     boundaries);
 * 2.  a period appears earlier in the ASCII chart than do any of the alpha-
 *     numeric characters
 *
 * @param offset how many characters (from the left) to skip
 * @param numBits how many characters to use
 */

case class GeoHashTextFormatter(offset: Int, numBits: Int) extends BaseTextFormatter {
  def formatString(gh: GeoHash, dt: DateTime, sf: SimpleFeature, isIndex: Boolean = false) = {
    val padded = gh.hash.padTo(7, ".").mkString
    padded.substring(offset, offset + numBits)
  }
}

// note:  this will fail if you have an entry lacking a valid date
case class DateTextFormatter(f: String) extends BaseTextFormatter {
  val numBits = f.length
  val formatter = org.joda.time.format.DateTimeFormat.forPattern(f).withZoneUTC()
  def formatString(gh: GeoHash, dt: DateTime, sf: SimpleFeature, isIndex: Boolean = false) =
   formatter.print(dt)
}

/**
 * Responsible for assigning a shard number (partition) to the given
 * entry based on a hash of the feature ID.
 *
 * MurmurHash3 was chosen, because 1) it is part of the standard
 * Scala libraries; 2) it claims to do a reasonable job spreading
 * hash values around.  See http://code.google.com/p/smhasher/wiki/MurmurHash3
 *
 * Assumptions:
 * <ul>
 *   <li>IDs that are null will be hashed based on the string version
 *       of the feature.  (It should not be possible to have a null
 *       ID, or at least not easy:  Both DataUtilities.createFeature
 *       and SimpleFeatureBuilder.buildFeature will automatically
 *       generate an ID if you don't provide a non-null ID of your
 *       own)
 *   <li>We will need code to cover the case where an ID changes,
 *       because it may mean moving an entry to a different tablet-
 *       server.  (How likely is this to happen?)</li>
 * </ul>
 *
 * @param shards "%99#r" will mean:  create shards from 0..98
 */
case class PartitionTextFormatter(shards: Int) extends BaseTextFormatter {
  val numPartitions = if (shards > 1) shards - 1 else 0
  val numBits: Int = numPartitions.toString.length
  val fmt = ("%0" + numBits + "d").format(_: Int)

  def getIdHashPartition(entry: SimpleFeature): Int = {
    val toHash = entry.getID match {
      case null => entry.getAttributes.toArray
      case id   => Array(id)
    }
    Math.abs(MurmurHash3.arrayHash(toHash) % (numPartitions + 1))
  }

  def formatString(gh: GeoHash, dt: DateTime, sf: SimpleFeature, isIndex: Boolean = false) =
    fmt(getIdHashPartition(sf))
}

case class ConstantTextFormatter(constStr: String) extends TextFormatter {
  val text = new Text(constStr)
  val numBits = constStr.length
  def format(gh: GeoHash, dt: DateTime, sf: SimpleFeature, isIndex: Boolean = false) = text
}

case class IndexOrDataTextFormatter() extends TextFormatter {
  val constTextIndex = new Text(SpatioTemporalTable.INDEX_FLAG)
  val constTextData = new Text(SpatioTemporalTable.DATA_FLAG)
  val numBits = 1
  def format(gh: GeoHash, dt: DateTime, sf: SimpleFeature, isIndex: Boolean = false) =
    if (isIndex) constTextIndex else constTextData
}

case class IdFormatter(maxLength: Int) extends BaseTextFormatter {
  def formatString(gh: GeoHash, dt: DateTime, sf: SimpleFeature, isIndex: Boolean = false) =
    sf.getID.padTo(maxLength, "_").mkString
  def numBits: Int = maxLength
}

case class CompositeTextFormatter(lf: Seq[TextFormatter], sep: String) extends BaseTextFormatter {
  val numBits = lf.map(_.numBits).sum
  def formatString(gh: GeoHash, dt: DateTime, sf: SimpleFeature, isIndex: Boolean = false) =
    lf.map { _.format(gh, dt, sf, isIndex) }.mkString(sep)
}
