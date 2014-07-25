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

package geomesa.core.index

import geomesa.core.index.IndexEntry._
import org.apache.hadoop.io.Text
import org.joda.time.{DateTime, DateTimeZone}
import org.opengis.feature.simple.SimpleFeature

import scala.util.hashing.MurmurHash3

trait TextFormatter[E] {
  def format(entry: E): Text
  def numBits: Int
}

object TextFormatter {
  implicit def string2Text(s: String): Text = new Text(s)
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

case class GeoHashTextFormatter(offset: Int, numBits: Int) extends TextFormatter[SimpleFeature] {
  def format(entry: SimpleFeature) = {
    val hash = entry.gh.hash
    val padded = hash.padTo(7, ".").mkString
    val partial = padded.drop(offset).take(numBits)
    new Text(partial)
  }
}

// note:  this will fail if you have an entry lacking a valid date
case class DateTextFormatter(f: String) extends TextFormatter[SimpleFeature] {
  val timeZone = DateTimeZone.forID("UTC")
  val numBits = f.length
  val formatter = org.joda.time.format.DateTimeFormat.forPattern(f)
  def format(entry: SimpleFeature) =
    new Text(formatter.print(entry.dt.getOrElse(new DateTime()).withZone(timeZone)))
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
 * @param numPartitions "%99#r" will mean:  create shards from 0..99
 * @tparam E some descendant of IndexEntry
 */
case class PartitionTextFormatter[E <: SimpleFeature](numPartitions: Int) extends TextFormatter[E] {
  val numBits: Int = numPartitions.toString.length
  val fmt = ("%0" + numBits + "d").format(_: Int)


  def getIdHashPartition(entry: E): Int = {
    val toHash = entry.getID match {
      case null => entry.getAttributes.toArray
      case id   => Array(id)
    }
    Math.abs(MurmurHash3.arrayHash(toHash) % (numPartitions + 1))
  }

  def format(entry: E): Text = new Text(fmt(getIdHashPartition(entry)))
}

case class ConstantTextFormatter[E](constStr: String) extends TextFormatter[E] {
  val constText = new Text(constStr)
  def format(entry: E) = constText
  def numBits = constStr.length
}

case class IdFormatter(maxLength: Int) extends TextFormatter[SimpleFeature] {
  def format(entry: SimpleFeature): Text = new Text(entry.sid.padTo(maxLength, "_").mkString)
  def numBits: Int = maxLength
}

case class CompositeTextFormatter[E](lf: Seq[TextFormatter[E]], sep: String) extends TextFormatter[E] {
  val numBits = lf.map(_.numBits).sum
  def format(entry: E) = new Text(lf.map { _.format(entry) }.mkString(sep))
}


