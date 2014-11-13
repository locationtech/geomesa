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

package org.locationtech.geomesa.core.index

import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom.Geometry
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.hadoop.io.Text
import org.joda.time.format.DateTimeFormatter
import org.joda.time.{DateTime, DateTimeZone}
import org.locationtech.geomesa.core.index.KeyUtils._
import org.locationtech.geomesa.utils.CartesianProductIterable
import org.locationtech.geomesa.utils.geohash.{GeoHash, GeohashUtils}

case class QueryPlan(iterators: Seq[IteratorSetting], ranges: Seq[org.apache.accumulo.core.data.Range], cf: Seq[Text])

trait KeyPlanningFilter

case object AcceptEverythingFilter extends KeyPlanningFilter
case class SpatialFilter(geom: Geometry) extends KeyPlanningFilter
case class DateFilter(dt:DateTime) extends KeyPlanningFilter
case class DateRangeFilter(start:DateTime, end:DateTime) extends KeyPlanningFilter
case class SpatialDateFilter(geom: Geometry, dt:DateTime) extends KeyPlanningFilter
case class SpatialDateRangeFilter(geom: Geometry, start:DateTime, end:DateTime)
  extends KeyPlanningFilter

sealed trait KeyPlan {
  def join(right: KeyPlan, sep: String): KeyPlan
  def toRange : KeyPlan = KeyInvalid
  def hasRange : Boolean = toRange != KeyInvalid
  def toRegex : KeyPlan = KeyInvalid
  def hasRegex : Boolean = toRegex != KeyInvalid
  def toList : KeyPlan = KeyInvalid
  def hasList : Boolean = toList != KeyInvalid
}

case object KeyInvalid extends KeyPlan {
  def join(right: KeyPlan, sep: String): KeyPlan = this
}

// this key-plan accepts all inputs
case object KeyAccept extends KeyPlan {
  // a word on Accumulo characters:  Even though the string-parser says that it
  // can handle bytes represented from \x00 to \xFF, it turns out that anything
  // above \x7F throws a "unsupported non-ascii character" error, which is why
  // our maximum character here is \0x7E, "~"
  val MIN_START = "\u0000"
  val MAX_END = "~"

  def join(right: KeyPlan, sep: String): KeyPlan = right match {
    case KeyRange(rstart, rend) => KeyRange(MIN_START + sep + rstart, MAX_END + sep + rend)
    case KeyRegex(rregex) => KeyRegex(".*?" + sep + rregex)
    case KeyList(rkeys) => {
      val sorted = rkeys.sorted
      KeyRange(MIN_START+sep+sorted.head, MAX_END+sep+sorted.last)
    }
    case KeyAccept => KeyRange(MIN_START + sep + MIN_START, MAX_END + sep + MAX_END)
    case _ => KeyInvalid
  }
}

case class KeyRange(start: String, end: String) extends KeyPlan {
  def join(right: KeyPlan, sep: String): KeyPlan = right match {
    case KeyRange(rstart, rend) => KeyRange(start + sep + rstart, end + sep + rend)
    case KeyRegex(rregex) => KeyRegex(estimateRangeRegex(start, end) + sep + rregex)
    case KeyList(rkeys) => {
      val sorted = rkeys.sorted
      KeyRange(start+sep+sorted.head, end+sep+sorted.last)
    }
    case KeyAccept => KeyRange(start + sep + KeyAccept.MIN_START, end + sep + KeyAccept.MAX_END)
    case KeyInvalid => KeyInvalid
    case _ => throw new Exception("Invalid KeyPlan match")
  }
  override def toRange = this
  override def toRegex = KeyRegex(estimateRangeRegex(start, end))
}

case class KeyRanges(ranges:Seq[KeyRange]) extends KeyPlan {
  // required of the KeyPlan contract, but never used
  def join(right: KeyPlan, sep: String): KeyPlan = KeyInvalid
}

case class KeyRegex(regex: String) extends KeyPlan {
  def join(right: KeyPlan, sep: String) = right match {
    case KeyRange(rstart,rend) => KeyRegex(regex+sep+estimateRangeRegex(rstart,rend))
    case KeyRegex(rregex) => KeyRegex(regex + sep + rregex)
    case KeyList(rkeys) => KeyRegex(regex + sep +
      (if (rkeys.size > MAX_KEYS_IN_REGEX) {
        generalizeStringsToRegex(rkeys)
      } else rkeys.mkString("(","|",")"))
    )
    case KeyAccept => KeyRegex(regex + sep + ".*?")
    case KeyInvalid => KeyInvalid
    case _ => throw new Exception("Invalid KeyPlan match")
  }
  override def toRegex = this
}

case class KeyList(keys:Seq[String]) extends KeyPlan {
  lazy val sorted = keys.sorted
  def join(right: KeyPlan, sep: String) = right match {
    case KeyRange(rstart,rend) =>
      KeyRange(sorted.head+sep+rstart, sorted.last+sep+rend)
    case KeyRegex(rregex) => KeyRegex(
      (if (keys.size > MAX_KEYS_IN_REGEX) {
        generalizeStringsToRegex(keys)
      } else keys.mkString("(","|",")")) + sep + rregex)
    case KeyList(rkeys) => {
      val combiner = CartesianProductIterable(Seq(keys, rkeys))
      if (combiner.expectedSize > MAX_KEYS_IN_LIST) {
        // there are too many keys; consolidate them

        // if there are too many (total) entries, use a range (losing some data);
        // otherwise, use a regular expression joining the lists
        if ((keys.size+rkeys.size) > MAX_KEYS_IN_REGEX) {
          val thatSorted = rkeys.sorted
          KeyRange(
            sorted.head + sep + thatSorted.head,
            sorted.last + sep + thatSorted.last
          )
        }
        else KeyRegex("((" + keys.mkString("|") + ")" + sep + "(" + rkeys.mkString("|") + "))")
      } else {
        // there are few enough combinations that we can preserve the explicit list
        KeyList(combiner.iterator.toList.map(_.mkString(sep)))
      }
    }
    case KeyAccept => KeyRegex("((" + keys.mkString("|") + ")" + sep + "(.*?))")
    case KeyInvalid => KeyInvalid
    case _ => throw new Exception("Invalid KeyPlan match")
  }
  override def toRange = {
    val sortedKeys = keys.sorted
    KeyRange(sortedKeys.head, sortedKeys.last)
  }
  override def toRegex = if (keys.size > MAX_KEYS_IN_REGEX)
    KeyRegex(generalizeStringsToRegex(keys))
    else KeyRegex("(" + keys.mkString("|") + ")")
  override def toList = this
}

/**
 * Accumulates tiers of discrete keys that can be reduced to one or more key-
 * plans once the information is complete.
 *
 * The problem with assuming a single KeyRange is that it sets up a conflict
 * between our partitioning scheme -- designed to spread queries across the
 * entire cluster -- and the ordering that makes per-tablet-server querying
 * efficient.  Consider the following KeyRange end-points for a query:
 *
 *   01~randomdatapoint~d~201111
 *   99~randomdatapoint~d~201201
 *
 * As a single range, this is largely ineffective, because it will include
 * ALL of the entries for partitions 02 to 98 (97/99), removing very few of
 * the entries from subsequent decoding and/or filtering.  A better plan would
 * be a sequence of ranges, one per partition:
 *
 *   01~randomdatapoint~d~201111, 01~randomdatapoint~d~201201
 *   02~randomdatapoint~d~201111, 02~randomdatapoint~d~201201
 *   ...
 *   99~randomdatapoint~d~201111, 99~randomdatapoint~d~201201
 *
 * Accumulo allows us to set multiple ranges per query, presumably for
 * exactly this type of sharded-range planning.
 */
sealed trait KeyTiered extends KeyPlan {
  val parent: Option[KeyTiered]
  val optList: Option[KeyList] = None
  val optRange: Option[KeyRange] = None
  def toRanges: Seq[KeyRange] = if (optRange.isDefined) Seq(optRange.get)
    else optList.get.keys.map(key => KeyRange(key, key))
  def toRanges(parentRange: KeyRange, sep: String): Seq[KeyRange] =
    toRanges.map(range =>
      KeyRange(parentRange.start + sep + range.start,
        parentRange.end + sep + range.end))
  def toRanges(sep: String): Seq[KeyRange] = parent match {
    case Some(kt:KeyTiered) => kt.toRanges(sep).flatMap { range => toRanges(range, sep) }
    case None => toRanges
    case _ => throw new Exception("Invalid parent for KeyTiered")
  }
  def join(right: KeyPlan, sep: String): KeyPlan = right match {
    case KeyRangeTiered(rstart, rend, None) => KeyRangeTiered(rstart, rend, Some(this))
    case KeyRange(rstart, rend)             => KeyRangeTiered(rstart, rend, Some(this))
    case KeyListTiered(rkeys, None)         => KeyListTiered(rkeys, Some(this))
    case KeyList(rkeys)                     => KeyListTiered(rkeys, Some(this))
    case KeyAccept                          => KeyRangeTiered(KeyAccept.MIN_START, KeyAccept.MAX_END, Some(this))
    case _                                  => KeyInvalid  // degenerate case
  }
}
case class KeyRangeTiered(start: String, end: String, parent:Option[KeyTiered]=None) extends KeyTiered {
  override val optRange = Some(KeyRange(start, end))
}
case class KeyListTiered(keys:Seq[String], parent:Option[KeyTiered]=None) extends KeyTiered {
  override val optList = Some(KeyList(keys))
}

object KeyUtils {
  val MAX_KEYS_IN_LIST = 65536
  val MAX_KEYS_IN_REGEX = 1024

  // assume that strings are all of the same size
  // (this will necessarily throw away information you will wish you had kept)
  def generalizeStringsToRegex(seq:Seq[String]) : String = {
    val pairs = seq.map(s => s.zipWithIndex.map(_ match { case (c,i) => (i,c)})).flatten
    val mapChars : Map[Int,List[Char]] = pairs.foldLeft(Map[Int,List[Char]]())((mapSoFar,pair) => pair match { case (i,c) => {
      if (mapSoFar.contains(i)) {
        val oldList : List[Char] = mapSoFar(i)
        (mapSoFar - i) + (i -> (c :: oldList).distinct)
      }
      else mapSoFar + (i -> List(c))
    }})

    // count how many expressions are allowable under this per-character regex
    val ranges = mapChars.keys.toList.sorted.map(i => {mapChars(i).sorted.mkString})
    val numCombinations = ranges.map(s => s.length.toLong).product

    // depending on 1) how selective the per-character regex is; and
    // 2) how many explicit strings there are to list;
    // determine which form is more useful
    if ((numCombinations > 10L*seq.size.toLong) && (seq.size < 1024)) seq.mkString("(","|",")")
    else ranges.map(s => if (s.length==1) s else "["+s+"]").mkString
  }

  // assumes an ASCII (not unicode) encoding
  def encode(c:Char) : String = c match {
    case a if (a>='a' && a<='z') => a.toString
    case a if (a>='A' && a<='A') => a.toString
    case d if (d>='0' && d<='9') => d.toString
    case _ => """\x""" + c.toInt.toHexString.reverse.padTo(2,"0").reverse.mkString
  }

  // encode as choices, so as to avoid syntax issues with ranges and hex-encoding
  def encodeRange(cMin:Char, cMax:Char) : String =
    (cMin to cMax).map(c => encode(c)).mkString("(","|",")")

  // assume that the two strings are the same size, and ASCII-encoded
  // (this will necessarily throw away information you will wish you had kept)
  def estimateRangeRegex(start:String, end:String) : String = {
    start.zip(end).foldLeft((true,""))((t1,t2) =>
      t1 match { case (inFrontMatch,regexSoFar) => {
        t2 match { case (cA,cB) => {
          if (inFrontMatch) {
            if (cA==cB) (true,regexSoFar+encode(cA))
            else {
              val cMin = if (cA < cB) cA else cB
              val cMax = if (cA > cB) cA else cB
              (false, regexSoFar+encodeRange(cMin,cMax))
            }
          } else (false,regexSoFar+".")
        }
      }}})._2
  }
}

trait KeyPlanner {
  def getKeyPlan(filter:KeyPlanningFilter, output: ExplainerOutputType): KeyPlan
}

trait ColumnFamilyPlanner {
  def getColumnFamiliesToFetch(filter: KeyPlanningFilter): KeyPlan
}

trait GeoHashPlanner extends Logging {
  def geomToGeoHashes(geom: Geometry, offset: Int, bits: Int): Seq[String] =
    GeohashUtils.getUniqueGeohashSubstringsInPolygon(geom, offset, bits, MAX_KEYS_IN_LIST)

  // takes care of the case where overflow forces a return value
  // that is an empty list
  def polyToPlan(geom: Geometry, offset: Int, bits: Int): KeyPlan = {
    val subHashes = geomToGeoHashes(geom, offset, bits).sorted
    logger.trace(s"Geom to GeoHashes has returned: ${subHashes.size} subhashes to cover $geom $offset $bits.")
    subHashes match {
      case subs if subs.size == 0 =>
        // if the list is empty, then there are probably too many 35-bit GeoHashes
        // that fall inside the given polygon; in this case, return the LL, UR
        // GeoHash endpoints of the entire range (which could encompass many
        // more GeoHashes than we wish, but can only be better than (or equal
        // to) a full-table scan)
        val env = geom.getEnvelopeInternal
        val ghLL = GeoHash(env.getMinX, env.getMinY)
        val ghUR = GeoHash(env.getMaxX, env.getMaxY)
        KeyRange(ghLL.hash, ghUR.hash)
      case subs => KeyList(subs.sorted)
    }
  }

  def getKeyPlan(filter: KeyPlanningFilter, offset: Int, bits: Int) = filter match {
    case SpatialFilter(geom) =>
      polyToPlan(geom, offset, bits)
    case SpatialDateFilter(geom, _) =>
      polyToPlan(geom, offset, bits)
    case SpatialDateRangeFilter(geom, _, _) =>
      polyToPlan(geom, offset, bits)
    case AcceptEverythingFilter => KeyAccept
    case _ => KeyInvalid // degenerate outcome
  }
}

case class GeoHashKeyPlanner(offset: Int, bits: Int) extends KeyPlanner with GeoHashPlanner {
  def getKeyPlan(filter: KeyPlanningFilter, output: ExplainerOutputType) = getKeyPlan(filter, offset, bits) match {
    case KeyList(keys) =>
      output(s"GeoHashKeyPlanner: ${keys.size} : ${keys.take(20)}")
      KeyListTiered(keys)

    case KeyAccept =>
      output(s"GeoHashKeyPlanner: KeyAccept")
      KeyAccept

    case _ => KeyInvalid
  }
}

case class GeoHashColumnFamilyPlanner(offset: Int, bits: Int) extends ColumnFamilyPlanner with GeoHashPlanner {
  def getColumnFamiliesToFetch(filter: KeyPlanningFilter): KeyPlan = getKeyPlan(filter, offset, bits)
}

case class RandomPartitionPlanner(numPartitions: Int) extends KeyPlanner {
  val numBits: Int = numPartitions.toString.length
  def getKeyPlan(filter: KeyPlanningFilter, output: ExplainerOutputType) = {
    val keys = (0 to numPartitions).map(_.toString.reverse.padTo(numBits,"0").reverse.mkString)
    output(s"Random Partition Planner: $keys")
    KeyListTiered(keys)
  }
}

case class ConstStringPlanner(cstr: String) extends KeyPlanner {
  def getKeyPlan(filter:KeyPlanningFilter, output: ExplainerOutputType) = {
    output(s"ConstPlanner: $cstr")
    KeyListTiered(List(cstr))
  }
}

case class ResolutionPlanner(ires: Double) extends KeyPlanner {
  def getKeyPlan(filter:KeyPlanningFilter, output: ExplainerOutputType) = KeyListTiered(List(lexiEncodeDoubleToString(ires)))
}

case class BandPlanner(band: String) extends KeyPlanner {
  def getKeyPlan(filter:KeyPlanningFilter, output: ExplainerOutputType) = KeyListTiered(List(band))
}

case class DatePlanner(formatter: DateTimeFormatter) extends KeyPlanner {
  val endDates = List(9999,12,31,23,59,59,999)
  val startDates = List(0,1,1,0,0,0,0)
  def getKeyPlan(filter:KeyPlanningFilter, output: ExplainerOutputType) = {
    val plan = filter match {
      case DateFilter(dt) => KeyRange(formatter.print(dt), formatter.print(dt))
      case SpatialDateFilter(_, dt) => KeyRange(formatter.print(dt), formatter.print(dt))
      case DateRangeFilter(start,end) => {// @todo - add better ranges for wrap-around case
        if (formatter.print(start).take(4).toInt == start.getYear && formatter.print(end).take(4).toInt == end.getYear) {
          // ***ASSUME*** that time components have been provided in order (if they start with the year)!
          KeyRangeTiered(formatter.print(start), formatter.print(end))
        } else {
          val matchedTime = getTimeComponents(start).zip(getTimeComponents(end)).takeWhile(tup => tup._1 == tup._2).map(_._1)
          val zeroTimeHead: List[Int] = bufferMatchedTime(matchedTime, startDates, start)
          val zeroTime = extractRelevantTimeUnits(zeroTimeHead, startDates)
          val endTimeHead = bufferMatchedTime(matchedTime, endDates, end)
          val endTime = extractRelevantTimeUnits(endTimeHead, endDates)
          KeyRangeTiered(formatter.print(createDate(zeroTime)),formatter.print(createDate(endTime)))
        }
      }
      case SpatialDateRangeFilter(_, start, end) => {// @todo - add better ranges for wrap-around case
        if (formatter.print(start).take(4).toInt == start.getYear && formatter.print(end).take(4).toInt == end.getYear) {
          // ***ASSUME*** that time components have been provided in order (if they start with the year)!
          KeyRangeTiered(formatter.print(start), formatter.print(end))
        } else {
          val matchedTime = getTimeComponents(start).zip(getTimeComponents(end)).takeWhile(tup => tup._1 == tup._2).map(_._1)
          val zeroTimeHead: List[Int] = bufferMatchedTime(matchedTime, startDates, start)
          val zeroTime = extractRelevantTimeUnits(zeroTimeHead, startDates)
          val endTimeHead = bufferMatchedTime(matchedTime, endDates, end)
          val endTime = extractRelevantTimeUnits(endTimeHead, endDates)
          KeyRangeTiered(formatter.print(createDate(zeroTime)),formatter.print(createDate(endTime)))
        }
      }
      case _ => defaultKeyRange
    }
    plan match {
      case KeyRange(start, end) =>          output(s"DatePlanner: start: $start end: $end")
      case KeyRangeTiered(start, end, _) => output(s"DatePlanner: start: $start end: $end")
    }
    plan
  }

  def bufferMatchedTime(matchedTime: List[Int], dates: List[Int], time: DateTime): List[Int] =
    if (matchedTime.length < dates.length)
      matchedTime ++ List(getTimeComponents(time)(matchedTime.length))
    else
      matchedTime

  private def extractRelevantTimeUnits(timeList: List[Int], base: List[Int]) =
    timeList.zipAll(base, -1, -1).map { case (l,r) => if(l>=0) l else r }

  private def getTimeComponents(dt: DateTime) =
    List(math.min(9999,math.max(0,dt.getYear)),  // constrain to 4-digit years
      dt.getMonthOfYear,dt.getDayOfMonth,dt.getHourOfDay,dt.getMinuteOfDay)

  private def createDate(list:List[Int]) = list match {
    case year::month::date::hour::minute::second::ms::Nil => {
      val day = if (date >= 28)
        new DateTime(year, month, 1, 0, 0).dayOfMonth().withMaximumValue().getDayOfMonth
      else
        date
      new DateTime(year, month, day, hour, minute, second, ms, DateTimeZone.forID("UTC"))
    }
    case _ => throw new Exception("invalid date list.")
  }

  private val defaultKeyRange =
    KeyRange(
      formatter.print(createDate(startDates)),// seems to print in local time, so force 0's
      formatter.print(createDate(endDates)))
}

case class CompositePlanner(seq: Seq[KeyPlanner], sep: String) extends KeyPlanner {
  def getKeyPlan(filter: KeyPlanningFilter, output: ExplainerOutputType): KeyPlan = {
    val joined = seq.map(_.getKeyPlan(filter, output)).reduce(_.join(_, sep))
    joined match {
      case kt:KeyTiered    => KeyRanges(kt.toRanges(sep))
      case KeyRegex(regex) => joined.join(KeyRegex(".*"), "")
      case _               => joined
    }
  }
}
