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

import com.vividsolutions.jts.geom.{Geometry, GeometryCollection, Polygon}
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone, Interval}
import org.locationtech.geomesa.utils.text.WKTUtils

import scala.annotation.tailrec
import scala.util.parsing.combinator.RegexParsers

trait SchemaHelpers extends RegexParsers {

  val minDateTime = new DateTime(0, 1, 1, 0, 0, 0, DateTimeZone.forID("UTC"))
  val maxDateTime = new DateTime(9999, 12, 31, 23, 59, 59, DateTimeZone.forID("UTC"))
  val everywhen = new Interval(minDateTime, maxDateTime)
  val everywhere = WKTUtils.read("POLYGON((-180 -90, 0 -90, 180 -90, 180 90, 0 90, -180 90, -180 -90))").asInstanceOf[Polygon]

  val DEFAULT_TIME = new DateTime(0, DateTimeZone.forID("UTC"))

  val CODE_START = "%"
  val CODE_END = "#"
  val GEO_HASH_CODE = "gh"
  val DATE_CODE = "d"
  val CONSTANT_CODE = "cstr"
  val RANDOM_CODE = "r"
  val SEPARATOR_CODE = "s"
  val ID_CODE = "id"
  val PART_DELIMITER = "::"

  def somewhen(interval: Interval): Option[Interval] =
    interval match {
      case null                => None
      case i if i == everywhen => None
      case _                   => Some(interval)
    }

  def innerSomewhere(geom: Geometry): Option[Geometry] =
    geom match {
      case null                 => None
      case p if p == everywhere => None
      case g: Geometry          => Some(g)
      case _                    => None
    }

  // This function helps catch nulls and 'entire world' polygons.
  def somewhere(geom: Geometry): Option[Geometry] =
    geom match {
      case null => None
      case gc: GeometryCollection =>
        val wholeWorld = (0 until gc.getNumGeometries).foldRight(false) {
          case (i, seenEverywhere) => gc.getGeometryN(i).equals(everywhere) || seenEverywhere
        }
        if(wholeWorld) None else Some(gc)
      case g: Geometry => innerSomewhere(g)
    }

  def pattern[T](p: => Parser[T], code: String): Parser[T] = CODE_START ~> p <~ (CODE_END + code)

  // A separator character, typically '%~#s' would indicate that elements are to be separated
  // with a '~'
  def sep = pattern("\\W".r, SEPARATOR_CODE)

  def offset = "[0-9]+".r ^^ { _.toInt }
  def bits = "[0-9]+".r ^^ { _.toInt }
  
  // A constant string encoder. '%fname#cstr' would yield fname
  //  We match any string other that does *not* contain % or # since we use those for delimiters
  def constStringPattern = pattern("[^%#]+".r, CONSTANT_CODE)
  def constantStringEncoder: Parser[ConstantTextFormatter] = constStringPattern ^^ {
    case str => ConstantTextFormatter(str)
  }

  // A date encoder. '%YYYY#d' would pull out the year from the date and write it to the key
  def datePattern = pattern("\\w+".r, DATE_CODE)
  def dateEncoder: Parser[DateTextFormatter] = datePattern ^^ {
    case t => DateTextFormatter(t)
  }

  // A geohash encoder.  '%2,4#gh' indicates that two characters starting at character 4 should
  // be extracted from the geohash and written to the field
  def geohashPattern = pattern((offset <~ ",") ~ bits, GEO_HASH_CODE)
  def geohashEncoder: Parser[GeoHashTextFormatter] = geohashPattern ^^ {
    case o ~ b => GeoHashTextFormatter(o, b)
  }

  // An id encoder. '%15#id' would pad the id out to 15 characters
  def idPattern = pattern("[0-9]*".r, ID_CODE)
  def idEncoder: Parser[IdFormatter] = idPattern ^^ {
    case len if len.length > 0 => IdFormatter(len.toInt)
    case _                     => IdFormatter(0)
  }

  // A random partitioner.  '%999#r' would write a random value between 000 and 999 inclusive
  def randPartitionPattern = pattern("\\d+".r, RANDOM_CODE)
  def randEncoder: Parser[PartitionTextFormatter] = randPartitionPattern ^^ {
    case d => PartitionTextFormatter(d.toInt)
  }

  def constStringPlanner: Parser[ConstStringPlanner] = constStringPattern ^^ {
    case str => ConstStringPlanner(str)
  }

  def randPartitionPlanner: Parser[RandomPartitionPlanner] = randPartitionPattern ^^ {
    case d => RandomPartitionPlanner(d.toInt)
  }

  def datePlanner: Parser[DatePlanner] = datePattern ^^ {
    case fmt => DatePlanner(DateTimeFormat.forPattern(fmt))
  }

  def geohashKeyPlanner: Parser[GeoHashKeyPlanner] = geohashPattern ^^ {
    case o ~ b => GeoHashKeyPlanner(o, b)
  }

  // a key element consists of a separator and any number of random partitions, geohashes, and dates
  def keypart: Parser[CompositeTextFormatter] =
    (sep ~ rep(randEncoder | geohashEncoder | dateEncoder | constantStringEncoder )) ^^
      {
        case sep ~ xs => CompositeTextFormatter(xs, sep)
      }

  // the column qualifier must end with an ID-encoder
  def cqpart: Parser[CompositeTextFormatter] =
    phrase(sep ~ rep(randEncoder | geohashEncoder | dateEncoder | constantStringEncoder) ~ idEncoder) ^^ {
      case sep ~ xs ~ id => CompositeTextFormatter(xs :+ id, sep)
    }

  // An index key is three keyparts, one for row, colf, and colq
  def formatter = keypart ~ PART_DELIMITER ~ keypart ~ PART_DELIMITER ~ cqpart ^^ {
    case rowf ~ PART_DELIMITER ~ cff ~ PART_DELIMITER ~ cqf => (rowf, cff, cqf)
  }

  // builds a geohash decoder to extract the entire geohash from the parts of the index key
  def ghDecoderParser = keypart ~ PART_DELIMITER ~ keypart ~ PART_DELIMITER ~ cqpart ^^ {
    case rowf ~ PART_DELIMITER ~ cff ~ PART_DELIMITER ~ cqf => {
      val (roffset, (ghoffset, rbits)) = extractGeohashEncoder(rowf.lf, 0, rowf.sep.length)
      val (cfoffset, (ghoffset2, cfbits)) = extractGeohashEncoder(cff.lf, 0, cff.sep.length)
      val (cqoffset, (ghoffset3, cqbits)) = extractGeohashEncoder(cqf.lf, 0, cqf.sep.length)
      val l = List((ghoffset, RowExtractor(roffset, rbits)),
        (ghoffset2, ColumnFamilyExtractor(cfoffset, cfbits)),
        (ghoffset3, ColumnQualifierExtractor(cqoffset, cqbits)))
      GeohashDecoder(l.sortBy { case (off, _) => off }.map { case (_, e) => e })
    }
  }

  def buildGeohashDecoder(s: String): GeohashDecoder = parse(ghDecoderParser, s).get

  // extracts an entire date encoder from a key part
  @tailrec
  final def extractDateEncoder(seq: Seq[TextFormatter], offset: Int, sepLength: Int): Option[(String, Int)] =
    seq match {
      case DateTextFormatter(f)::xs => Some(f,offset)
      case x::xs => extractDateEncoder(xs, offset + x.numBits + sepLength, sepLength)
      case Nil => None
    }

  // extracts the geohash encoder from a keypart
  @tailrec
  final def extractGeohashEncoder(seq: Seq[TextFormatter], offset: Int, sepLength: Int): (Int, (Int, Int)) =
    seq match {
      case GeoHashTextFormatter(off, bits)::xs => (offset, (off, bits))
      case x::xs => extractGeohashEncoder(xs, offset + x.numBits + sepLength, sepLength)
      case Nil => (0,(0,0))
    }

  def extractIdEncoder(seq: Seq[TextFormatter], offset: Int, sepLength: Int): Int =
    seq match {
      case IdFormatter(maxLength)::xs => maxLength
      case _ => sys.error("Id must be first element of column qualifier")
    }

}