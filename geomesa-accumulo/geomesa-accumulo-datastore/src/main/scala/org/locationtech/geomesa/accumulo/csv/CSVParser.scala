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

package org.locationtech.geomesa.accumulo.csv

import java.util.Date
import java.lang.{Integer => jInt, Double => jDouble}

import com.vividsolutions.jts.geom._
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormatter, DateTimeFormat}
import org.locationtech.geomesa.accumulo.util.SftBuilder
import org.locationtech.geomesa.utils.text.WKTUtils

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

object CSVParser {

  implicit object IntParser extends CSVParser[jInt] {
    val priority = 0
    def parse(datum: String) = Try(new jInt(datum.toInt))
    def buildSpec(builder: SftBuilder, field: String, default: Boolean = false) = builder.intType(field)
  }

  implicit object DoubleParser extends CSVParser[jDouble] {
    val priority = 1
    def parse(datum: String) =
      Try(new jDouble(datum.toDouble)).orElse(Try(DMS(datum).toDouble).map(new jDouble(_)))
    def buildSpec(builder: SftBuilder, field: String, default: Boolean = false) = builder.doubleType(field)
  }

  implicit object TimeParser extends CSVParser[Date] {
    val priority = 2

    val timePatterns = Seq("YYYY-MM-dd'T'HH:mm:ss",
                           "YYYY-MM-dd'T'HH:mm:ssZ",
                           "YYYY-MM-dd'T'HH:mm:ss.sss",
                           "YYYY-MM-dd'T'HH:mm:ss.sssZ")
    val timeFormats = timePatterns.map(p => DateTimeFormat.forPattern(p).withZoneUTC)

    @tailrec
    def parseUsingFormats(datum: String, formats: Seq[DateTimeFormatter]): Try[DateTime] =
      if (formats.isEmpty) {
        Failure(new Exception(s"""Failed to parse "$datum" as time using formats ${timePatterns.mkString(",")}"""))
      } else {
        val t = Try(formats.head.parseDateTime(datum))
        if (t.isSuccess) t else parseUsingFormats(datum, formats.tail)
      }

    def parse(datum: String) = parseUsingFormats(datum, timeFormats).map(_.toDate)
    def buildSpec(builder: SftBuilder, field: String, default: Boolean = false) = {
      builder.date(field)
      if (default) {
        builder.withDefaultDtg(field)
      }
    }
  }

  implicit object PointParser extends CSVParser[Point] {
    val priority = 3
    override val isGeom = true
    def parseWKT(datum: String) = Try(WKTUtils.read(datum).asInstanceOf[Point])
    def parseDMS(datum: String) = {
      import DMS.{LatHemi, LonHemi}
      for {
        List(c1, c2) <- Try { DMS.regex.findAllIn(datum).map(DMS.apply).toList }
      } yield {
        (c1.hemisphere, c2.hemisphere) match {
          case (_: LatHemi, _: LonHemi) => parseWKT(s"POINT(${c2.toDouble} ${c1.toDouble})").get
          case (_: LonHemi, _: LatHemi) => parseWKT(s"POINT(${c1.toDouble} ${c2.toDouble})").get
          case _ => throw new IllegalArgumentException("Need one coordinate in each direction")
        }
      }

    }
    def parse(datum: String) = parseWKT(datum).orElse(parseDMS(datum))
    def buildSpec(builder: SftBuilder, field: String, default: Boolean = false) =
      builder.point(field, default = default)
  }

  implicit object LineStringParser extends CSVParser[LineString] {
    val priority = 4
    override val isGeom = true
    def parse(datum: String) = Try(WKTUtils.read(datum).asInstanceOf[LineString])
    def buildSpec(builder: SftBuilder, field: String, default: Boolean = false) =
      builder.lineString(field, default = default)
  }

  implicit object PolygonParser extends CSVParser[Polygon] {
    val priority = 5
    override val isGeom = true
    def parse(datum: String) = Try(WKTUtils.read(datum).asInstanceOf[Polygon])
    def buildSpec(builder: SftBuilder, field: String, default: Boolean = false) =
      builder.polygon(field, default = default)
  }

  implicit object MultiPointParser extends CSVParser[MultiPoint] {
    val priority = 6
    override val isGeom = true
    def parse(datum: String) = Try(WKTUtils.read(datum).asInstanceOf[MultiPoint])
    def buildSpec(builder: SftBuilder, field: String, default: Boolean = false) =
      builder.multiPoint(field, default = default)
  }

  implicit object MultiLineStringParser extends CSVParser[MultiLineString] {
    val priority = 7
    override val isGeom = true
    def parse(datum: String) = Try(WKTUtils.read(datum).asInstanceOf[MultiLineString])
    def buildSpec(builder: SftBuilder, field: String, default: Boolean = false) =
      builder.multiLineString(field, default = default)
  }

  implicit object MultiPolygonParser extends CSVParser[MultiPolygon] {
    val priority = 8
    override val isGeom = true
    def parse(datum: String) = Try(WKTUtils.read(datum).asInstanceOf[MultiPolygon])
    def buildSpec(builder: SftBuilder, field: String, default: Boolean = false) =
      builder.multiPolygon(field, default = default)
  }

  implicit object GeometryParser extends CSVParser[Geometry] {
    val priority = 9
    override val isGeom = true
    def parse(datum: String) = Try(WKTUtils.read(datum))
    def buildSpec(builder: SftBuilder, field: String, default: Boolean = false) =
      builder.geometry(field, default = default)
  }

  implicit object StringParser extends CSVParser[String] {
    val priority = 10
    def parse(datum: String) = Success(datum)
    def buildSpec(builder: SftBuilder, field: String, default: Boolean = false) =
      builder.stringType(field)
  }

  lazy val parsers: Seq[CSVParser[_ <: AnyRef]] =
    Seq(IntParser,
        DoubleParser,
        TimeParser,
        LineStringParser,
        PolygonParser,
        MultiLineStringParser,
        MultiPointParser,
        MultiPolygonParser,
        GeometryParser,
        PointParser,
        StringParser).sortBy(_.priority)
}

sealed trait CSVParser[A <: AnyRef] {
  def priority: Int
  def parse(datum: String): Try[A]
  def buildSpec(builder: SftBuilder, field: String, default: Boolean = false)
  def isGeom: Boolean = false
}
