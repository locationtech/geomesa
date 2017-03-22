/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.csv

import java.lang.{Double => jDouble, Integer => jInt}
import java.util.Date

import com.vividsolutions.jts.geom._
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.locationtech.geomesa.utils.geotools.SftBuilder
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
