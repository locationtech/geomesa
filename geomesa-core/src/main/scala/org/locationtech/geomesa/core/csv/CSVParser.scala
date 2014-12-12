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

package org.locationtech.geomesa.core.csv

import java.util.Date
import java.lang.{Integer => jInt, Double => jDouble}

import com.vividsolutions.jts.geom.Point
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormatter, DateTimeFormat}
import org.locationtech.geomesa.utils.text.WKTUtils

import scala.util.{Failure, Success, Try}

object CSVParser {
  implicit object IntParser extends CSVParser[jInt] {
    val priority = 0
    val typeChar = 'i'
    def parse(datum: String): Try[jInt] = Try(new jInt(datum.toInt))
  }

  implicit object DoubleParser extends CSVParser[jDouble] {
    val priority = 1
    val typeChar = 'd'
    def parse(datum: String): Try[jDouble] =
      Try(datum.toDouble) orElse Try(DMS(datum).toDouble) map { new jDouble(_) }
  }

  implicit object TimeParser extends CSVParser[Date] {
    val priority = 2
    val timePatterns = Seq("YYYY-MM-dd'T'HH:mm:ss",
                           "YYYY-MM-dd'T'HH:mm:ssZ",
                           "YYYY-MM-dd'T'HH:mm:ss.sss",
                           "YYYY-MM-dd'T'HH:mm:ss.sssZ")

    val timeFormats = timePatterns.map(DateTimeFormat.forPattern _ andThen (_.withZoneUTC))

    val typeChar = 't'
    def parse(datum: String): Try[Date] = {
      def parseUsingFormats(formats: Seq[DateTimeFormatter]): Try[DateTime] = {
        if (formats.isEmpty) {
          Failure(new Exception(s"""Failed to parse "$datum" as time using formats ${timePatterns.mkString(",")}"""))
        } else {
          Try(formats.head parseDateTime datum) orElse parseUsingFormats(formats.tail)
        }
      }
      parseUsingFormats(timeFormats).map(_.toDate)
    }
  }

  implicit object PointParser extends CSVParser[Point] {
    val priority = 3
    val typeChar = 'p'
    def parseWKT(datum: String) = Try { WKTUtils.read(datum).asInstanceOf[Point] }
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
    def parse(datum: String): Try[Point] = parseWKT(datum) orElse parseDMS(datum)

  }

  implicit object StringParser extends CSVParser[String] {
    val priority = 4
    val typeChar = 's'
    def parse(datum: String): Try[String] = Success(datum)
  }

  lazy val parsers: Seq[CSVParser[_ <: AnyRef]] =
    Seq(IntParser,
        DoubleParser,
        TimeParser,
        PointParser,
        StringParser).sortBy(_.priority)
  lazy val parserMap: Map[Char, CSVParser[_ <: AnyRef]] =
    parsers.map(p => (p.typeChar, p)).toMap
}

sealed trait CSVParser[A <: AnyRef] {
  def priority: Int
  def parse(datum: String): Try[A]
  def typeChar: Char
  def parseAndType(datum: String): Try[(A, Char)] = parse(datum).map((_, typeChar))
}
