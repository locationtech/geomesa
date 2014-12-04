package org.locationtech.geomesa.core.csv

import java.util.Date
import java.lang.{Integer => jInt, Double => jDouble}

import com.vividsolutions.jts.geom.{Point, Geometry}
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormatter, DateTimeFormat}
import org.locationtech.geomesa.utils.text.WKTUtils

import scala.util.{Failure, Success, Try}

object Parsable {
  implicit object IntIsParsable extends Parsable[jInt] {
    val priority = 0
    val typeChar = 'i'
    def parse(datum: String): Try[jInt] = Try(new jInt(datum.toInt))
  }

  implicit object DoubleIsParsable extends Parsable[jDouble] {
    val priority = 1
    val typeChar = 'd'
    def parse(datum: String): Try[jDouble] = Try(new jDouble(datum.toDouble))
  }

  implicit object TimeIsParsable extends Parsable[Date] {
    val priority = 2
    val timePatterns = Seq("YYYY-MM-DD'T'HH:mm:ss",
                           "YYYY-MM-DD'T'HH:mm:ssZ",
                           "YYYY-MM-DD'T'HH:mm:ss.sss",
                           "YYYY-MM-DD'T'HH:mm:ss.sssZ")

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

  implicit object PointIsParsable extends Parsable[Point] {
    val priority = 3
    val typeChar = 'p'
    def parse(datum: String): Try[Point] = Try { WKTUtils.read(datum).asInstanceOf[Point] }
  }

  implicit object StringIsParsable extends Parsable[String] {
    val priority = 4
    val typeChar = 's'
    def parse(datum: String): Try[String] = Success(datum)
  }

  lazy val parsers: Seq[Parsable[_ <: AnyRef]] =
    Seq(IntIsParsable,
        DoubleIsParsable,
        TimeIsParsable,
        PointIsParsable,
        StringIsParsable).sortBy(_.priority)
  lazy val parserMap: Map[Char, Parsable[_ <: AnyRef]] =
    parsers.map(p => (p.typeChar, p)).toMap
}

sealed trait Parsable[A <: AnyRef] {
  def priority: Int
  def parse(datum: String): Try[A]
  def typeChar: Char
  def parseAndType(datum: String): Try[(A, Char)] = parse(datum).map((_, typeChar))
}
