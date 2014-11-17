package org.locationtech.geomesa.core.csv

import java.util.Date

import com.vividsolutions.jts.geom.Geometry
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormatter, DateTimeFormat}
import org.locationtech.geomesa.utils.text.WKTUtils

import scala.util.{Failure, Success, Try}

object Parsable {
  implicit object StringIsParsable extends Parsable[String] {
    val typeChar = 's'
    def parse(datum: String): Try[String] = Success(datum)
  }

  implicit object IntIsParsable extends Parsable[Int] {
    val typeChar = 'i'
    def parse(datum: String): Try[Int] = Try(datum.toInt)
  }

  implicit object DoubleIsParsable extends Parsable[Double] {
    val typeChar = 'd'
    def parse(datum: String): Try[Double] = Try(datum.toDouble)
  }

  implicit object GeometryIsParsable extends Parsable[Geometry] {
    val typeChar = 'g'
    def parse(datum: String): Try[Geometry] = Try { WKTUtils.read(datum) }
  }

  implicit object TimeIsParsable extends Parsable[Date] {
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
}

trait Parsable[A] {
  def parse(datum: String): Try[A]
  def typeChar: Char
  def parseAndType(datum: String): Try[(A, Char)] = parse(datum).map((_, typeChar))
}
