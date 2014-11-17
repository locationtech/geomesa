package org.locationtech.geomesa.core

import java.io.File
import java.net.URI

import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory, Geometry}
import org.geotools.data.shapefile.ShapefileDataStoreFactory
import org.geotools.factory.CommonFactoryFinder
import org.geotools.feature.{FeatureCollection, DefaultFeatureCollection, FeatureCollections}
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.locationtech.geomesa.core.util.SftBuilder
import org.opengis.feature.simple.SimpleFeatureType

import scala.annotation.tailrec
import scala.io.Source
import scala.util.{Failure, Success, Try}

package object csv extends Logging {
  abstract class Schema(name: String, fields: Seq[String], types: Seq[Char], tField: String) {
    { // Schema must satisfy these to be well-formed
      require(types.size == fields.size, s"Number of types (${types.size}) must equal number of fields (${types.size})")
      val tfIdx = fields indexOf tField
      require(tfIdx != -1, s"Requested temporal field $tField not present")
      require(types(tfIdx) == 't', s"Requested temporal field $tField is not temporal")
    }

    lazy val ft: SimpleFeatureType = {
      val sftb = new SftBuilder
      for ((field, typeChar) <- fields zip types) {
        typeChar match {
          case 'i' => sftb.intType(field)
          case 'd' => sftb.doubleType(field)
          case 't' => sftb.date(field)
          case 'g' => sftb.geometry(field)
          case 's' => sftb.stringType(field)
        }
      }
      sftb.withDefaultDtg(tField)

      sftb.build(name)
    }

    def parseLine(csvLine: String): Try[Map[String, String]] = {
      val entries = csvLine.split(",")
      if (entries.size == fields.size) {
        Success((fields zip entries).toMap)
      } else {
        Failure(new Exception(s"Found ${entries.size} entries in line; expected ${fields.size}\nError parsing line: $csvLine"))
      }
    }

    def parseEntries(stringData: Map[String, String]): Try[Map[String, Any]] = {
      def verifySize(entries: Map[String, String]): Try[Unit] =
        if (entries.size == types.size) {
          Failure(new Exception(s"Expected ${types.size} entries but received ${entries.size}"))
        } else { Success(()) }

      val getParser: Char => Try[Parsable[_]] = tchar => Try {
                                                               parsers.find(_.typeChar == tchar)
                                                               .getOrElse(throw new Exception(s"Cannot find parser for type character $tchar"))
                                                             }

      val parseEntry: ((String, String), Char) => Try[(String, Any)] = { (entry, typeHint) =>
        val (name, datum) = entry

        for {
          parser <- getParser(typeHint)
          parsed <- parser.parse(datum)
        } yield {
          (name, parsed)
        }
      }

      def parseLoop(entries: Map[String, String]): Try[Map[String, Any]] = {
        @tailrec
        def loop(entries: Seq[((String, String), Char)], acc: Map[String, Any]): Try[Map[String, Any]] = {
          if (entries.isEmpty) { Success(acc) } else {
            parseEntry.tupled(entries.head) match {
              case Success(parsedPair) => loop(entries.tail, acc + parsedPair)
              case Failure(ex) => Failure(ex) // would be nice to flatmap this but we lose the tailrec optimization!
              // really this should be written already as Try.sequence but nope!
            }
          }
        }

        loop(entries.toSeq zip types, Map())
      }

      for {
        _ <- verifySize(stringData)
        parsedEntries <- parseLoop(stringData)
      } yield {
        parsedEntries
      }
    }

    def extractGeometry(fields: Map[String, Any]): Try[(Geometry, Map[String, Any])]
  }

  class GeomSchema(name: String, fields: Seq[String], types: Seq[Char], tField: String, gField: String)
    extends Schema(name: String, fields: Seq[String], types: Seq[Char], tField: String) {

    def extractGeometry(entries: Map[String, Any]): Try[(Geometry, Map[String, Any])] =
      for {
        spatialEntry <- Try { entries.getOrElse(gField, throw new Exception(s"Cannot find spatial field $gField")) }
        spatialData  <- Try { spatialEntry.asInstanceOf[Geometry] }
      } yield {
        (spatialData, entries - gField)
      }
  }

  class LatLonSchema(name: String, fields: Seq[String], types: Seq[Char], tField: String, latField: String, lonField: String)
    extends Schema(name: String, fields: Seq[String], types: Seq[Char], tField: String) {
    val gf = new GeometryFactory()

    def extractGeometry(entries: Map[String, Any]): Try[(Geometry, Map[String, Any])] =
      for {
        latEntry <- Try { entries.getOrElse(latField, throw new Exception(s"Cannot find latitude field $latField")) }
        lat      <- Try { latEntry.asInstanceOf[Double] }
        lonEntry <- Try { entries.getOrElse(lonField, throw new Exception(s"Cannot find longitude field $lonField")) }
        lon      <- Try { lonEntry.asInstanceOf[Double] }
      } yield {
        val spatialData = gf.createPoint(new Coordinate(lon, lat))
        (spatialData, entries - (latField, lonField))
      }
  }

  import Parsable._

  // need a sequence of which ones to try, and in which order; Int comes before Double; String comes last
  val parsers: Seq[Parsable[_]] =
    Seq(
         IntIsParsable,
         DoubleIsParsable,
         TimeIsParsable,
         GeometryIsParsable,
         StringIsParsable
       )

  def buildFeatureCollection(lines: Stream[String], schema: Schema): DefaultFeatureCollection = {
    val ft = schema.ft
    val featureFactory = CommonFactoryFinder.getFeatureFactory(null)
    val builder = new SimpleFeatureBuilder(ft, featureFactory)
    val fc = new DefaultFeatureCollection()
    for ((line, idx) <- lines.zipWithIndex) {
      val feature = for {
        parsedLine <- schema.parseLine(line)
        parsedEntries <- schema.parseEntries(parsedLine)
        (spatial, otherEntries) <- schema.extractGeometry(parsedEntries)
      } yield { // doesn't handle geometry yet!
        builder.reset()
        for ((name, value) <- otherEntries) {
          builder.set(name, value)
        }
        builder.buildFeature(idx.toString)
      }
      feature match {
        case Success(f)  => fc.add(f)
        case Failure(ex) => logger.warn(s"Failed to parse CSV line as feature:\n$line")
      }
    }
    fc
  }

//  not done yet
//  def buildShapefile(fc: FeatureCollection): File = {
//    val newFile = getNewShapeFile(file)
//    val dataStoreFactory = new ShapefileDataStoreFactory()
//  }

  def readCSV(csvPath: URI, name: String, types: Seq[Char], temporalField: String, spatialField: String) {
    val sb = (name: String, header: Seq[String], types: Seq[Char], tField: String) =>
      new GeomSchema(name, header, types, tField, spatialField)
    readCSV(csvPath, name, types, temporalField, sb)
  }

  def readCSV(csvPath: URI, name: String, types: Seq[Char], temporalField: String, latField: String, lonField: String) {
    val sb = (name: String, header: Seq[String], types: Seq[Char], tField: String) =>
      new LatLonSchema(name, header, types, tField, latField, lonField)
    readCSV(csvPath, name, types, temporalField, sb)
  }

  type SchemaBuilder = (String, Seq[String], Seq[Char], String) => Schema

  def readCSV(csvPath: URI, name: String, types: Seq[Char], temporalField: String, sb: SchemaBuilder) {
    val csvLines = Source.fromFile(csvPath).getLines()
    val header = csvLines.next().split(",")
    val schema = sb(name, header, types, temporalField)
    val fc = buildFeatureCollection(csvLines.toStream, schema)
    // need to build shapefile now
  }

  def typeData(rawData: Seq[String]): Try[Seq[Char]] =
    rawData.foldLeft[Try[Seq[Char]]](Success(Seq[Char]()))
    { case (trytypes, datum) =>
      for {
        types <- trytypes
        (_, vtype) <- parsers.foldLeft[Try[(Any, Char)]](Failure[(Any, Char)](new Exception))
                      { case (result, parser) =>
                        result orElse parser.parseAndType(datum)
                      }
      } yield {
        types :+ vtype
      }
    }

  // should we handle the exception here, log the failure, and return a blank string?
  // what's the canonical Geomesa Way to handle exceptions in web services?
  def guessTypes(csvPath: URI): Try[Seq[Char]] = {
    val csvLines = Source.fromFile(csvPath).getLines()
    val header = csvLines.next().split(",") // mostly unused in guessing types
    val firstDataLine = csvLines.next().split(",")
    for {
      _ <- Try {
                 require(firstDataLine.size == header.size,
                         "Malformed CSV; data lines must have the same number of entries as header line")
               }
      types <- typeData(firstDataLine)
    } yield {
      types
    }
  }
}
