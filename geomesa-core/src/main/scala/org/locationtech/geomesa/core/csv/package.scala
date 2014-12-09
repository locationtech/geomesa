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

package org.locationtech.geomesa.core

import java.io._
import java.lang.{Double => jDouble, Integer => jInt}
import java.util.{Date, Iterator => jIterator}
import java.util.zip.{ZipEntry, ZipOutputStream}

import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory, Point}
import org.apache.commons.csv.{CSVRecord, CSVFormat}
import org.apache.commons.io.FilenameUtils
import org.geotools.data.DefaultTransaction
import org.geotools.data.shapefile.{ShapefileDataStore, ShapefileDataStoreFactory}
import org.geotools.data.simple.{SimpleFeatureStore, SimpleFeatureCollection}
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.feature.DefaultFeatureCollection
import org.locationtech.geomesa.core.csv.Parsable._
import org.locationtech.geomesa.core.util.SftBuilder
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._
import scala.collection.generic.CanBuildFrom
import scala.concurrent.Future
import scala.io.Source
import scala.util.{Failure, Success, Try}

package object csv extends Logging {
  // a couple things to make Try work better
  def tryTraverse[A, B, M[_] <: TraversableOnce[_]](in: M[A])(fn: A => Try[B])
                                                   (implicit cbf: CanBuildFrom[M[A], B, M[B]]): Try[M[B]] =
    in.foldLeft(Try(cbf(in))) { (tr, a) =>
      for (r <- tr; b <- fn(a.asInstanceOf[A])) yield r += b
    }.map(_.result())

  implicit class TryOps[A](val t: Try[A]) extends AnyVal {
    def eventually[Ignore](effect: => Ignore): Try[A] = {
      val ignoring = (_: Any) => { effect; t }
      t.transform(ignoring, ignoring)
    }
  }

  case class TypeSchema(name: String, schema: String)

  import scala.concurrent.ExecutionContext.Implicits.global

  def guessTypes(csvFile: File, hasHeader: Boolean): Future[TypeSchema] =
    for {       // Future{} ensures we're working in the Future monad
      typename <- Future { FilenameUtils.getBaseName(csvFile.getName) }
      reader   =  Source.fromFile(csvFile).bufferedReader()
      guess    <- guessTypes(typename, reader, hasHeader)
    } yield {
      reader.close()
      guess
    }

  // this can probably be cleaned up and simplified now that parsers don't need to do double duty...
  def typeData(rawData: TraversableOnce[String]): Try[Seq[Char]] = {
    def tryAllParsers(datum: String): Try[(Any, Char)] =
      Parsable.parsers.view.map(_.parseAndType(datum)).collectFirst { case Success(x) => x } match {
        case Some(x) => Success(x)
        case None    => Failure(new IllegalArgumentException(s"Could not parse $datum as any known type"))
      }

    tryTraverse(rawData)(tryAllParsers(_).map { case (_, c) => c }).map(_.toSeq)
  }

  def sampleRecords(records: jIterator[CSVRecord], hasHeader: Boolean): Try[(Seq[String], CSVRecord)] =
    Try {
      if (hasHeader) {
        val header = records.next
        val record = records.next
        (header.toSeq, record)
      } else {
        val record = records.next
        val header = Seq.tabulate(record.size()) { n => s"C$n" }
        (header, record)
      }
    }

  def guessTypes(name: String,
                 csvReader: Reader,
                 hasHeader: Boolean = true,
                 format: CSVFormat = CSVFormat.DEFAULT): Future[TypeSchema] =
    Future {
      val records = format.parse(csvReader).iterator
      (for {
        (header, record) <- sampleRecords(records, hasHeader)
        typeChars        <- typeData(record.iterator)
      } yield {
        val sftb = new SftBuilder
        var defaultDateSet = false
        var defaultGeomSet = false
        for ((field, c) <- header.zip(typeChars)) { c match {
          case 'i' =>
            sftb.intType(field)
          case 'd' =>
            sftb.doubleType(field)
          case 't' =>
            sftb.date(field)
            if (!defaultDateSet) {
              sftb.withDefaultDtg(field)
              defaultDateSet = true
            }
          case 'p' =>
            if (defaultGeomSet) sftb.geometry(field)
            else {
              sftb.point(field, default = true)
              defaultGeomSet = true
            }
          case 's' =>
            sftb.stringType(field)
        }}

        TypeSchema(name, sftb.getSpec())
      }).get
    }

  val fieldParserMap =
    Map[Class[_], Parsable[_ <: AnyRef]](
      classOf[jInt]    -> IntIsParsable,
      classOf[jDouble] -> DoubleIsParsable,
      classOf[Date]    -> TimeIsParsable,
      classOf[Point]   -> PointIsParsable,
      classOf[String]  -> StringIsParsable
    )

  val gf = new GeometryFactory

  protected[csv] def buildFeatureCollection(csvFile: File,
                                            hasHeader: Boolean,
                                            sft: SimpleFeatureType,
                                            latlonFields: Option[(String, String)]): Try[SimpleFeatureCollection] = {
    val reader = Source.fromFile(csvFile).bufferedReader()
    buildFeatureCollection(reader, hasHeader, sft, latlonFields).eventually(reader.close())
  }

  protected[csv] def buildFeatureCollection(reader: Reader,
                                            hasHeader: Boolean,
                                            sft: SimpleFeatureType,
                                            latlonFields: Option[(String, String)]): Try[SimpleFeatureCollection] =
    Try {
      def idxOfField(fname: String) = {
        sft.getType(fname)
        val idx = sft.indexOf(fname)
        if (idx > -1) {
          val t = sft.getType(idx)
          if (t.getBinding == classOf[java.lang.Double]) Success(idx)
          else Failure(new IllegalArgumentException(s"field $fname is not a Double field"))
        } else Failure(new IllegalArgumentException(s"could not find field $fname"))
      }

      val latlonIdx = for ((latf, lonf) <- latlonFields) yield {
        (for (lati <- idxOfField(latf); loni <- idxOfField(lonf)) yield (lati, loni)).get
      }
      val fb = new SimpleFeatureBuilder(sft)
      val fieldParsers = for (t <- sft.getTypes) yield { fieldParserMap(t.getBinding) }

      def buildFeature(record: CSVRecord): Option[SimpleFeature] =
        Try {
          fb.reset()
          val fieldVals =
            tryTraverse(record.iterator.toIterable.zip(fieldParsers)) { case (v, p) => p.parse(v) }.get.toArray
          fb.addAll(fieldVals)
          for ((lati, loni) <- latlonIdx) {
            val lat = fieldVals(lati).asInstanceOf[jDouble] // should be Doubles, as verified
            val lon = fieldVals(loni).asInstanceOf[jDouble] // when determining latlonIdx
            fb.add(gf.createPoint(new Coordinate(lon, lat)))
          }
          fb.buildFeature(null)
        } match {
          case Success(f)  => Some(f)
          case Failure(ex) => logger.info(s"Failed to parse CSV record:\n$record"); None
        }

      val fc = new DefaultFeatureCollection
      val records = CSVFormat.DEFAULT.parse(reader).iterator()
      if (hasHeader) { records.next } // burn off header rather than try (and fail) to parse it.
      for {
        record <- records
        f      <- buildFeature(record) // logs and discards lines that fail to parse but keeps processing
      } fc.add(f)
      fc
    }

  private val dsFactory = new ShapefileDataStoreFactory

  private def shpDataStore(shpFile: File, sft: SimpleFeatureType): Try[ShapefileDataStore] =
    Try {
      val params =
        Map("url" -> shpFile.toURI.toURL,
            "create spatial index" -> java.lang.Boolean.FALSE)
      val shpDS = dsFactory.createNewDataStore(params).asInstanceOf[ShapefileDataStore]
      shpDS.createSchema(sft)
      shpDS
    }
  
  private def writeFeatures(fc: SimpleFeatureCollection, shpFS: SimpleFeatureStore): Try[Unit] = {
    val transaction = new DefaultTransaction("create")
    shpFS.setTransaction(transaction)
    Try { shpFS.addFeatures(fc); transaction.commit() } recover {
      case ex => transaction.rollback(); throw ex
    } eventually { transaction.close() }
  }

  private def writeZipFile(shpFile: File): Try[File] = {
    def byteStream(in: InputStream): Stream[Int] = { in.read() #:: byteStream(in) }

    val dir  = shpFile.getParent
    val rootName = FilenameUtils.getBaseName(shpFile.getName)

    val extensions = Seq("dbf", "fix", "prj", "shp", "shx")
    val files = extensions.map(ext => new File(dir, s"$rootName.$ext"))
    val zipFile = new File(dir, s"$rootName.zip")

    def writeZipData = {
      val zip = new ZipOutputStream(new FileOutputStream(zipFile))
      Try {
        for (file <- files) {
          zip.putNextEntry(new ZipEntry(file.getName))
          val in = new FileInputStream(file.getCanonicalFile)
          (Try {byteStream(in).takeWhile(_ > -1).toList.foreach(zip.write)} eventually in.close()).get
          zip.closeEntry()
        }
      } eventually zip.close()
    }

    for (_ <- writeZipData) yield {
      for (file <- files) file.delete()
      zipFile
    }
  }

  def ingestCSV(csvFile: File,
                hasHeader: Boolean,
                name: String,
                schema: String,
                latlonFields: Option[(String, String)] = None): Try[File] =
    for {
      sft     <- Try { SimpleFeatureTypes.createType(name, schema) }
      fc      <- buildFeatureCollection(csvFile, hasHeader, sft, latlonFields)
      shpFile <- Try { new File(csvFile.getParentFile, s"${FilenameUtils.getBaseName(csvFile.getName)}.shp") }
      shpDS   <- shpDataStore(shpFile, sft)
      shpFS   <- Try { shpDS.getFeatureSource(name).asInstanceOf[SimpleFeatureStore] }
      _       <- writeFeatures(fc, shpFS)
      zipFile <- writeZipFile(shpFile)
    } yield zipFile
}
