package org.locationtech.geomesa.core

import java.io._
import java.lang.{Double => jDouble, Integer => jInt}
import java.util.Date
import java.util.zip.{ZipEntry, ZipOutputStream}

import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory, Point}
import org.apache.commons.csv.CSVFormat
import org.geotools.data.DefaultTransaction
import org.geotools.data.shapefile.{ShapefileDataStore, ShapefileDataStoreFactory}
import org.geotools.data.simple.{SimpleFeatureStore, SimpleFeatureCollection}
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.feature.DefaultFeatureCollection
import org.locationtech.geomesa.core.csv.Parsable._
import org.locationtech.geomesa.core.util.SftBuilder
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType

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

  def guessTypes(csvFile: File): Future[TypeSchema] =
    for {       // Future{} ensures we're working in the Future monad
      filename <- Future { csvFile.getName }
      typename <- Future { filename.substring(0, filename.length - 4) } // assumes a filename ending in ".csv"
      reader   <- Future { Source.fromFile(csvFile).bufferedReader() }
      guess    <- guessTypes(typename, reader)
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

  def guessTypes(name: String, csvReader: Reader, format: CSVFormat = CSVFormat.DEFAULT): Future[TypeSchema] =
    Future {
             val records = format.parse(csvReader).iterator
             (for {
               header    <- Try { records.next }
               record    <- Try { records.next }
               typeChars <- typeData(record.iterator)
             } yield {
               val sftb = new SftBuilder
               var defaultDateSet = false
               var defaultGeomSet = false
               for ((field, c) <- header.iterator.zip(typeChars.iterator)) { c match {
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

  private def buildFeatureCollection(csvFile: File,
                                     sft: SimpleFeatureType,
                                     latlonFields: Option[(String, String)]): Try[SimpleFeatureCollection] = {
    def idxOfField(fname: String) = {
      sft.getType(fname)
      val idx = sft.indexOf(fname)
      if (idx > -1) {
        val t = sft.getType(idx)
        if (t.getBinding == classOf[java.lang.Double]) Success(idx)
        else Failure(new IllegalArgumentException(s"field $fname is not a Double field"))
      } else Failure(new IllegalArgumentException(s"could not find field $fname"))
    }

    val reader = Source.fromFile(csvFile).bufferedReader()
    val records = CSVFormat.DEFAULT.parse(reader).iterator()
    records.next()  // burn off the header
    val fct = Try {
      val fc = new DefaultFeatureCollection
      val fb = new SimpleFeatureBuilder(sft)
      val fieldParsers = for (t <- sft.getTypes) yield { fieldParserMap(t.getBinding) }
      val latlonIdx = for ((latf, lonf) <- latlonFields) yield {
        (for (lati <- idxOfField(latf); loni <- idxOfField(lonf)) yield (lati, loni)).get
      }
      for (record <- records) {
        fb.reset()
        val fieldVals =
          tryTraverse(record.iterator.toIterable.zip(fieldParsers)) { case (v, p) => p.parse(v) }.get.toArray
        fb.addAll(fieldVals)
        for ((lati, loni) <- latlonIdx) {
          val lat = fieldVals(lati).asInstanceOf[jDouble] // should be Doubles, as verifie
          val lon = fieldVals(loni).asInstanceOf[jDouble]
          fb.add(gf.createPoint(new Coordinate(lon, lat)))
        }
        val f = fb.buildFeature(null)
        fc.add(f)
      }
      fc
        }
    fct.eventually(reader.close())
    fct
  }

  private def shpDataStore(shpFile: File, sft: SimpleFeatureType): Try[ShapefileDataStore] =
    Try {
          val dsFactory = new ShapefileDataStoreFactory
          val params =
            Map("url" -> shpFile.toURI.toURL,
                "create spatial index" -> java.lang.Boolean.TRUE)
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
    val makeFile: String => File = {
      val shpFileDir  = shpFile.getParent
      val shpFileName = shpFile.getName
      val shpFileRoot = shpFileName.substring(0, shpFileName.length - 4)
      ext => new File(shpFileDir, s"$shpFileRoot.$ext")
    }

    val files = for (ext <- Seq("dbf", "fix", "prj", "shp", "shx")) yield { makeFile(ext) }
    val zipFile = makeFile("zip")

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

    for (_ <- writeZipData) yield { zipFile }
  }

  def ingestCSV(csvFile: File,
                name: String,
                schema: String,
                latlonFields: Option[(String, String)] = None): Try[File] =
    for {
      sft     <- Try { SimpleFeatureTypes.createType(name, schema) }
      fc      <- buildFeatureCollection(csvFile, sft, latlonFields)
      shpFile <- Try {
                       val csvFileName = csvFile.getName
                       val shpFileRoot = csvFileName.substring(0, csvFileName.length - 4)
                       new File(csvFile.getParentFile, s"$shpFileRoot.shp")
                     }
      shpDS   <- shpDataStore(shpFile, sft)
      shpFS   <- Try { shpDS.getFeatureSource(name).asInstanceOf[SimpleFeatureStore] }
      _       <- writeFeatures(fc, shpFS)
      zipFile <- writeZipFile(shpFile)
    } yield zipFile
}
