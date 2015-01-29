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
import org.locationtech.geomesa.core.csv.CSVParser._
import org.locationtech.geomesa.core.util.SftBuilder
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._
import scala.io.Source
import scala.util.Success

package object csv extends Logging {
  case class TypeSchema(name: String, schema: String)

  // this can probably be cleaned up and simplified now that parsers don't need to do double duty...
  def typeData(rawData: TraversableOnce[String]): Seq[Char] = {
    def tryAllParsers(datum: String): Char =
      CSVParser.parsers.view.map(_.parseAndType(datum)).collectFirst { case Success(x) => x } match {
        case Some(x) => x._2
        case None    => 's'   // should get to this anyway as StringParser is guaranteed to succeed
      }

    rawData.map(tryAllParsers).toSeq
  }
  
  def sampleRecords(records: jIterator[CSVRecord], hasHeader: Boolean): (Seq[String], CSVRecord) =
    if (hasHeader) {
      val header = records.next
      val record = records.next
      (header.toSeq, record)
    } else {
      val record = records.next
      val header = Seq.tabulate(record.size()) { n => s"C$n" }
      (header, record)
    }

  def guessTypes(name: String,
                 csvReader: Reader,
                 hasHeader: Boolean = true,
                 format: CSVFormat = CSVFormat.DEFAULT): TypeSchema = {
    val records = format.parse(csvReader).iterator
    val (header, record) = sampleRecords(records, hasHeader)
    val typeChars = typeData(record.iterator)

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

    TypeSchema(name, sftb.getSpec)
  }

  def guessTypes(csvFile: File, hasHeader: Boolean): TypeSchema = {
    val typename = FilenameUtils.getBaseName(csvFile.getName)
    val reader = Source.fromFile(csvFile).bufferedReader()
    val guess = guessTypes(typename, reader, hasHeader)
    reader.close()
    guess
  }

  val fieldParserMap =
    Map[Class[_], CSVParser[_ <: AnyRef]](
      classOf[jInt]    -> IntParser,
      classOf[jDouble] -> DoubleParser,
      classOf[Date]    -> TimeParser,
      classOf[Point]   -> PointParser,
      classOf[String]  -> StringParser
    )

  val gf = new GeometryFactory

  protected[csv] def buildFeatureCollection(csvFile: File,
                                            hasHeader: Boolean,
                                            sft: SimpleFeatureType,
                                            latlonFields: Option[(String, String)]): SimpleFeatureCollection = {
    val reader = Source.fromFile(csvFile).bufferedReader()
    try {
      buildFeatureCollection(reader, hasHeader, sft, latlonFields)
    } finally {
      reader.close()
    }
  }

  def buildFeature(record: CSVRecord,
                   fb: SimpleFeatureBuilder,
                   parsers: Seq[CSVParser[_<:AnyRef]],
                   lli: Option[(Int, Int)]): Option[SimpleFeature] =
    try {
      fb.reset()
      val fieldVals = record.iterator.toIterable.zip(parsers).
                      map { case (v, p) => p.parse(v).get }.toArray
      fb.addAll(fieldVals)
      for ((lati, loni) <- lli) {
        val lat = fieldVals(lati).asInstanceOf[jDouble] // should be Doubles, as verified
        val lon = fieldVals(loni).asInstanceOf[jDouble] // when determining latlonIdx
        fb.add(gf.createPoint(new Coordinate(lon, lat)))
      }
      Some(fb.buildFeature(null))
    } catch {
      case ex: Throwable => logger.info(s"Failed to parse CSV record:\n$record"); None
    }

  // if the types in sft do not match the data in the reader, the resulting FeatureCollection will be empty.
  protected[csv] def buildFeatureCollection(reader: Reader,
                                            hasHeader: Boolean,
                                            sft: SimpleFeatureType,
                                            latlonFields: Option[(String, String)]): SimpleFeatureCollection = {
    def idxOfField(fname: String): Int = {
      sft.getType(fname)
      val idx = sft.indexOf(fname)
      if (idx > -1) {
        val t = sft.getType(idx)
        if (t.getBinding == classOf[java.lang.Double]) idx
        else throw new IllegalArgumentException(s"field $fname is not a Double field")
      } else throw new IllegalArgumentException(s"could not find field $fname")
    }

    val latlonIdx = latlonFields.map { case (latf, lonf) => (idxOfField(latf), idxOfField(lonf)) }
    val fb = new SimpleFeatureBuilder(sft)
    val parsers = sft.getTypes.map { t => fieldParserMap(t.getBinding) }
    val fc = new DefaultFeatureCollection
    val records = CSVFormat.DEFAULT.parse(reader).iterator()
    if (hasHeader) { records.next } // burn off header rather than try (and fail) to parse it.
    for {
      record <- records
      f      <- buildFeature(record, fb, parsers, latlonIdx) // logs and discards lines that fail to parse but keeps processing
    } fc.add(f)
    fc
  }

  private val dsFactory = new ShapefileDataStoreFactory

  private def shpDataStore(shpFile: File, sft: SimpleFeatureType): ShapefileDataStore = {
    val params =
      Map("url" -> shpFile.toURI.toURL,
          "create spatial index" -> java.lang.Boolean.FALSE)
    val shpDS = dsFactory.createNewDataStore(params).asInstanceOf[ShapefileDataStore]
    shpDS.createSchema(sft)
    shpDS
  }

  private def writeFeatures(fc: SimpleFeatureCollection, shpFS: SimpleFeatureStore) {
    val transaction = new DefaultTransaction("create")
    shpFS.setTransaction(transaction)
    try {
      shpFS.addFeatures(fc)
      transaction.commit()
    } catch {
      case ex: Throwable =>
        transaction.rollback()
        throw ex
    } finally {
      transaction.close()
    }
  }

  private def writeZipFile(shpFile: File): File = {
    def byteStream(in: InputStream): Stream[Int] = { in.read() #:: byteStream(in) }

    val dir  = shpFile.getParent
    val rootName = FilenameUtils.getBaseName(shpFile.getName)

    val extensions = Seq("dbf", "fix", "prj", "shp", "shx")
    val files = extensions.map(ext => new File(dir, s"$rootName.$ext"))
    val zipFile = new File(dir, s"$rootName.zip")

    val zip = new ZipOutputStream(new FileOutputStream(zipFile))
    try {
      for (file <- files if file.exists) {
        zip.putNextEntry(new ZipEntry(file.getName))
        val in = new FileInputStream(file.getCanonicalFile)
        try {
          byteStream(in).takeWhile(_ > -1).toList.foreach(zip.write)
        } finally {
          in.close()
        }
        zip.closeEntry()
      }
    } finally {
      zip.close()
    }

    for (file <- files) file.delete()
    zipFile
  }

  def ingestCSV(csvFile: File,
                hasHeader: Boolean,
                name: String,
                schema: String,
                latlonFields: Option[(String, String)] = None): File = {
    val sft = SimpleFeatureTypes.createType(name, schema)
    val fc = buildFeatureCollection(csvFile, hasHeader, sft, latlonFields)
    val shpFile = new File(csvFile.getParentFile, s"${FilenameUtils.getBaseName(csvFile.getName)}.shp")
    val shpDS = shpDataStore(shpFile, sft)
    val shpFS = shpDS.getFeatureSource(name).asInstanceOf[SimpleFeatureStore]
    writeFeatures(fc, shpFS)
    writeZipFile(shpFile)
  }
}
