/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils

import java.io._
import java.lang.{Double => jDouble, Integer => jInt}
import java.util.zip.{ZipEntry, ZipOutputStream}
import java.util.{Date, Iterator => jIterator}

import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom._
import org.apache.commons.csv.{CSVFormat, CSVRecord}
import org.apache.commons.io.FilenameUtils
import org.geotools.data.DefaultTransaction
import org.geotools.data.shapefile.{ShapefileDataStore, ShapefileDataStoreFactory}
import org.geotools.data.simple.{SimpleFeatureCollection, SimpleFeatureStore}
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.locationtech.geomesa.utils.csv.CSVParser._
import org.locationtech.geomesa.utils.geotools.{SftBuilder, SimpleFeatureTypes}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._
import scala.io.Source

case class TypeSchema(name: String, schema: String, latLonFields: Option[(String, String)])

package object csv extends LazyLogging {

  def guessTypes(csvFile: File, hasHeader: Boolean): TypeSchema = {
    val typename = FilenameUtils.getBaseName(csvFile.getName)
    val reader = Source.fromFile(csvFile).bufferedReader()
    val guess = guessTypes(typename, reader, hasHeader)
    reader.close()
    guess
  }

  def csvToFeatures(csvFile: File,
                    hasHeader: Boolean,
                    typeSchema: TypeSchema): SimpleFeatureCollection = {
    val sft = SimpleFeatureTypes.createType(typeSchema.name, typeSchema.schema)
    buildFeatureCollection(csvFile, hasHeader, sft, typeSchema.latLonFields)
  }

  protected[csv] def tryParsers(rawData: TraversableOnce[String]): Seq[CSVParser[_]] = {
    def tryAllParsers(datum: String) =
      CSVParser.parsers.find(_.parse(datum).isSuccess).getOrElse(StringParser)
    rawData.map(tryAllParsers).toSeq
  }

  protected[csv] def guessHeaders(record: CSVRecord, hasHeader: Boolean = true): Seq[String] =
    if (hasHeader) record.toSeq else Seq.tabulate(record.size())(n => s"C$n")

  protected[csv] def guessTypes(name: String,
                 csvReader: Reader,
                 hasHeader: Boolean = true,
                 format: CSVFormat = CSVFormat.DEFAULT,
                 numSamples: Int = 5): TypeSchema = {
    assert(numSamples > 0)
    val records = format.parse(csvReader).iterator.take(numSamples + 1).toSeq
    assert(records.size > 1 || (!hasHeader && records.size > 0))

    val headers = guessHeaders(records(0), hasHeader)
    val sample = records.drop(1)

    // make sure type chars are valid for first few records
    val parsers = sample.map(tryParsers(_)).reduceLeft { (pc1, pc2)  =>
      pc1.zip(pc2).map { case (p1, p2) => if (p1 == p2) p1 else StringParser }
    }

    val sftb = new SftBuilder
    var defaultGeomSet = false
    headers.zip(parsers).foreach { case (field, parser) =>
      if (!defaultGeomSet && parser.isGeom) {
        parser.buildSpec(sftb, field, true)
        defaultGeomSet = true
      } else {
        parser.buildSpec(sftb, field)
      }
    }

    TypeSchema(name, sftb.getSpec, None)
  }

  protected[csv] def getParser[A](clas: Class[A]) = clas match {
    case c if c.isAssignableFrom(classOf[jInt])            => IntParser
    case c if c.isAssignableFrom(classOf[jDouble])         => DoubleParser
    case c if c.isAssignableFrom(classOf[Date])            => TimeParser
    case c if c.isAssignableFrom(classOf[Point])           => PointParser
    case c if c.isAssignableFrom(classOf[LineString])      => LineStringParser
    case c if c.isAssignableFrom(classOf[Polygon])         => PolygonParser
    case c if c.isAssignableFrom(classOf[MultiPoint])      => MultiPointParser
    case c if c.isAssignableFrom(classOf[MultiLineString]) => MultiLineStringParser
    case c if c.isAssignableFrom(classOf[MultiPolygon])    => MultiPolygonParser
    case c if c.isAssignableFrom(classOf[Geometry])        => GeometryParser
    case c if c.isAssignableFrom(classOf[String])          => StringParser
    case _ => StringParser
  }

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
      val fieldVals = record.iterator.toIterable.zip(parsers).map { case (v, p) => p.parse(v).get }.toArray
      fb.addAll(fieldVals)
      for ((lati, loni) <- lli) {
        val lat = fieldVals(lati).asInstanceOf[jDouble] // should be Doubles, as verified
        val lon = fieldVals(loni).asInstanceOf[jDouble] // when determining latlonIdx
        fb.add(gf.createPoint(new Coordinate(lon, lat)))
      }
      Some(fb.buildFeature(null))
    } catch {
      case ex: Exception => logger.info(s"Failed to parse CSV record:\n$record"); None
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
    val parsers = sft.getTypes.map(t => getParser(t.getBinding))
    val fc = new DefaultFeatureCollection
    val records = CSVFormat.DEFAULT.parse(reader).iterator()
    if (hasHeader) { records.next } // burn off header rather than try (and fail) to parse it.
    for {
      record  <- records
      // logs and discards lines that fail to parse but keeps processing
      feature <- buildFeature(record, fb, parsers, latlonIdx)
    } {
      fc.add(feature)
    }
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
