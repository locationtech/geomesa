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

package org.locationtech.geomesa.web.csv

import java.io.{ByteArrayOutputStream, OutputStream, BufferedOutputStream, File}
import java.net.URL
import java.nio.charset.Charset
import java.util.UUID
import java.util.concurrent.TimeUnit

import com.google.common.cache._
import com.typesafe.scalalogging.slf4j.Logging
import org.apache.commons.io.FilenameUtils
import org.eclipse.xsd._
import org.eclipse.xsd.util.{XSDConstants, XSDResourceImpl}
import org.geotools.GML
import org.geotools.gml.producer.FeatureTransformer
import org.locationtech.geomesa.core.{TypeSchema, csv}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.web.core.GeoMesaScalatraServlet
import org.scalatra._
import org.scalatra.servlet.{FileUploadSupport, MultipartConfig, SizeConstraintExceededException}

class CSVEndpoint extends GeoMesaScalatraServlet with FileUploadSupport with Logging {

  override val root: String = "csv"

  // caps CSV file size at 10MB
  configureMultipartHandling(MultipartConfig(maxFileSize = Some(10*1024*1024)))
  error {
    case e: SizeConstraintExceededException => RequestEntityTooLarge("Uploaded file too large!")
  }

  class Record(val csvFile: File, val hasHeader: Boolean, var schema: TypeSchema)

  val records: Cache[String, Record] = {
    val removalListener = new RemovalListener[String, Record]() {
      override def onRemoval(notification: RemovalNotification[String, Record]) =
        cleanup(notification.getKey, notification.getValue)
    }
    CacheBuilder.newBuilder()
        .expireAfterAccess(1, TimeUnit.HOURS)
        .removalListener(removalListener)
        .build()
  }

  post("/") {
    try {
      val fileItem = fileParams("csvfile")
      val uuid = UUID.randomUUID.toString
      val csvFile = File.createTempFile(FilenameUtils.removeExtension(fileItem.name), ".csv")
      fileItem.write(csvFile)
      val hasHeader = params.get("hasHeader").map(_.toBoolean).getOrElse(true)
      val schema = csv.guessTypes(csvFile, hasHeader)
      records.put(uuid, new Record(csvFile, hasHeader, schema))
      Ok(uuid)
    } catch {
      case ex: Throwable =>
        logger.warn("Error uploading CSV", ex)
        NotAcceptable(reason = ex.getMessage)
    }
  }

  get("/types/:csvid") {
    val record = records.getIfPresent(params("csvid"))
    if (record == null) {
      NotFound()
    } else {
      val TypeSchema(name, schema, _) = record.schema
      Ok(s"$name\n$schema")
    }
  }

  post("/types/update/:csvid") {
    val id = params("csvid")
    val record = records.getIfPresent(id)
    if (record == null) {
      BadRequest(reason = s"$id doesn't exist")
    } else {
      val name = params.getOrElse("name", record.schema.name)
      val schema = params.getOrElse("schema", record.schema.schema)
      val latLon = for (latf <- params.get("latField"); lonf <- params.get("lonField")) yield (latf, lonf)
      record.schema = TypeSchema(name, schema, latLon)
      Ok()
    }
  }

  get("/:csvid.gml") {
    val id = params("csvid")
    val record = records.getIfPresent(id)
    if (record == null) {
      NotFound()
    } else {
      contentType = "application/xml"
      val file = record.csvFile
      val header = record.hasHeader
      try {
        // before running the gml code, first create the XSD, otherwise it can cause deadlocks in geotools
        getXsd(id, record, new ByteArrayOutputStream())

        val fc = csv.csvToFeatures(file, header, record.schema)
        val out = new BufferedOutputStream(response.getOutputStream)
        val transformer = new FeatureTransformer()
        transformer.getFeatureTypeNamespaces.declareNamespace(fc.getSchema,
          "geomesa", s"feat:geomesa:$id")
        transformer.addSchemaLocation(s"feat:geomesa:$id",
          request.getRequestURL.toString.replaceAll("gml$", "xsd"))

        transformer.setIndentation(2)
        transformer.setCollectionBounding(true)
        transformer.setEncoding(Charset.forName("utf-8"))
        transformer.setGmlPrefixing(true)
        transformer.setSrsName("http://www.opengis.net/gml/srs/epsg.xml#4326")

        transformer.transform(fc, out)
        out.flush()

        Ok()
      } catch {
        case ex: Throwable =>
          logger.error("Error creating GML", ex)
          InternalServerError()
      }
    }
  }

  get("/:csvid.xsd") {
    val id = params("csvid")
    val record = records.getIfPresent(id)
    if (record == null) {
      NotFound()
    } else {
      contentType = "application/xml"
      try {
        val out = new BufferedOutputStream(response.getOutputStream)
        getXsd(id, record, out)
        out.flush()
        Ok()
      } catch {
        case ex: Throwable =>
          logger.error("Error creating GML", ex)
          InternalServerError()
      }
    }
  }

  def getXsd(id: String, record: Record, out: OutputStream) = {
    record.synchronized {
      val sft = SimpleFeatureTypes.createType(record.schema.name, record.schema.schema)
      val gml = new GML(GML.Version.GML2)
      gml.setBaseURL(new URL("http://localhost"))
      gml.setNamespace("geomesa", s"feat:geomesa:$id")
      gml.encode(out, sft)
    }
  }

  post("/delete/:csvid.csv") {
    val id = params("csvid")
    Option(records.getIfPresent(id)).foreach(cleanup(id, _))
    Ok()
  }

  delete("/:csvid.csv") {
    val id = params("csvid")
    Option(records.getIfPresent(id)).foreach(cleanup(id, _))
    Ok()
  }

  private def cleanup(id: String, record: Record) {
    record.csvFile.delete()
    records.invalidate(id)
  }
}
