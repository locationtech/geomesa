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

import java.io.File
import java.util.UUID

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.commons.io.FilenameUtils

import scala.collection.mutable
import scala.concurrent.Future
import scala.util.{Success, Failure}

import org.locationtech.geomesa.web.core.GeoMesaScalatraServlet
import org.locationtech.geomesa.core.{TypeSchema, csv}
import org.scalatra._
import org.scalatra.servlet.{FileUploadSupport, MultipartConfig, SizeConstraintExceededException}

class CSVEndpoint
  extends GeoMesaScalatraServlet
          with FileUploadSupport
          with Logging {
  override val root: String = "csv"

  // caps CSV file size at 10MB
  configureMultipartHandling(MultipartConfig(maxFileSize = Some(10*1024*1024)))
  error {
    case e: SizeConstraintExceededException => RequestEntityTooLarge("Uploaded file too large!")
  }

  object Record {
    import scala.concurrent.ExecutionContext.Implicits.global

    def apply(localFile: File, hasHeader: Boolean): Record =
      Record(localFile, Future(csv.guessTypes(localFile, hasHeader)), None, hasHeader)
  }
  case class Record(csvFile: File,
                    inferredSchemaF: Future[TypeSchema],
                    shapefile: Option[File],
                    hasHeader: Boolean) {
    def inferredTS: TypeSchema =
      inferredSchemaF.value.
      getOrElse(throw new Exception("Inferred schema not available yet")) match {
        case Success(ts) => ts
        case Failure(ex) => throw ex
      }
    def inferredName: String = inferredTS.name
    def inferredSchema: String = inferredTS.schema
  }
  val records = mutable.Map[String, Record]()

  post("/") {
    try {
      val fileItem = fileParams("csvfile")
      val uuid = UUID.randomUUID.toString
      val csvFile = File.createTempFile(FilenameUtils.removeExtension(fileItem.name), ".csv")
      fileItem.write(csvFile)
      val hasHeader = params.get("hasHeader").map(_.toBoolean).getOrElse(true)
      records += uuid -> Record(csvFile, hasHeader)
      Ok(uuid)
    } catch {
      case ex: Throwable =>
        logger.warn("Error uploading CSV", ex)
        NotAcceptable(body = ex, reason = ex.getMessage)
    }
  }

  get("/:csvid.csv/types") {
    try {
      val record = records(params("csvid"))
      val TypeSchema(name, schema) = record.inferredTS
      Ok(s"$name\n$schema")
    } catch {
      case ex: Throwable =>
        logger.warn("Error inferring types", ex)
        NotFound(body = ex, reason = ex.getMessage)
    }
  }

  // for lat/lon geometry, add a new geometry field to the end of the requested schema
  // and specify the latField and lonField in request parameters
  post("/:csvid.shp") {
    try {
      val csvId = params("csvid")
      val latlonFields = for (latf <- params.get("latField"); lonf <- params.get("lonField")) yield (latf, lonf)
      val record = records(csvId)
      val name = params.getOrElse("name", record.inferredName)
      val schema = params.getOrElse("schema", record.inferredSchema)
      val shapefile = csv.ingestCSV(record.csvFile, record.hasHeader, name, schema, latlonFields)
      for (shpFile <- record.shapefile) { shpFile.delete() }  // clear if one exists already
      records.update(csvId, record.copy(shapefile = Some(shapefile)))
      Ok(csvId + ".shp")
    } catch {
      case ex: Throwable =>
        logger.warn("Error creating shapefile", ex)
        NotAcceptable(body = ex, reason = ex.getMessage)
    }
  }

  get("/:csvid.shp") {
    try {
      val csvId = params("csvid")
      val record = records(csvId)
      record.shapefile match {
        case Some(shpFile) =>
          contentType = "application/octet-stream"
          response.setHeader("Content-Disposition", s"attachment; filename=${shpFile.getName}")
          Ok(shpFile)
        case None => NotFound("Shapefile content has not been created")
      }
    } catch {
      case ex: Throwable =>
        logger.warn("Error retrieving shapefile", ex)
        NotFound(body = ex, reason = ex.getMessage)
    }
  }

  private def cleanup(csvId: String) {
    for (record <- records.get(csvId)) {
      record.csvFile.delete()
      record.shapefile.foreach(_.delete())
    }
    records -= csvId
  }

  post("/:csvid.csv/delete") {
    val csvId = params("csvid")
    cleanup(csvId)
    Ok()
  }

  delete("/:csvid.csv") {
    val csvId = params("csvid")
    cleanup(csvId)
    Ok()
  }
}
