package org.locationtech.geomesa.web.csv

import java.io.File
import java.util.UUID

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.commons.io.FilenameUtils

import scala.collection.mutable
import scala.concurrent.Future
import scala.util.{Failure, Try}

import org.locationtech.geomesa.web.core.GeoMesaScalatraServlet
import org.locationtech.geomesa.core.csv
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
    def apply(localFile: File): Record =
      Record(localFile, csv.guessTypes(localFile), None)
  }
  case class Record(csvFile: File, inferredSchemaF: Future[csv.TypeSchema], shapefile: Option[File]) {
    def inferredTS: Try[csv.TypeSchema] =
      inferredSchemaF.value.getOrElse(Failure(new Exception("Inferred schema not available yet")))
    def inferredName: Try[String] = inferredTS.map(_.name)
    def inferredSchema: Try[String] = inferredTS.map(_.schema)
  }
  val records = mutable.Map[String, Record]()

  post("/") {
    val idResponse =
      for (fileItem <- Try { fileParams("csvfile") }) yield {
        val uuid = UUID.randomUUID.toString
        val csvFile = File.createTempFile(FilenameUtils.removeExtension(fileItem.name),".csv")
        fileItem.write(csvFile)
        records += uuid -> Record(csvFile)
        Ok(uuid)
      }

    idResponse.recover { case ex =>
      logger.warn("Error uploading CSV", ex)
      NotAcceptable(body = ex, reason = ex.getMessage)
    }.get
  }

  get("/:csvid.csv/types") {
    val schemaResponse =
      for {
        record                       <- Try { records(params("csvid")) }
        csv.TypeSchema(name, schema) <- record.inferredTS
      } yield {
        Ok(s"$name\n$schema")
      }

    schemaResponse.recover { case ex =>
      logger.warn("Error inferring types", ex)
      NotFound(body = ex, reason = ex.getMessage)
    }.get
  }

  // for lat/lon geometry, add a new geometry field to the end of the requested schema
  // and specify the latField and lonField in request parameters
  post("/:csvid.shp") {
    val csvId = params("csvid")
    val latlonFields = for (latf <- params.get("latField"); lonf <- params.get("lonField")) yield (latf, lonf)
    val urlResponse = for {
      record    <- Try { records(csvId) }
      name      <- Try { params("name") }   orElse record.inferredName
      schema    <- Try { params("schema") } orElse record.inferredSchema
      shapefile <- csv.ingestCSV(record.csvFile, name, schema, latlonFields)
    } yield {
      records.update(csvId, record.copy(shapefile = Some(shapefile)))
      Ok(csvId + ".shp")
    }

    urlResponse.recover { case ex =>
      logger.warn("Error creating shapefile", ex)
      NotAcceptable(body = ex, reason = ex.getMessage)
    }.get
  }

  get("/:csvid.shp") {
    val csvId = params("csvid")
    val fileResponse = for (record <- Try { records(csvId) }) yield {
      record.shapefile match {
        case Some(shpFile) =>
          contentType = "application/octet-stream"
          response.setHeader("Content-Disposition", s"attachment; filename=${shpFile.getName}")
          Ok(shpFile)
        case None => NotFound("Shapefile content has not been created")
      }
    }

    fileResponse.recover { case ex =>
      logger.warn("Error retrieving shapefile", ex)
      NotFound(body = ex, reason = ex.getMessage)
    }.get
  }
}
