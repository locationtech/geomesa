package org.locationtech.geomesa.web.csv

import java.io.File
import java.util.UUID

import org.apache.commons.io.FilenameUtils

import scala.collection.mutable
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

import org.locationtech.geomesa.web.core.GeoMesaScalatraServlet
import org.locationtech.geomesa.core.csv
import org.scalatra._
import org.scalatra.servlet.{FileUploadSupport, MultipartConfig, SizeConstraintExceededException}

class CSVEndpoint
  extends GeoMesaScalatraServlet
          with FileUploadSupport {
  override val root: String = "csv"

  // caps file size at 3MB (from Scalatra examples); what is more realistic for us?
  configureMultipartHandling(MultipartConfig(maxFileSize = Some(3*1024*1024)))
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
    val response =
      for (fileItem <- Try { fileParams("csvfile") }) yield {
        val uuid = UUID.randomUUID.toString
        val csvFile = File.createTempFile(FilenameUtils.removeExtension(fileItem.name),".csv")
        fileItem.write(csvFile)
        records += uuid -> Record(csvFile)
        Ok(uuid)
      }

    response.recover { case ex => NotAcceptable(body = ex, reason = ex.getMessage) }
            .get
  }

  get("/:csvid.csv/types") {
    val response =
      for {
        record                       <- Try { records(params("csvid")) }
        csv.TypeSchema(name, schema) <- record.inferredTS
      } yield {
        Ok(s"$name\n$schema")
      }

    response.recover { case ex => NotFound(body = ex, reason = ex.getMessage) }
            .get
  }

  post("/:csvid.shp") {
    val csvId = params("csvid")
    val tryShpURI = for {
      record    <- Try { records(csvId) }
      name      <- Try { params("name") }   orElse record.inferredName
      schema    <- Try { params("schema") } orElse record.inferredSchema
      shapefile <- ingestCSV(record.csvFile, name, schema)
    } yield {
      records.update(csvId, record.copy(shapefile = Some(shapefile)))
      csvId + ".shp"
    }
    tryShpURI.getOrElse("") // what's the Right Thing to Do with exceptions?
  }

  // dummied here to build; should be handled by csv ingest code in core/tools
  def ingestCSV(csvPath: File, name: String, schema: String): Try[File] =
    Success {
      new File(csvPath.getParentFile,
        csvPath.getName.replace(".csv", ".shp"))
    }
}
