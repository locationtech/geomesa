package org.locationtech.geomesa.web.csv

import java.io.File
import java.util.UUID

import org.locationtech.geomesa.web.core.GeoMesaScalatraServlet
import org.scalatra.{RequestEntityTooLarge, Ok}
import org.scalatra.servlet.{SizeConstraintExceededException, MultipartConfig, FileUploadSupport}
import org.springframework.security.core.context.SecurityContextHolder

import scala.collection.mutable
import scala.concurrent.Future

class CSVEndpoint
  extends GeoMesaScalatraServlet
          with FileUploadSupport {
  override val root: String = "csv/"

  // caps file size at 3MB (from Scalatra examples); what is more realistic for us?
  configureMultipartHandling(MultipartConfig(maxFileSize = Some(3*1024*1024)))
  error {
    case e: SizeConstraintExceededException => RequestEntityTooLarge("Uploaded file too large!")
  }

  import scala.concurrent.ExecutionContext.Implicits.global
//  implicit val csvParser =
//    CSVFormat.newFormat(',')
//             .withQuote('"')
//             .withEscape('\\')
//             .withIgnoreEmptyLines(true)
//             .withCommentMarker('#')
//             .withIgnoreSurroundingSpaces(true)
//             .withSkipHeaderRecord(true)
//             .withRecordSeparator('\n')

  val csvData = mutable.Map[String, (File, Future[String])]()

  get("/") {
    val principal = SecurityContextHolder.getContext.getAuthentication.getPrincipal
    s"Hello $principal"
  }

  get("/:that") {
    s"What's ${params("that")}?"
  }

  post("/") {
    val uuid = UUID.randomUUID.toString
    val csvFile = File.createTempFile(uuid, "csv")
    fileParams("csvfile").write(csvFile)
    csvData + uuid -> (csvFile, guessTypes(csvFile))
    Ok(uuid)
  }

  // dummied here to build; should be handled by csv type-guessing code in core
  def guessTypes(csvPath: File) = Future { "types!" }

//  get("/:uuid")
}
