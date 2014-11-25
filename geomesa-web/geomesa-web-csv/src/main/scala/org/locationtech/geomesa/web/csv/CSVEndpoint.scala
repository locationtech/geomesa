package org.locationtech.geomesa.web.csv

import java.io.File
import java.util.UUID
import javax.servlet.http.HttpServletRequest

import org.locationtech.geomesa.web.core.GeoMesaScalatraServlet
import org.scalatra._
import org.scalatra.servlet.{SizeConstraintExceededException, MultipartConfig, FileUploadSupport}

import scala.collection.mutable
import scala.concurrent.Future
import scala.util.{Success, Try, Failure}

class CSVEndpoint
  extends GeoMesaScalatraServlet
          with FileUploadSupport {

  // This overridden method is purely about diagnosing the errors in routing a sub-servlet under geoserver
  // it should be removed before anything gets merged into geomesa.
  override protected def runRoutes(routes: Traversable[Route]) = {
    def testRequestPath(request: HttpServletRequest) {
      import org.scalatra.util.RicherString._

      val uri = Try { request.getRequestURI } match {
        case Success(u) => println(s"got request URI $u"); u
        case Failure(_) => println("No request URI; using \"/\""); "/"
      }
      val idx = {
        val ctxPathOpt = request.getContextPath.blankOption
        println(s"context path: $ctxPathOpt")
        val srvPathOpt = request.getServletPath.blankOption
        println(s"servlet path: $srvPathOpt")

        ctxPathOpt.map(_.length).getOrElse(0) +
        srvPathOpt.map(_.length).getOrElse(0)
      }
      println(s"start index $idx")
      val path = {
        val u1 = UriDecoder.firstStep(uri)
        println(s"first step decoding: $u1")
        val u2 = u1.blankOption map {_.substring(idx)}
        u2.foreach(s => println(s"substring: $s"))
        val u3 = u2 flatMap (_.blankOption) getOrElse "/"
        println(s"pre-clip: $u3")
        // clips until ';'
        val pos = u3.indexOf(';')
        if (pos > -1) u3.substring(0, pos) else u3
      }
      println(s"got request path $path")
      path
    }

    testRequestPath(implicitly[HttpServletRequest])
    super.runRoutes(routes)
  }


  override val root: String = "csv"

  // caps file size at 3MB (from Scalatra examples); what is more realistic for us?
  configureMultipartHandling(MultipartConfig(maxFileSize = Some(3*1024*1024)))
  error {
    case e: SizeConstraintExceededException => RequestEntityTooLarge("Uploaded file too large!")
  }

  import scala.concurrent.ExecutionContext.Implicits.global

  object Record {
    def apply(localFile: File): Record =
      Record(localFile, guessTypes(localFile), None)
  }
  case class Record(csvFile: File, inferredSchemaF: Future[String], shapefile: Option[File]) {
    def inferredSchema: Try[String] =
      inferredSchemaF.value.getOrElse(Failure(new Exception("Inferred schema not available yet")))
  }
  val records = mutable.Map[String, Record]()

  post("/") {
    val uuid = UUID.randomUUID.toString
    val csvFile = File.createTempFile(uuid, "csv")
    fileParams("csvfile").write(csvFile)
    records + uuid -> Record(csvFile)
    Ok(uuid)
  }

  // dummied here to build; should be handled by csv type-guessing code in core
  def guessTypes(csvPath: File) = Future { "types!" }

  get("/:csvid.csv/types") {
    records(params("csvid")).inferredSchema
                            .getOrElse("")  // what's the Right Thing to Do with exceptions?
  }

  post("/:csvid.shp") {
    val csvId = params("csvid")
    val tryShpURI = for {
      record    <- Try { records(csvId) }
      schema    <- Try { params("schema") } orElse record.inferredSchema
      shapefile <- ingestCSV(record.csvFile, schema)
    } yield {
      records.update(csvId, record.copy(shapefile = Some(shapefile)))
      csvId + ".shp"
    }
    tryShpURI.getOrElse("") // what's the Right Thing to Do with exceptions?
  }

  // dummied here to build; should be handled by csv ingest code in core/tools
  def ingestCSV(csvPath: File, schema: String): Try[File] =
    Success {
              new File(csvPath.getParentFile,
                       csvPath.getName.replace(".csv", ".shp"))
            }
}
