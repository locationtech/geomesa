package org.locationtech.geomesa.web.csv

import java.io.File
import java.util.UUID

import scala.collection.mutable
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

import javax.servlet.http.HttpServletRequest
import org.locationtech.geomesa.web.core.GeoMesaScalatraServlet
import org.locationtech.geomesa.core.csv
import org.scalatra._
import org.scalatra.servlet.{FileUploadSupport, MultipartConfig, SizeConstraintExceededException}

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
    val uuid = UUID.randomUUID.toString
    val csvFile = File.createTempFile(uuid, "csv")
    fileParams("csvfile").write(csvFile)
    records + uuid -> Record(csvFile)
    Ok(uuid)
  }

  get("/:csvid.csv/types") {
    records(params("csvid")).inferredTS match {
      case Success(ts) => Ok(s"${ts.name}\n${ts.schema}")
      case Failure(ex) => NotFound(body = ex, reason = ex.getMessage)
    }
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
