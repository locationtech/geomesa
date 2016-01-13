/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.blob.api

import java.io.{IOException, File}
import java.nio.file.Files
import java.nio.file.attribute.PosixFilePermission._
import java.nio.file.attribute.PosixFilePermissions

import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloDataStoreFactory}
import org.locationtech.geomesa.blob.core.AccumuloBlobStore
import org.locationtech.geomesa.web.core.GeoMesaScalatraServlet
import org.scalatra._
import org.scalatra.servlet.{FileUploadSupport, MultipartConfig, SizeConstraintExceededException}

import scala.collection.JavaConversions._
import scala.util.control.NonFatal

class BlobstoreServlet extends GeoMesaScalatraServlet with FileUploadSupport with GZipSupport {
  override def root: String = "blob"

  // caps blob size at 10MB
  configureMultipartHandling(
    MultipartConfig(
      maxFileSize = Some(50*1024*1024),
      maxRequestSize = Some(100*1024*1024)
    )
  )
  error {
    case e: SizeConstraintExceededException => RequestEntityTooLarge("Uploaded file too large!")
    case e: IOException => halt(500, "IO Exception on server")
  }

  var abs: AccumuloBlobStore = null

  val tempDir = Files.createTempDirectory("blobs", BlobstoreServlet.permissions)

  delete("/:id/?") {
    val id = params("id")
    logger.debug("In DELETE method for blobstore, attempting to delete: {}", id)
    if (abs == null) {
      NotFound(reason = "AccumuloBlobStore is not initialized.")
    } else {
      abs.delete(id)
      NoContent(reason = s"deleted feature: $id")
    }
  }

  get("/:id/?") {
    val id = params("id")
    logger.debug("In ID method, trying to retrieve id {}", id)
    if (abs == null) {
      NotFound(reason = "AccumuloBlobStore is not initialized.")
    } else {
      val (returnBytes, filename) = abs.get(id)
      if (returnBytes == null) {
        NotFound(reason = s"Unknown ID $id")
      } else {
        contentType = "application/octet-stream"
        response.setHeader("Content-Disposition", "attachment;filename=" + filename)

        Ok(returnBytes)
      }
    }
  }

  // TODO: Revisit configuration and persistence of configuration.
  // https://geomesa.atlassian.net/browse/GEOMESA-958
  // https://geomesa.atlassian.net/browse/GEOMESA-984
  post("/ds/?") {
    logger.debug("In ds registration method")

    val dsParams = datastoreParams
    val ds = new AccumuloDataStoreFactory().createDataStore(dsParams).asInstanceOf[AccumuloDataStore]

    if (ds == null) {
      NotFound(reason = "Could not load data store using the provided parameters.")
    } else {
      this.synchronized {
        abs = new AccumuloBlobStore(ds)
      }
      //see https://httpstatuses.com/204
      NoContent()
    }
  }

  // scalatra routes bottom up, so we want the ds post to be checked first
  post("/") {
    try {
      logger.debug("In file upload post method")
      if (abs == null) {
        NotFound(reason = "AccumuloBlobStore is not initialized.")
      } else {
        fileParams.get("file") match {
          case None =>
            halt(400, reason = "no file parameter in request")
          case Some(file) =>
            val otherParams = multiParams.toMap.map { case (s, p) => s -> p.head }
            val tempFile = new File(tempDir.toString + '/' + file.getName)
            file.write(tempFile)
            val actRes: ActionResult = abs.put(tempFile, otherParams) match {
              case Some(id) =>
                Created(body = id, headers = Map("Location" -> s"${request.getRequestURL append id}"))
              case None =>
                UnprocessableEntity(reason = s"Unable to process file: ${file.name}")
            }
            tempFile.delete()
            actRes
        }
      }
    } catch {
      case NonFatal(ex) =>
        logger.error("Error uploading file", ex)
        UnprocessableEntity(reason = ex.getMessage)
    }
  }

}

object BlobstoreServlet {
  val permissions  = PosixFilePermissions.asFileAttribute(Set(OWNER_READ, OWNER_WRITE, OWNER_EXECUTE, GROUP_READ, GROUP_WRITE))
}
