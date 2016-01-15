/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.blob.api

import java.io.{File, IOException}
import java.nio.file.Files
import java.nio.file.attribute.PosixFilePermission._
import java.nio.file.attribute.PosixFilePermissions

import org.locationtech.geomesa.utils.cache.FilePersistence
import org.scalatra._
import org.scalatra.servlet.{FileUploadSupport, MultipartConfig, SizeConstraintExceededException}

import scala.collection.JavaConversions._
import scala.util.Try

class BlobstoreServlet(val persistence: FilePersistence) extends GeoMesaPersistentBlobStoreServlet with FileUploadSupport with GZipSupport {
  override def root: String = "blob"

  val maxFileSize: Int = Try(System.getProperty(BlobstoreServlet.maxFileSizeSysProp, "50").toInt).getOrElse(50)
  val maxRequestSize: Int = Try(System.getProperty(BlobstoreServlet.maxRequestSizeSysProp, "100").toInt).getOrElse(100)
  // caps blob size
  configureMultipartHandling(
    MultipartConfig(
      maxFileSize = Some(maxFileSize * 1024 * 1024),
      maxRequestSize = Some(maxRequestSize * 1024 * 1024)
    )
  )
  error {
    case e: SizeConstraintExceededException =>
      handleError("Uploaded file too large!", e)
    case e: IOException =>
      handleError("IO exception in BlobstoreServlet", e)
  }

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

  // scalatra routes bottom up, so we want the ds post to be checked first
  post("/") {
    try {
      logger.debug("In file upload post method")
      if (abs == null) {
        handleError("AccumuloBlobStore is not initialized.", null)
      } else {
        fileParams.get("file") match {
          case None =>
            handleError("no file parameter in request", null)
          case Some(file) =>
            val otherParams = multiParams.toMap.map { case (s, p) => s -> p.head }
            val tempFile = new File(tempDir.toString + '/' + file.getName)
            file.write(tempFile)
            val actRes: ActionResult = abs.put(tempFile, otherParams) match {
              case Some(id) =>
                Created(body = id, headers = Map("Location" -> request.getRequestURL.append(id).toString))
              case None =>
                UnprocessableEntity(reason = "Unable to process file")
            }
            tempFile.delete()
            actRes
        }
      }
    } catch {
      case e: Exception => handleError("Error uploading file", e)
    }
  }

}

object BlobstoreServlet {
  val permissions  = PosixFilePermissions.asFileAttribute(Set(OWNER_READ, OWNER_WRITE, OWNER_EXECUTE, GROUP_READ, GROUP_WRITE))
  val maxFileSizeSysProp = "org.locationtech.geomesa.blob.api.maxFileSizeMB"
  val maxRequestSizeSysProp = "org.locationtech.geomesa.blob.api.maxRequestSizeMB"
}
