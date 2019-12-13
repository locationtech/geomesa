/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.blob.web

import java.io.{File, IOException}
import java.nio.file.attribute.PosixFilePermission._
import java.nio.file.attribute.PosixFilePermissions
import java.util.concurrent.ConcurrentHashMap
import java.util.{Locale, UUID}

import javax.servlet.http.{HttpServletRequest, HttpServletRequestWrapper}
import org.apache.commons.io.FilenameUtils
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloDataStoreFactory}
import org.locationtech.geomesa.blob.accumulo.GeoMesaAccumuloBlobStore
import org.locationtech.geomesa.blob.api.{Blob, GeoMesaBlobStoreSFT}
import org.locationtech.geomesa.utils.cache.FilePersistence
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.web.core.PersistentDataStoreServlet
import org.scalatra._
import org.scalatra.servlet.{FileItem, FileUploadSupport, MultipartConfig, SizeConstraintExceededException}

import scala.collection.JavaConversions._
import scala.collection.concurrent

class BlobstoreServlet(val persistence: FilePersistence)
  extends PersistentDataStoreServlet
    with FileUploadSupport
    with ContentEncodingSupport {
  override def root: String = "blobstore"

  val maxFileSize: Int = SystemProperty(BlobstoreServlet.maxFileSizeSysProp, "50").get.toInt
  val maxRequestSize: Int = SystemProperty(BlobstoreServlet.maxRequestSizeSysProp, "100").get.toInt

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

  val blobStores: concurrent.Map[String, GeoMesaAccumuloBlobStore] = new ConcurrentHashMap[String, GeoMesaAccumuloBlobStore]
  getPersistedDataStores.foreach {
    case (alias, params) => connectToBlobStore(params).map(abs => blobStores.putIfAbsent(alias, abs))
  }

  private def connectToBlobStore(dsParams: Map[String, String]): Option[GeoMesaAccumuloBlobStore] = {
    val ds = new AccumuloDataStoreFactory().createDataStore(dsParams).asInstanceOf[AccumuloDataStore]
    if (ds == null) {
      logger.warn("Bad Connection Params: {}", dsParams)
      None
    } else {
      Some(GeoMesaAccumuloBlobStore(ds))
    }
  }

  /**
    * Override of the requestPath function, may need to match on type of request if wrapped
    * depending on deployment environment
    *
    * @param request
    * @return
    */
  override def requestPath(implicit request: HttpServletRequest): String = request match {
    case r: HttpServletRequestWrapper =>
      r.getMethod.toLowerCase(Locale.US) match {
        case "post" =>
          if (r.getServletPath == "/blob") {
            s"/blob${r.getPathInfo}"
          } else {
            r.getPathInfo
          }
        case "delete" =>
          r.getPathInfo.replace(s"/$root", "")
        case _        =>
          r.getPathInfo
      }
    case _ => super.requestPath
  }

  // TODO: Revisit configuration and persistence of configuration.
  // https://geomesa.atlassian.net/browse/GEOMESA-958
  /**
    * Registers a data store, making it available for later use
    */
  post("/ds/:alias/?") {
    logger.debug("Attempting to register accumulo connection in Blob Store")
    val dsParams = datastoreParams
    val ds = new AccumuloDataStoreFactory().createDataStore(dsParams).asInstanceOf[AccumuloDataStore]
    if (ds == null) {
      BadRequest(body = "Could not load data store using the provided parameters")
    } else {
      val alias = params("alias")
      val prefix = keyFor(alias)
      val toPersist = dsParams.map { case (k, v) => keyFor(alias, k) -> v }
      try {
        persistence.removeAll(persistence.keys(prefix).toSeq)
        persistence.persistAll(toPersist)
        blobStores.put(alias, GeoMesaAccumuloBlobStore(ds))
        Ok()
      } catch {
        case e: Exception => handleError(s"Error persisting data store '$alias':", e)
      }
    }
  }

  /**
    * Retrieve an existing data store
    */
  get("/ds/:alias/?") {
    try {
      getPersistedDataStore(params("alias"))
    } catch {
      case e: Exception => handleError(s"Error reading data store:", e)
    }
  }

  /**
    * Remove the reference to an existing data store and removes tables
    */
  delete("/ds/:alias/?") {
    val alias = params("alias")
    val prefix = keyFor(alias)
    try {
      persistence.removeAll(persistence.keys(prefix).toSeq)
      blobStores.remove(alias) match {
        case None      => BadRequest(body = s"Error removing data store '$alias'")
        case Some(abs) => Ok()
      }
    } catch {
      case e: Exception => handleError(s"Error removing data store '$alias':", e)
    }
  }

  /**
    * Retrieve all existing data stores
    */
  get("/ds/?") {
    try {
      getPersistedDataStores
    } catch {
      case e: Exception => handleError(s"Error reading data stores:", e)
    }
  }

  /**
    * Deletes for blobs given id
    */
  delete("/blob/:alias/:id/?") {
    val alias = params("alias")
    blobStores.get(alias) match {
      case None => BadRequest(body = "AccumuloBlobStore is not initialized")
      case Some(abs) =>
        val id = params("id")
        logger.debug("Attempting to deleteBlob: {} from store: {}", id, alias)
        try {
          abs.delete(id)
          Ok(body = s"deleted feature: $id")
        } catch {
          case e: Exception => handleError("Error deleting blob", e)
        }
    }
  }

  /**
    * Get for blobs given id
    */
  get("/blob/:alias/:id/?") {
    val alias = params("alias")
    blobStores.get(alias) match {
      case None => BadRequest(body = "AccumuloBlobStore is not initialized")
      case Some(abs) =>
        val id = params("id")
        logger.debug("Attempting to get blob for id: {} from store: {}", id, alias)
        try {
          val ret: Option[Blob] = Option(abs.get(id))
          ret match {
            case None =>
              BadRequest(body = s"Unknown ID $id")
            case Some(blob) =>
              contentType = "application/octet-stream"
              response.setHeader("Content-Disposition", "attachment;filename=" + blob.getLocalName)
              Ok(blob.getPayload)
          }
        } catch {
          case e: Exception => handleError("Error retrieving blob", e)
        }
    }
  }

  /**
    * Post for blob binaries
    */
  post("/blob/:alias/?") {
    val alias = params("alias")
    blobStores.get(alias) match {
      case None => BadRequest(body = "AccumuloBlobStore is not initialized in BlobStore")
      case Some(abs) =>
        logger.debug("Attempting to ingest file to BlobStore")
        try {
          fileParams.get("file") match {
            case None =>
              BadRequest(body = "no file parameter in request")
            case Some(file) =>
              val params = multiParams.map{case (k, v) => k -> v.toString}.updated(GeoMesaBlobStoreSFT.FilenameFieldName, file.getName)
              attemptBlobWriting(abs, file, params)
          }
        } catch {
          case e: Exception => handleError("Error uploading file", e)
        }
    }
  }

  private def attemptBlobWriting(abs: GeoMesaAccumuloBlobStore, file: FileItem, otherParams: Map[String, String]) = {
    val tempFile = File.createTempFile(UUID.randomUUID().toString, FilenameUtils.getExtension(file.getName))
    try {
      file.write(tempFile)
      val ret = abs.put(tempFile, otherParams)
      if (ret == null) {
        BadRequest(body = "Unable to ingest file to blobstore")
      } else {
        Created(body = ret, headers = Map("Location" -> request.getRequestURL.append(ret).toString))
      }
    } finally {
      tempFile.delete()
    }
  }

}

object BlobstoreServlet {
  val permissions  = PosixFilePermissions.asFileAttribute(Set(OWNER_READ, OWNER_WRITE, OWNER_EXECUTE, GROUP_READ, GROUP_WRITE))
  val maxFileSizeSysProp = "org.locationtech.geomesa.blob.api.maxFileSizeMB"
  val maxRequestSizeSysProp = "org.locationtech.geomesa.blob.api.maxRequestSizeMB"
}
