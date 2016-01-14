/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.blob.api

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloDataStoreFactory}
import org.locationtech.geomesa.blob.core.AccumuloBlobStore
import org.locationtech.geomesa.web.core.GeoMesaScalatraServlet
import org.scalatra.{NotFound, Ok}

import scala.collection.JavaConversions._

class BlobstoreServlet extends GeoMesaScalatraServlet with LazyLogging {
  override def root: String = "blob"

  var abs: AccumuloBlobStore = null

  // TODO: Revisit configuration and persistence of configuration.
  // https://geomesa.atlassian.net/browse/GEOMESA-958
  // https://geomesa.atlassian.net/browse/GEOMESA-984
  post("/ds") {

    logger.debug("In ds registration method")

    val dsParams = datastoreParams
    val ds = new AccumuloDataStoreFactory().createDataStore(dsParams).asInstanceOf[AccumuloDataStore]

    if (ds == null) {
      NotFound(reason = "Could not load data store using the provided parameters.")
    } else {
      // TODO: Synchronize Blobstore creation
      // https://geomesa.atlassian.net/browse/GEOMESA-985
      abs = new AccumuloBlobStore(ds)
      Ok()
    }
  }

  get("/:id") {
    val id = params("id")
    logger.debug(s"In ID method, trying to retrieve id $id")

    if (abs == null) {
      NotFound(reason = "AccumuloBlobStore is not initialized.")
    } else {
      val (returnBytes, filename) = abs.get(id)
      if (returnBytes == null) {
        NotFound(reason = s"Unknown ID $id")
      } else {
        contentType = "application/octet-stream"
        response.setHeader("Content-Disposition", "attachment; filename=" + filename)

        Ok(returnBytes)
      }
    }
  }
}
