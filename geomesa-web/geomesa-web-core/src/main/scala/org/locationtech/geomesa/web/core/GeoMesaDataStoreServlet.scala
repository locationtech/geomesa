/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.web.core

import org.geotools.data.DataStoreFinder
import org.scalatra.{BadRequest, Ok}

import scala.collection.JavaConversions._

trait GeoMesaDataStoreServlet extends GeoMesaBaseDataStoreServlet {

  /**
   * Registers a data store, making it available for later use
   */
  post("/ds/:alias") {
    val dsParams = datastoreParams
    val ds = DataStoreFinder.getDataStore(dsParams)
    if (ds == null) {
      BadRequest(reason = "Could not load data store using the provided parameters.")
    } else {
      ds.dispose()
      val alias = params("alias")
      val prefix = keyFor(alias)
      val toPersist = dsParams.map { case (k, v) => keyFor(alias, k) -> v }
      try {
        persistence.removeAll(persistence.keys(prefix).toSeq)
        persistence.persistAll(toPersist)
        Ok()
      } catch {
        case e: Exception => handleError(s"Error persisting data store '$alias':", e)
      }
    }
  }

  /**
   * Retrieve an existing data store
   */
  get("/ds/:alias") {
    try {
      getPersistedDataStore(params("alias"))
    } catch {
      case e: Exception => handleError(s"Error reading data store:", e)
    }
  }

  /**
   * Remove the reference to an existing data store
   */
  delete("/ds/:alias") {
    val alias = params("alias")
    val prefix = keyFor(alias)
    try {
      persistence.removeAll(persistence.keys(prefix).toSeq)
      Ok()
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

}

