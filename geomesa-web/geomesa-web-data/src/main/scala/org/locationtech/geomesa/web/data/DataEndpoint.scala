/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.web.data

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.DataStoreFinder
import org.locationtech.geomesa.web.core.GeoMesaScalatraServlet
import org.scalatra.{BadRequest, InternalServerError, Ok}

import scala.collection.JavaConversions._

@deprecated
class DataEndpoint extends GeoMesaScalatraServlet with LazyLogging {

  override val root: String = "data"

  delete("/:catalog/:feature") {
    delete()
  }

  post("/:catalog/:feature/delete") {
    delete()
  }

  def delete(): Unit = {
    val fn = params("feature")
    try {
      val ds = DataStoreFinder.getDataStore(datastoreParams)
      if (ds == null) {
        BadRequest()
      } else {
        ds.removeSchema(fn)
        ds.dispose()
        Ok()
      }
    } catch {
      case e: Exception =>
        logger.error(s"Error deleting feature $fn", e)
        InternalServerError()
    }
  }
}
