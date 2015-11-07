package org.locationtech.geomesa.blob.api

import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloDataStoreFactory}
import org.locationtech.geomesa.blob.core.AccumuloBlobStore
import org.locationtech.geomesa.web.core.GeoMesaScalatraServlet
import org.scalatra.{BadRequest, Ok}

import scala.collection.JavaConversions._

class BlobstoreServlet extends GeoMesaScalatraServlet {
  override def root: String = "blob"

  var abs: AccumuloBlobStore = null

  post("ds/:alias") {
    val dsParams = datastoreParams
    val ds = new AccumuloDataStoreFactory().createDataStore(dsParams).asInstanceOf[AccumuloDataStore]

    if (ds == null) {
      BadRequest(reason = "Could not load data store using the provided parameters.")
    } else {
      abs = new AccumuloBlobStore(ds)
      Ok()
    }
  }

  get("/:id") {
    if (abs == null) {
      BadRequest(reason = "AccumuloBlobStore is not initialized.")
    } else {
      abs.get("id")._1
    }
  }
}
