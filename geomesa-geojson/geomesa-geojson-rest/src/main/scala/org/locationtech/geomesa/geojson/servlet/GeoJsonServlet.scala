/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.geojson.servlet

import org.json4s.{DefaultFormats, Formats}
import org.locationtech.geomesa.geojson.GeoJsonGtIndex
import org.locationtech.geomesa.utils.cache.FilePersistence
import org.locationtech.geomesa.web.core.GeoMesaDataStoreServlet
import org.scalatra._
import org.scalatra.json.NativeJsonSupport

import scala.collection.mutable.ArrayBuffer

class GeoJsonServlet(val persistence: FilePersistence) extends GeoMesaDataStoreServlet with NativeJsonSupport {

  // TODO GEOMESA-1452 ability to update features
  // TODO GEOMESA-1452 ability to transform query responses

  override val root: String = "geojson"

  override protected implicit val jsonFormats: Formats = DefaultFormats

  before() {
    contentType = formats("json")
  }

  post("/index/:alias/:index/?") {
    try {
      withDataStore((ds) => {
        val index = params.get("index").orNull
        if (index == null) {
          BadRequest(GeoJsonServlet.NoIndex)
        } else {
          val points = params.get("points").map(java.lang.Boolean.valueOf).getOrElse(java.lang.Boolean.FALSE)
          new GeoJsonGtIndex(ds).createIndex(index, params.get("id"), params.get("date"), points)
          Created()
        }
      })
    } catch {
      case e: Exception => handleError(s"Error creating index:", e)
    }
  }

  delete("/index/:alias/:index/?") {
    try {
      withDataStore((ds) => {
        val index = params.get("index").orNull
        if (index == null) {
          BadRequest(GeoJsonServlet.NoIndex)
        } else {
          new GeoJsonGtIndex(ds).deleteIndex(index)
          NoContent() // 204
        }
      })
    } catch {
      case e: Exception => handleError(s"Error creating index:", e)
    }
  }

  post("/index/:alias/:index/features") {
    try {
      withDataStore((ds) => {
        val index = params.get("index").orNull
        val json = params.get("json").orNull
        if (index == null || json == null) {
          val msg = ArrayBuffer.empty[String]
          if (index == null) { msg.append(GeoJsonServlet.NoIndex) }
          if (json == null) { msg.append(GeoJsonServlet.NoJson) }
          BadRequest(msg.mkString(" "))
        } else {
          val ids = new GeoJsonGtIndex(ds).add(index, json)
          Ok(ids)
        }
      })
    } catch {
      case e: Exception => handleError(s"Error adding features:", e)
    }
  }

  get("/index/:alias/:index/features") {
    try {
      withDataStore((ds) => {
        val index = params.get("index").orNull
        if (index == null) {
          BadRequest(GeoJsonServlet.NoIndex)
        } else {
          val query = params.get("q").getOrElse("")
          val result = new GeoJsonGtIndex(ds).query(index, query)
          try {
            response.setStatus(200)
            val output = response.getOutputStream
            output.print("""{"type":"FeatureCollection","features":[""")
            if (result.hasNext) {
              output.print(result.next)
            }
            while (result.hasNext) {
              output.print(',')
              output.print(result.next)
            }
            output.print("]}")
          } finally {
            result.close()
          }
          Unit // return Unit to indicate we've processed the response
        }
      })
    } catch {
      case e: Exception => handleError(s"Error querying features:", e)
    }
  }
}

object GeoJsonServlet {
  val NoIndex = "Index name not specified."
  val NoJson  = "GEOJSON not specified."
}
