/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.geojson.servlet

import java.io.Closeable
import java.util.concurrent.ConcurrentHashMap

import org.geotools.data.DataStore
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

  private val indexCache = new ConcurrentHashMap[DataStore, GeoJsonGtIndex]

  before() {
    contentType = formats("json")
  }

  /**
    * Create a new index (i.e. schema)
    */
  post("/index/:alias/:index/?") {
    try {
      withGeoJsonIndex((geoJsonIndex) => {
        val index = params.get("index").orNull
        if (index == null) {
          BadRequest(GeoJsonServlet.NoIndex)
        } else {
          val points = params.get("points").map(java.lang.Boolean.valueOf).getOrElse(java.lang.Boolean.FALSE)
          geoJsonIndex.createIndex(index, params.get("id"), params.get("date"), points)
          Created()
        }
      })
    } catch {
      case e: IllegalArgumentException => BadRequest(e.getMessage)
      case e: Exception => handleError(s"Error creating index:", e)
    }
  }

  /**
    * Delete an index and everything in it
    */
  delete("/index/:alias/:index/?") {
    try {
      withGeoJsonIndex((geoJsonIndex) => {
        val index = params.get("index").orNull
        if (index == null) {
          BadRequest(GeoJsonServlet.NoIndex)
        } else {
          geoJsonIndex.deleteIndex(index)
          NoContent() // 204
        }
      })
    } catch {
      case e: IllegalArgumentException => BadRequest(e.getMessage)
      case e: Exception => handleError(s"Error creating index:", e)
    }
  }

  /**
    * Add new features to an index
    */
  post("/index/:alias/:index/features/?") {
    try {
      withGeoJsonIndex((geoJsonIndex) => {
        val index = params.get("index").orNull
        val json = request.body
        if (index == null || json == null || json.isEmpty) {
          val msg = ArrayBuffer.empty[String]
          if (index == null) { msg.append(GeoJsonServlet.NoIndex) }
          if (json == null || json.isEmpty) { msg.append(GeoJsonServlet.NoJson) }
          BadRequest(msg.mkString(" "))
        } else {
          val ids = geoJsonIndex.add(index, json)
          Ok(ids)
        }
      })
    } catch {
      case e: IllegalArgumentException => BadRequest(e.getMessage)
      case e: Exception => handleError(s"Error adding features:", e)
    }
  }

  /**
    * Update existing features in an index
    */
  put("/index/:alias/:index/features/?") {
    try {
      withGeoJsonIndex((geoJsonIndex) => {
        val index = params.get("index").orNull
        val json = request.body
        if (index == null || json == null || json.isEmpty) {
          val msg = ArrayBuffer.empty[String]
          if (index == null) { msg.append(GeoJsonServlet.NoIndex) }
          if (json == null || json.isEmpty) { msg.append(GeoJsonServlet.NoJson) }
          BadRequest(msg.mkString(" "))
        } else {
          geoJsonIndex.update(index, json)
          Ok()
        }
      })
    } catch {
      case e: IllegalArgumentException => BadRequest(e.getMessage)
      case e: Exception => handleError(s"Error updating features:", e)
    }
  }

  /**
    * Update existing features in an index
    */
  put("/index/:alias/:index/features/:fid") {
    try {
      withGeoJsonIndex((geoJsonIndex) => {
        val index = params.get("index").orNull
        val json = request.body
        if (index == null || json == null || json.isEmpty) {
          val msg = ArrayBuffer.empty[String]
          if (index == null) { msg.append(GeoJsonServlet.NoIndex) }
          if (json == null || json.isEmpty) { msg.append(GeoJsonServlet.NoJson) }
          BadRequest(msg.mkString(" "))
        } else {
          val fids = params("fid").split(",") // can't match this path without a fid
          geoJsonIndex.update(index, fids, json)
          Ok()
        }
      })
    } catch {
      case e: IllegalArgumentException => BadRequest(e.getMessage)
      case e: Exception => handleError(s"Error updating features:", e)
    }
  }

  /**
    * Delete features from an index
    */
  delete("/index/:alias/:index/features/:fid") {
    try {
      withGeoJsonIndex((geoJsonIndex) => {
        val index = params.get("index").orNull
        if (index == null) {
          BadRequest(GeoJsonServlet.NoIndex)
        } else {
          val fids = params("fid").split(",") // can't match this path without a fid
          geoJsonIndex.delete(index, fids)
          Ok()
        }
      })
    } catch {
      case e: IllegalArgumentException => BadRequest(e.getMessage)
      case e: Exception => handleError(s"Error adding features:", e)
    }
  }

  /**
    * Query features in an index
    */
  get("/index/:alias/:index/features/?") {
    try {
      withGeoJsonIndex((geoJsonIndex) => {
        val index = params.get("index").orNull
        if (index == null) {
          BadRequest(GeoJsonServlet.NoIndex)
        } else {
          val query = params.get("q").getOrElse("")
          outputFeatures(geoJsonIndex.query(index, query))
          Unit // return Unit to indicate we've processed the response
        }
      })
    } catch {
      case e: IllegalArgumentException => BadRequest(e.getMessage)
      case e: Exception => handleError(s"Error querying features:", e)
    }
  }

  /**
    * Query a single feature in an index
    */
  get("/index/:alias/:index/features/:fid") {
    try {
      withGeoJsonIndex((geoJsonIndex) => {
        val index = params.get("index").orNull
        if (index == null) {
          BadRequest(GeoJsonServlet.NoIndex)
        } else {
          val fids = params("fid").split(",") // can't match this path without a fid
          val features = geoJsonIndex.get(index, fids)
          if (features.isEmpty) { NotFound() } else {
            outputFeatures(features)
            Unit // return Unit to indicate we've processed the response
          }
        }
      })
    } catch {
      case e: IllegalArgumentException => BadRequest(e.getMessage)
      case e: Exception => handleError(s"Error querying features:", e)
    }
  }

  private def withGeoJsonIndex[T](method: (GeoJsonGtIndex) => T): Any = {
    withDataStore((ds: DataStore) => {
      val index = Option(indexCache.get(ds)).getOrElse {
        val index = new GeoJsonGtIndex(ds)
        indexCache.put(ds, index)
        index
      }
      method(index)
    })
  }

  private def outputFeatures(features: Iterator[String] with Closeable): Unit = {
    try {
      response.setStatus(200)
      val output = response.getOutputStream
      output.print("""{"type":"FeatureCollection","features":[""")
      if (features.hasNext) {
        output.print(features.next)
        while (features.hasNext) {
          output.print(',')
          output.print(features.next)
        }
      }
      output.print("]}")
    } finally {
      features.close()
    }
  }
}

object GeoJsonServlet {
  val NoIndex = "Index name not specified."
  val NoJson  = "GEOJSON not specified."
}
