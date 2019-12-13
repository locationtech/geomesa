/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.geojson.servlet

import java.net.URLEncoder
import java.nio.file.Files

import org.json4s.{DefaultFormats, Formats}
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.cache.FilePersistence
import org.locationtech.geomesa.utils.io.PathUtils
import org.scalatra.test.specs2.MutableScalatraSpec
import org.specs2.runner.JUnitRunner
import org.specs2.specification.core.Fragments

@RunWith(classOf[JUnitRunner])
class GeoJsonServletTest extends MutableScalatraSpec {

  sequential

  val tmpDir = Files.createTempDirectory("geojsontest")

  val f0 = """{"type":"Feature","geometry":{"type":"Point","coordinates":[30,10]},"properties":{"id":"0","name":"n0"}}"""
  val f1 = """{"type":"Feature","geometry":{"type":"Point","coordinates":[32,10]},"properties":{"id":"1","name":"n1"}}"""
  val f2 = """{"type":"Feature","geometry":{"type":"Point","coordinates":[34,10]},"properties":{"id":"2","name":"n2"}}"""

  val f0Updated = f0.replace("n0", "n0-updated").replace("30,10", "20,10")
  val f1Updated = f1.replace("n1", "n1-updated").replace("32,10", "22,10")

  def urlEncode(s: String): String = URLEncoder.encode(s, "UTF-8")

  // cleanup tmp dir after tests run
  override def map(fragments: => Fragments): Fragments = super.map(fragments) ^ step {
    PathUtils.deleteRecursively(tmpDir)
  }

  addServlet(new GeoJsonServlet(new FilePersistence(tmpDir.toFile, "servlet")), "/*")

  implicit val formats: Formats = DefaultFormats

  "GeoJsonServlet" should {
    "register a datastore" in {
      import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreParams._
      val params = Map(InstanceIdParam.key -> "GeoJsonServletTest", UserParam.key -> "root", ZookeepersParam.key -> "myzoo",
        PasswordParam.key -> "", CatalogParam.key -> "GeoJsonServletTest", MockParam.key -> "true")
      post("/ds/geojsontest", params) {
        status mustEqual 200
      }
      post("/index/geojsontest/geojsontest", Map("id" -> "properties.id")) {
        status mustEqual 201 // created
      }
    }
    "return empty list from query" in {
      get("/index/geojsontest/geojsontest/features") {
        status mustEqual 200
        body mustEqual """{"type":"FeatureCollection","features":[]}"""
      }
    }
    "add geojson features" in {
      post("/index/geojsontest/geojsontest/features", f0.getBytes("UTF-8")) {
        status mustEqual 200
        body mustEqual """["0"]"""
      }
      get("/index/geojsontest/geojsontest/features") {
        status mustEqual 200
        body mustEqual s"""{"type":"FeatureCollection","features":[$f0]}"""
      }
    }
    "add geojson feature collections" in {
      post("/index/geojsontest/geojsontest/features",
          s"""{"type":"FeatureCollection","features":[$f1,$f2]}""".getBytes("UTF-8")) {
        status mustEqual 200
        body mustEqual """["1","2"]"""
      }
      get("/index/geojsontest/geojsontest/features") {
        status mustEqual 200
        body must startWith("""{"type":"FeatureCollection","features":[""")
        body must endWith("]}")
        body must haveLength(s"""{"type":"FeatureCollection","features":[$f0,$f1,$f2]}""".length)
        body must contain(f0)
        body must contain(f1)
        body must contain(f2)
      }
    }
    "query geojson features by id" in {
      get(s"/index/geojsontest/geojsontest/features?q=${urlEncode("""{"properties.id":"0"}""")}") {
        status mustEqual 200
        body mustEqual s"""{"type":"FeatureCollection","features":[$f0]}"""
      }
      get(s"/index/geojsontest/geojsontest/features?q=${urlEncode("""{"properties.id":"1"}""")}") {
        status mustEqual 200
        body mustEqual s"""{"type":"FeatureCollection","features":[$f1]}"""
      }
      get(s"/index/geojsontest/geojsontest/features/1") {
        status mustEqual 200
        body mustEqual s"""{"type":"FeatureCollection","features":[$f1]}"""
      }
      get(s"/index/geojsontest/geojsontest/features/1,2") {
        status mustEqual 200
        body must startWith("""{"type":"FeatureCollection","features":[""")
        body must endWith("]}")
        body must haveLength(s"""{"type":"FeatureCollection","features":[$f1,$f2]}""".length)
        body must contain(f1)
        body must contain(f2)
      }
    }
    "query geojson features by geometry" in {
      get(s"/index/geojsontest/geojsontest/features?q=${urlEncode("""{"geometry":{"$bbox":[33,9,35,11]}}""")}") {
        status mustEqual 200
        body mustEqual s"""{"type":"FeatureCollection","features":[$f2]}"""
      }
    }
    "query geojson features by properties" in {
      get(s"/index/geojsontest/geojsontest/features?q=${urlEncode("""{"properties.name":"n1"}""")}") {
        status mustEqual 200
        body mustEqual s"""{"type":"FeatureCollection","features":[$f1]}"""
      }
    }
    "update geojson features" in {
      put(s"/index/geojsontest/geojsontest/features", f0Updated.getBytes("UTF-8")) {
        status mustEqual 200
      }
      get(s"/index/geojsontest/geojsontest/features/0") {
        status mustEqual 200
        body mustEqual s"""{"type":"FeatureCollection","features":[$f0Updated]}"""
      }
      put(s"/index/geojsontest/geojsontest/features/1", f1Updated.getBytes("UTF-8")) {
        status mustEqual 200
      }
      get(s"/index/geojsontest/geojsontest/features/1") {
        status mustEqual 200
        body mustEqual s"""{"type":"FeatureCollection","features":[$f1Updated]}"""
      }
      get("/index/geojsontest/geojsontest/features") {
        status mustEqual 200
        body must startWith("""{"type":"FeatureCollection","features":[""")
        body must endWith("]}")
        body must haveLength(s"""{"type":"FeatureCollection","features":[$f0Updated,$f1Updated,$f2]}""".length)
        body must contain(f0Updated)
        body must contain(f1Updated)
        body must contain(f2)
      }
    }
    "delete geojson features" in {
      delete(s"/index/geojsontest/geojsontest/features/0") {
        status mustEqual 200
      }
      get(s"/index/geojsontest/geojsontest/features/0") {
        println(body)
        status mustEqual 404
      }
      get("/index/geojsontest/geojsontest/features") {
        status mustEqual 200
        body must startWith("""{"type":"FeatureCollection","features":[""")
        body must endWith("]}")
        body must haveLength(s"""{"type":"FeatureCollection","features":[$f1Updated,$f2]}""".length)
        body must contain(f1Updated)
        body must contain(f2)
      }
    }
  }
}
