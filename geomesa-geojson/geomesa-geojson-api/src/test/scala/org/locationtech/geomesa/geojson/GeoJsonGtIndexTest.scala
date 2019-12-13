/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.geojson

import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.geotools.data.DataStoreFinder
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreParams
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GeoJsonGtIndexTest extends Specification {

  import scala.collection.JavaConversions._

  val connector = new MockInstance().getConnector("root", new PasswordToken(""))

  val ds = DataStoreFinder.getDataStore(Map(
    AccumuloDataStoreParams.ConnectorParam.getName -> connector,
    AccumuloDataStoreParams.CatalogParam.getName   -> "GeoJsonAccumuloIndexTest",
    AccumuloDataStoreParams.MockParam.getName      -> "true"
  ))

  val f0 = """{"type":"Feature","geometry":{"type":"Point","coordinates":[30,10]},"properties":{"id":"0","name":"n0"}}"""
  val f1 = """{"type":"Feature","geometry":{"type":"Point","coordinates":[31,10]},"properties":{"id":"1","name":"n1"}}"""
  val f2 = """{"type":"Feature","geometry":{"type":"Point","coordinates":[32,10]},"properties":{"id":"2","name":"n2"}}"""

  "GeoJsonAccumuloIndex" should {
    "read and write geojson" in {
      val name = "test0"
      val index = new GeoJsonGtIndex(ds)
      index.createIndex(name, Some("$.properties.id"), points = true)

      val features = s"""{ "type": "FeatureCollection", "features": [ $f0, $f1, $f2 ]}"""
      index.add(name, features)

      val result = index.query(name, "").toList
      result must haveLength(3)
      result must contain(f0, f1, f2)

      val nameResult = index.query(name, """{ "properties.name" : "n1" }""").toList
      nameResult must haveLength(1)
      nameResult must contain(f1)

      val bboxResult = index.query(name, """{"geometry":{"$bbox":[30.5,9,32.5,11]}}""").toList
      bboxResult must haveLength(2)
      bboxResult must contain(f1, f2)

      val idResult0 = index.query(name, """{ "properties.id" : "0" }""").toList
      idResult0 must haveLength(1)
      idResult0 must contain(f0)

      val idResult1 = index.get(name, Seq("1", "2")).toList
      idResult1 must haveLength(2)
      idResult1 must contain(f1, f2)

      index.delete(name, Seq("1", "2"))

      val deleteResult = index.query(name, "").toList
      deleteResult must haveLength(1)
      deleteResult must contain(f0)
    }

    "transform geojson at query time" in {
      val name = "test1"
      val index = new GeoJsonGtIndex(ds)
      index.createIndex(name, Some("$.properties.id"), points = true)

      val features = s"""{ "type": "FeatureCollection", "features": [ $f0, $f1, $f2 ]}"""
      index.add(name, features)

      val result0 = index.query(name, "", Map("foo" -> "geometry")).toList
      result0 must haveLength(3)
      result0 must contain("""{"foo":{"type":"Point","coordinates":[30,10]}}""")
      result0 must contain("""{"foo":{"type":"Point","coordinates":[31,10]}}""")
      result0 must contain("""{"foo":{"type":"Point","coordinates":[32,10]}}""")

      val result1 = index.query(name, "", Map("foo.bar" -> "geometry")).toList
      result1 must haveLength(3)
      result1 must contain("""{"foo":{"bar":{"type":"Point","coordinates":[30,10]}}}""")
      result1 must contain("""{"foo":{"bar":{"type":"Point","coordinates":[31,10]}}}""")
      result1 must contain("""{"foo":{"bar":{"type":"Point","coordinates":[32,10]}}}""")

      val result2 = index.query(name, "", Map("foo.bar" -> "geometry", "foo.baz" -> "properties.name")).toList
      result2 must haveLength(3)
      result2 must contain("""{"foo":{"bar":{"type":"Point","coordinates":[30,10]},"baz":"n0"}}""")
      result2 must contain("""{"foo":{"bar":{"type":"Point","coordinates":[31,10]},"baz":"n1"}}""")
      result2 must contain("""{"foo":{"bar":{"type":"Point","coordinates":[32,10]},"baz":"n2"}}""")
    }
  }
}
