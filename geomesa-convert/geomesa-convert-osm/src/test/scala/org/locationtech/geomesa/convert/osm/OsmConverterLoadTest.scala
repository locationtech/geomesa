/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.osm

import com.typesafe.config.ConfigFactory
import org.junit.runner.RunWith
import org.locationtech.geomesa.convert.SimpleFeatureConverters
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class OsmConverterLoadTest extends Specification {

  sequential

  // note: this file isn't checked into git because it's pretty large
  def getFile = getClass.getClassLoader.getResourceAsStream("northern-ireland.osm")

  "OSM Converter" should {

    "parse large nodes files" >> {
      skipped("integration test")

      val sftConf = ConfigFactory.parseString(
        """{ type-name = "osmNodeType"
          |  attributes = [
          |    { name = "user", type = "String" }
          |    { name = "tags", type = "Map[String,String]" }
          |    { name = "dtg",  type = "Date", default = "true" }
          |    { name = "geom", type = "Point", default = "true" }
          |  ]
          |}
        """.stripMargin)

      val parserConf = ConfigFactory.parseString(
        """
          | {
          |   type         = "osm-nodes"
          |   id-field     = "$id"
          |   fields = [
          |     { name = "id",   attribute = "id", transform = "toString($0)" }
          |     { name = "user", attribute = "user" }
          |     { name = "tags", attribute = "tags" }
          |     { name = "dtg",  attribute = "timestamp" }
          |     { name = "geom", attribute = "geometry" }
          |   ]
          | }
        """.stripMargin)

      val nodesSft = SimpleFeatureTypes.createType(sftConf)
      val converter = SimpleFeatureConverters.build[Any](nodesSft, parserConf)
      val start = System.currentTimeMillis()
      val features = converter.process(getFile)
      val count = features.length
      println(s"parsed $count node features in ${System.currentTimeMillis() - start}ms")
      // parsed 3278174 node features in 15817ms
      ok
    }

    "parse large ways files" >> {
      skipped("integration test")

      val sftConf = ConfigFactory.parseString(
        """{ type-name = "osmWayType"
          |  attributes = [
          |    { name = "user", type = "String" }
          |    { name = "tags", type = "Map[String,String]" }
          |    { name = "dtg",  type = "Date", default = "true" }
          |    { name = "geom", type = "LineString", default = "true" }
          |  ]
          |}
        """.stripMargin)

      val parserConf = ConfigFactory.parseString(
        """
          | {
          |   type         = "osm-ways"
          |   id-field     = "$id"
          |   fields = [
          |     { name = "id", attribute = "id", transform = "toString($0)" }
          |     { name = "user", attribute = "user" }
          |     { name = "tags", attribute = "tags" }
          |     { name = "name", transform = "mapValue($tags, 'name')" }
          | //  { name = "name", attribute = "tags" transform = "mapValue($0, 'name')" }
          |     { name = "dtg",  attribute = "timestamp" }
          |     { name = "geom", attribute = "geometry" }
          |   ]
          | }
        """.stripMargin)

      val waysSft = SimpleFeatureTypes.createType(sftConf)
      val converter = SimpleFeatureConverters.build[Any](waysSft, parserConf)
      try {
        val start = System.currentTimeMillis()
        val features = converter.process(getFile)
        val count = features.length
        println(s"parsed $count way features in ${System.currentTimeMillis() - start}ms")
        // parsed 238717 way features in 39180ms
      } finally {
        converter.close()
      }

      ok
    }
  }
}


