/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.osm

import java.util.Date

import com.typesafe.config.ConfigFactory
import org.geotools.util.Converters
import org.junit.runner.RunWith
import org.locationtech.geomesa.convert.SimpleFeatureConverters
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class OsmWaysConverterTest extends Specification {

  sequential

  val sftSimpleConf = ConfigFactory.parseString(
    """{ type-name = "osmSimpleWayType"
      |  attributes = [
      |    {name = "geom", type = "LineString", default = "true" }
      |  ]
      |}
    """.stripMargin)

  val sftConf = ConfigFactory.parseString(
    """{ type-name = "osmWayType"
      |  attributes = [
      |    { name = "user", type = "String" }
      |    { name = "tags", type = "Map[String,String]" }
      |    { name = "name", type = "String" }
      |    { name = "dtg",  type = "Date" }
      |    { name = "geom", type = "LineString", default = "true" }
      |  ]
      |}
    """.stripMargin)

  val simpleSft = SimpleFeatureTypes.createType(sftSimpleConf)
  val sft = SimpleFeatureTypes.createType(sftConf)

  "OSM Way Converter" should {

    "parse simple attributes" >> {
      val parserConf = ConfigFactory.parseString(
        """
          | {
          |   type         = "osm-ways"
          |   format       = "xml" // or pbf
          |   id-field     = "$id"
          |   fields = [
          |     { name = "id", attribute = "id", transform = "toString($0)" }
          |     { name = "geom", attribute = "geometry" }
          |   ]
          | }
        """.stripMargin)

      val converter = SimpleFeatureConverters.build[Any](simpleSft, parserConf)
      try {
        converter must beAnInstanceOf[OsmWaysConverter]
        converter.asInstanceOf[OsmWaysConverter].needsMetadata must beFalse

        val features = converter.process(getClass.getClassLoader.getResourceAsStream("small.osm")).toList.sortBy(_.getID)
        features must haveLength(1)
        features.head.getID mustEqual "1789239"
        features.head.getDefaultGeometry.toString mustEqual
            "LINESTRING (-6.3341538 54.1790829, -6.3339244 54.179083, -6.3316723 54.179379, -6.3314593 54.1794726)"
      } finally {
        converter.close()
      }
    }

    "parse metadata" >> {
      val parserConf = ConfigFactory.parseString(
        """
          | {
          |   type         = "osm-ways"
          |   format       = "xml" // or pbf
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

      val converter = SimpleFeatureConverters.build[Any](sft, parserConf)
      try {
        converter must beAnInstanceOf[OsmWaysConverter]
        converter.asInstanceOf[OsmWaysConverter].needsMetadata must beTrue

        val features = converter.process(getClass.getClassLoader.getResourceAsStream("small.osm")).toList.sortBy(_.getID)
        features must haveLength(1)
        features.head.getID mustEqual "1789239"
        features.head.getAttribute("user") mustEqual "mackerski"
        import scala.collection.JavaConverters._
        features.head.getAttribute("tags") mustEqual Map("name" -> "Church Avenue", "highway" -> "residential").asJava
        features.head.getAttribute("name") mustEqual "Church Avenue"
        features.head.getAttribute("dtg").asInstanceOf[Date] mustEqual Converters.convert("2010-10-28T18:06:37Z", classOf[Date])
        features.head.getDefaultGeometry.toString mustEqual
            "LINESTRING (-6.3341538 54.1790829, -6.3339244 54.179083, -6.3316723 54.179379, -6.3314593 54.1794726)"
      } finally {
        converter.close()
      }
    }

    "handle user data" >> {
      val parserConf = ConfigFactory.parseString(
        """
          | {
          |   type         = "osm-ways"
          |   format       = "xml" // or pbf
          |   id-field     = "$id"
          |   user-data    = {
          |     my.user.key  = "$id"
          |   }
          |   fields = [
          |     { name = "id", attribute = "id", transform = "toString($0)" }
          |     { name = "geom", attribute = "geometry" }
          |   ]
          | }
        """.stripMargin)

      val converter = SimpleFeatureConverters.build[Any](simpleSft, parserConf)
      try {
        val features = converter.process(getClass.getClassLoader.getResourceAsStream("small.osm")).toList.sortBy(_.getID)
        features must haveLength(1)
        features.head.getID mustEqual "1789239"
        features.head.getDefaultGeometry.toString mustEqual
            "LINESTRING (-6.3341538 54.1790829, -6.3339244 54.179083, -6.3316723 54.179379, -6.3314593 54.1794726)"
      } finally {
        converter.close()
      }
    }
  }
}


