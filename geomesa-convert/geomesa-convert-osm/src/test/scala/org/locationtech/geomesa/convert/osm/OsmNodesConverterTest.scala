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
class OsmNodesConverterTest extends Specification {

  sequential

  val sftSimpleConf = ConfigFactory.parseString(
    """{ type-name = "osmSimpleNodeType"
      |  attributes = [
      |    {name = "geom", type = "Point", default = "true" }
      |  ]
      |}
    """.stripMargin)

  val sftConf = ConfigFactory.parseString(
    """{ type-name = "osmNodeType"
      |  attributes = [
      |    { name = "user", type = "String" }
      |    { name = "tags", type = "Map[String,String]" }
      |    { name = "dtg",  type = "Date" }
      |    { name = "geom", type = "Point", default = "true" }
      |  ]
      |}
    """.stripMargin)

  val simpleSft = SimpleFeatureTypes.createType(sftSimpleConf)
  val sft = SimpleFeatureTypes.createType(sftConf)

  "OSM Node Converter" should {

    "parse simple attributes" >> {
      val parserConf = ConfigFactory.parseString(
        """
          | {
          |   type         = "osm-nodes"
          |   format       = "xml" // or pbf
          |   id-field     = "$id"
          |   fields = [
          |     { name = "id", attribute = "id", transform = "toString($0)" }
          |     { name = "geom", attribute = "geometry" }
          |   ]
          | }
        """.stripMargin)

      val converter = SimpleFeatureConverters.build[Any](simpleSft, parserConf)
      converter must beAnInstanceOf[OsmNodesConverter]
      converter.asInstanceOf[OsmNodesConverter].needsMetadata must beFalse

      val features = converter.process(getClass.getClassLoader.getResourceAsStream("small.osm")).toList.sortBy(_.getID)
      features must haveLength(4)
      features.map(_.getID) mustEqual Seq("350151", "350152", "350153", "350154")
      features.map(_.getDefaultGeometry.toString) mustEqual Seq("POINT (-6.3341538 54.1790829)",
        "POINT (-6.3339244 54.179083)", "POINT (-6.3316723 54.179379)", "POINT (-6.3314593 54.1794726)")
    }

    "parse metadata" >> {
      val parserConf = ConfigFactory.parseString(
        """
          | {
          |   type         = "osm-nodes"
          |   format       = "xml" // or pbf
          |   id-field     = "$id"
          |   fields = [
          |     { name = "id", attribute = "id", transform = "toString($0)" }
          |     { name = "user", attribute = "user" }
          |     { name = "tags", attribute = "tags" }
          |     { name = "dtg",  attribute = "timestamp" }
          |     { name = "geom", attribute = "geometry" }
          |   ]
          | }
        """.stripMargin)

      val converter = SimpleFeatureConverters.build[Any](sft, parserConf)
      converter must beAnInstanceOf[OsmNodesConverter]
      converter.asInstanceOf[OsmNodesConverter].needsMetadata must beTrue

      val features = converter.process(getClass.getClassLoader.getResourceAsStream("small.osm")).toList.sortBy(_.getID)
      features must haveLength(4)
      features.map(_.getID) mustEqual Seq("350151", "350152", "350153", "350154")
      forall(features.map(_.getAttribute("user")))(_ mustEqual "mackerski")
      forall(features.map(_.getAttribute("tags")))(_ mustEqual new java.util.HashMap[String, String]())
      features.map(_.getAttribute("dtg").asInstanceOf[Date]) mustEqual
          Seq("2015-10-28T21:17:49Z", "2015-10-28T21:17:49Z", "2015-10-28T21:17:49Z", "2015-10-28T21:17:49Z")
              .map(Converters.convert(_, classOf[Date]))
      features.map(_.getDefaultGeometry.toString) mustEqual Seq("POINT (-6.3341538 54.1790829)",
        "POINT (-6.3339244 54.179083)", "POINT (-6.3316723 54.179379)", "POINT (-6.3314593 54.1794726)")
    }

    "handle user data" >> {
      val parserConf = ConfigFactory.parseString(
        """
          | {
          |   type         = "osm-nodes"
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
      converter must beAnInstanceOf[OsmNodesConverter]
      converter.asInstanceOf[OsmNodesConverter].needsMetadata must beFalse

      val features = converter.process(getClass.getClassLoader.getResourceAsStream("small.osm")).toList.sortBy(_.getID)
      features must haveLength(4)
      features.map(_.getID) mustEqual Seq("350151", "350152", "350153", "350154")
      features.map(_.getDefaultGeometry.toString) mustEqual Seq("POINT (-6.3341538 54.1790829)",
        "POINT (-6.3339244 54.179083)", "POINT (-6.3316723 54.179379)", "POINT (-6.3314593 54.1794726)")
      features.map(_.getUserData.get("my.user.key")) mustEqual Seq("350151", "350152", "350153", "350154")
    }
  }
}


