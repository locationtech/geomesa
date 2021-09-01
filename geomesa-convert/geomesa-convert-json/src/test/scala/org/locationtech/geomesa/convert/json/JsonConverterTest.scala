/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.json

import com.typesafe.config.{Config, ConfigFactory}
import org.geotools.api.feature.simple.SimpleFeatureType
import org.junit.runner.RunWith
import org.locationtech.geomesa.convert2.SimpleFeatureConverter
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.text.WKTUtils
import org.locationtech.jts.geom._
import org.specs2.matcher.MatchResult
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.io.{ByteArrayInputStream, InputStream}
import java.nio.charset.StandardCharsets
import java.util.{Date, UUID}
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

@RunWith(classOf[JUnitRunner])
class JsonConverterTest extends Specification {

  import scala.collection.JavaConverters._

  sequential

  val sftConfPoint = ConfigFactory.parseString(
    """{ type-name = "jsonFeatureType"
      |  attributes = [
      |    { name = "number", type = "Integer" }
      |    { name = "color",  type = "String"  }
      |    { name = "weight", type = "Double"  }
      |    { name = "geom",   type = "Point"   }
      |  ]
      |}
    """.stripMargin)

  val sftConfMixed = ConfigFactory.parseString(
    """{ type-name = "jsonFeatureType"
      |  attributes = [
      |    { name = "number", type = "Integer" }
      |    { name = "color",  type = "String"  }
      |    { name = "weight", type = "Double"  }
      |    { name = "geom",   type = "Geometry"   }
      |  ]
      |}
    """.stripMargin)

  val sftTypeLineString = ConfigFactory.parseString(
    """{ type-name = "jsonFeatureType"
      |  attributes = [
      |    { name = "number", type = "Integer" }
      |    { name = "color",  type = "String"  }
      |    { name = "weight", type = "Double"  }
      |    { name = "geom",   type = "LineString"   }
      |  ]
      |}
    """.stripMargin)

  val sft = SimpleFeatureTypes.createType(sftConfPoint)

  "Json Converter" should {

    "parse multiple features out of a single document" >> {
      val jsonStr =
        """ {
          |    DataSource: { name: "myjson" },
          |    Features: [
          |      {
          |        id: 1,
          |        number: 123,
          |        color: "red",
          |        physical: {
          |          weight: 127.5,
          |          height: "5'11"
          |        },
          |        lat: 0,
          |        lon: 0
          |      },
          |      {
          |        id: 2,
          |        number: 456,
          |        color: "blue",
          |        physical: {
          |          weight: 150,
          |          height: "5'11"
          |        },
          |        lat: 1,
          |        lon: 1
          |      }
          |    ]
          | }
        """.stripMargin

      val parserConf = ConfigFactory.parseString(
        """
          | {
          |   type         = "json"
          |   id-field     = "$id"
          |   feature-path = "$.Features[*]"
          |   fields = [
          |     { name = "id",     json-type = "integer", path = "$.id",               transform = "toString($0)"      }
          |     { name = "number", json-type = "integer", path = "$.number",                                           }
          |     { name = "color",  json-type = "string",  path = "$.color",            transform = "trim($0)"          }
          |     { name = "weight", json-type = "double",  path = "$.physical.weight",                                  }
          |     { name = "lat",    json-type = "double",  path = "$.lat",                                              }
          |     { name = "lon",    json-type = "double",  path = "$.lon",                                              }
          |     { name = "geom",                                                       transform = "point($lon, $lat)" }
          |   ]
          | }
        """.stripMargin)

      val pt1 = new Point(new Coordinate(0, 0), new PrecisionModel(PrecisionModel.FIXED), 4326)
      val pt2 = new Point(new Coordinate(1, 1), new PrecisionModel(PrecisionModel.FIXED), 4326)

      WithClose(SimpleFeatureConverter(sft, parserConf)) { converter =>
        val features = WithClose(converter.process(new ByteArrayInputStream(jsonStr.getBytes(StandardCharsets.UTF_8))))(_.toList)
        features must haveLength(2)
        features(0).getAttribute("number").asInstanceOf[Integer] mustEqual 123
        features(0).getAttribute("color").asInstanceOf[String] mustEqual "red"
        features(0).getAttribute("weight").asInstanceOf[Double] mustEqual 127.5
        features(0).getDefaultGeometry must be equalTo pt1
        features(1).getAttribute("number").asInstanceOf[Integer] mustEqual 456
        features(1).getAttribute("color").asInstanceOf[String] mustEqual "blue"
        features(1).getAttribute("weight").asInstanceOf[Double] mustEqual 150
        features(1).getDefaultGeometry must be equalTo pt2
      }
    }

    // NB: To enable this feature, we are reading the JSON path relative from the global context and the 'feature path'.
    //  The data on the feature path has priority.
    "parse multiple features out of a single document using arrays" >> {
      val jsonStr =
        """ {
          |    DataSource: { name: "myjson" },
          |    lat: 5,
          |    lon: 4,
          |    Features: [
          |      {
          |        id: 1,
          |        number: 123,
          |        color: "red",
          |        physical: {
          |          weight: 127.5,
          |          height: "5'11"
          |        }
          |      },
          |      {
          |        id: 2,
          |        number: 456,
          |        color: "blue",
          |        physical: {
          |          weight: 150,
          |          height: "5'11"
          |        }
          |      }
          |    ]
          | }
        """.stripMargin

      val parserConf = ConfigFactory.parseString(
        """
          | {
          |   type         = "json"
          |   id-field     = "$id"
          |   feature-path = "$.Features[*]"
          |   fields = [
          |     { name = "id",     json-type = "integer", path = "$.id",               transform = "toString($0)"      }
          |     { name = "number", json-type = "integer", path = "$.number",                                           }
          |     { name = "color",  json-type = "string",  path = "$.color",            transform = "trim($0)"          }
          |     { name = "weight", json-type = "double",  path = "$.physical.weight",                                  }
          |     { name = "lat",    json-type = "double",  root-path = "$.lat",                                         }
          |     { name = "lon",    json-type = "double",  root-path = "$.lon",                                         }
          |     { name = "geom",                                                       transform = "point($lon, $lat)" }
          |   ]
          | }
        """.stripMargin)

      val pt1 = new Point(new Coordinate(4, 5), new PrecisionModel(PrecisionModel.FIXED), 4326)
      val pt2 = new Point(new Coordinate(4, 5), new PrecisionModel(PrecisionModel.FIXED), 4326)

      WithClose(SimpleFeatureConverter(sft, parserConf)) { converter =>
        val features = WithClose(converter.process(new ByteArrayInputStream(jsonStr.getBytes(StandardCharsets.UTF_8))))(_.toList)
        features must haveLength(2)
        features(0).getAttribute("number").asInstanceOf[Integer] mustEqual 123
        features(0).getAttribute("color").asInstanceOf[String] mustEqual "red"
        features(0).getAttribute("weight").asInstanceOf[Double] mustEqual 127.5
        features(0).getDefaultGeometry must be equalTo pt1
        features(1).getAttribute("number").asInstanceOf[Integer] mustEqual 456
        features(1).getAttribute("color").asInstanceOf[String] mustEqual "blue"
        features(1).getAttribute("weight").asInstanceOf[Double] mustEqual 150
        features(1).getDefaultGeometry must be equalTo pt2
      }
    }

    "parse in single line-mode" >> {

      val jsonStr1 =
        """ {
          |    DataSource: { name: "myjson" },
          |    Features: [
          |      {
          |        id: 1,
          |        number: 123,
          |        color: "red",
          |        physical: {
          |          weight: 127.5,
          |          height: "5'11"
          |        },
          |        lat: 0,
          |        lon: 0
          |      }
          |    ]
          | }
        """.stripMargin

      val jsonStr2 =
        """ {
          |    DataSource: { name: "myjson" },
          |    Features: [
          |      {
          |        id: 2,
          |        number: 456,
          |        color: "blue",
          |        physical: {
          |          weight: 150,
          |          height: "5'11"
          |        },
          |        lat: 1,
          |        lon: 1
          |      }
          |    ]
          | }
        """.stripMargin

      val jsonStr = jsonStr1.replaceAllLiterally("\n", " ") + "\n" + jsonStr2.replaceAllLiterally("\n", " ")

      val parserConf = ConfigFactory.parseString(
        """
          | {
          |   type         = "json"
          |   id-field     = "$id"
          |   feature-path = "$.Features[*]"
          |   options {
          |     line-mode = "single"
          |   }
          |   fields = [
          |     { name = "id",     json-type = "integer", path = "$.id",               transform = "toString($0)"      }
          |     { name = "number", json-type = "integer", path = "$.number",                                           }
          |     { name = "color",  json-type = "string",  path = "$.color",            transform = "trim($0)"          }
          |     { name = "weight", json-type = "double",  path = "$.physical.weight",                                  }
          |     { name = "lat",    json-type = "double",  path = "$.lat",                                              }
          |     { name = "lon",    json-type = "double",  path = "$.lon",                                              }
          |     { name = "geom",                                                       transform = "point($lon, $lat)" }
          |   ]
          | }
        """.stripMargin)

      val pt1 = new Point(new Coordinate(0, 0), new PrecisionModel(PrecisionModel.FIXED), 4326)
      val pt2 = new Point(new Coordinate(1, 1), new PrecisionModel(PrecisionModel.FIXED), 4326)

      WithClose(SimpleFeatureConverter(sft, parserConf)) { converter =>
        val features = WithClose(converter.process(new ByteArrayInputStream(jsonStr.getBytes)))(_.toList)
        features must haveLength(2)
        features(0).getAttribute("number").asInstanceOf[Integer] mustEqual 123
        features(0).getAttribute("color").asInstanceOf[String] mustEqual "red"
        features(0).getAttribute("weight").asInstanceOf[Double] mustEqual 127.5
        features(0).getDefaultGeometry must be equalTo pt1
        features(1).getAttribute("number").asInstanceOf[Integer] mustEqual 456
        features(1).getAttribute("color").asInstanceOf[String] mustEqual "blue"
        features(1).getAttribute("weight").asInstanceOf[Double] mustEqual 150
        features(1).getDefaultGeometry must be equalTo pt2
      }
    }

    "parse in multi line-mode" >> {

      val jsonStr =
        """ {
          |    DataSource: { name: "myjson" },
          |    Features: [
          |      {
          |        id: 1,
          |        number: 123,
          |        color: "red",
          |        physical: {
          |          weight: 127.5,
          |          height: "5'11"
          |        },
          |        lat: 0,
          |        lon: 0
          |      },
          |      {
          |        id: 2,
          |        number: 456,
          |        color: "blue",
          |        physical: {
          |          weight: 150,
          |          height: "5'11"
          |        },
          |        lat: 1,
          |        lon: 1
          |      }
          |    ]
          | }
        """.stripMargin

      val parserConf = ConfigFactory.parseString(
        """
          | {
          |   type         = "json"
          |   id-field     = "$id"
          |   feature-path = "$.Features[*]"
          |   options {
          |     line-mode = "multi"
          |   }
          |   fields = [
          |     { name = "id",     json-type = "integer", path = "$.id",               transform = "toString($0)"      }
          |     { name = "number", json-type = "integer", path = "$.number",                                           }
          |     { name = "color",  json-type = "string",  path = "$.color",            transform = "trim($0)"          }
          |     { name = "weight", json-type = "double",  path = "$.physical.weight",                                  }
          |     { name = "lat",    json-type = "double",  path = "$.lat",                                              }
          |     { name = "lon",    json-type = "double",  path = "$.lon",                                              }
          |     { name = "geom",                                                       transform = "point($lon, $lat)" }
          |   ]
          | }
        """.stripMargin)

      val pt1 = new Point(new Coordinate(0, 0), new PrecisionModel(PrecisionModel.FIXED), 4326)
      val pt2 = new Point(new Coordinate(1, 1), new PrecisionModel(PrecisionModel.FIXED), 4326)

      WithClose(SimpleFeatureConverter(sft, parserConf)) { converter =>
        val features = WithClose(converter.process(new ByteArrayInputStream(jsonStr.getBytes)))(_.toList)
        features must haveLength(2)
        features(0).getAttribute("number").asInstanceOf[Integer] mustEqual 123
        features(0).getAttribute("color").asInstanceOf[String] mustEqual "red"
        features(0).getAttribute("weight").asInstanceOf[Double] mustEqual 127.5
        features(0).getDefaultGeometry must be equalTo pt1
        features(1).getAttribute("number").asInstanceOf[Integer] mustEqual 456
        features(1).getAttribute("color").asInstanceOf[String] mustEqual "blue"
        features(1).getAttribute("weight").asInstanceOf[Double] mustEqual 150
        features(1).getDefaultGeometry must be equalTo pt2
      }
    }

    "parse nested feature nodes" >> {
      val jsonStr =
        """ {
          |    DataSource: { name: "myjson" },
          |    Features: [
          |      {
          |        Feature: {
          |          id: 1,
          |          number: 123,
          |          color: "red",
          |          physical: {
          |            weight: 127.5,
          |            height: "5'11"
          |          },
          |          lat: 0,
          |          lon: 0
          |        }
          |      },
          |      {
          |        Feature: {
          |          id: 2,
          |          number: 456,
          |          color: "blue",
          |          physical: {
          |            weight: 150,
          |            height: "5'11"
          |          },
          |          lat: 1,
          |          lon: 1
          |        }
          |      }
          |    ]
          | }
        """.stripMargin

      val parserConf = ConfigFactory.parseString(
        """
          | {
          |   type         = "json"
          |   id-field     = "$id"
          |   feature-path = "$.Features[*].Feature"
          |   fields = [
          |     { name = "id",     json-type = "integer", path = "$.id",               transform = "toString($0)"      }
          |     { name = "number", json-type = "integer", path = "$.number",                                           }
          |     { name = "color",  json-type = "string",  path = "$.color",            transform = "trim($0)"          }
          |     { name = "weight", json-type = "double",  path = "$.physical.weight",                                  }
          |     { name = "lat",    json-type = "double",  path = "$.lat",                                              }
          |     { name = "lon",    json-type = "double",  path = "$.lon",                                              }
          |     { name = "geom",                                                       transform = "point($lon, $lat)" }
          |   ]
          | }
        """.stripMargin)

      val pt1 = new Point(new Coordinate(0, 0), new PrecisionModel(PrecisionModel.FIXED), 4326)
      val pt2 = new Point(new Coordinate(1, 1), new PrecisionModel(PrecisionModel.FIXED), 4326)

      WithClose(SimpleFeatureConverter(sft, parserConf)) { converter =>
        val features = WithClose(converter.process(new ByteArrayInputStream(jsonStr.getBytes(StandardCharsets.UTF_8))))(_.toList)
        features must haveLength(2)
        features(0).getAttribute("number").asInstanceOf[Integer] mustEqual 123
        features(0).getAttribute("color").asInstanceOf[String] mustEqual "red"
        features(0).getAttribute("weight").asInstanceOf[Double] mustEqual 127.5
        features(0).getDefaultGeometry must be equalTo pt1
        features(1).getAttribute("number").asInstanceOf[Integer] mustEqual 456
        features(1).getAttribute("color").asInstanceOf[String] mustEqual "blue"
        features(1).getAttribute("weight").asInstanceOf[Double] mustEqual 150
        features(1).getDefaultGeometry must be equalTo pt2
      }
    }

    "use an ID hash for each node" >> {
      val jsonStr =
        """ {
          |    DataSource: { name: "myjson" },
          |    Features: [
          |      {
          |        Feature: {
          |          id: 1,
          |          number: 123,
          |          color: "red",
          |          physical: {
          |            weight: 127.5,
          |            height: "5'11"
          |          },
          |          lat: 0,
          |          lon: 0
          |        }
          |      },
          |      {
          |        Feature: {
          |          id: 2,
          |          number: 456,
          |          color: "blue",
          |          physical: {
          |            weight: 150,
          |            height: "5'11"
          |          },
          |          lat: 1,
          |          lon: 1
          |        }
          |      }
          |    ]
          | }
        """.stripMargin

      val parserConf = ConfigFactory.parseString(
        """
          | {
          |   type         = "json"
          |   id-field     = "md5(string2bytes(json2string($0)))"
          |   feature-path = "$.Features[*].Feature"
          |   fields = [
          |     { name = "id",     json-type = "integer", path = "$.id",               transform = "toString($0)"      }
          |     { name = "number", json-type = "integer", path = "$.number",                                           }
          |     { name = "color",  json-type = "string",  path = "$.color",            transform = "trim($0)"          }
          |     { name = "weight", json-type = "double",  path = "$.physical.weight",                                  }
          |     { name = "lat",    json-type = "double",  path = "$.lat",                                              }
          |     { name = "lon",    json-type = "double",  path = "$.lon",                                              }
          |     { name = "geom",                                                       transform = "point($lon, $lat)" }
          |   ]
          | }
        """.stripMargin)

      WithClose(SimpleFeatureConverter(sft, parserConf)) { converter =>
        val features = WithClose(converter.process(new ByteArrayInputStream(jsonStr.getBytes(StandardCharsets.UTF_8))))(_.toList)
        features(0).getID must be equalTo "a159e39826218d193761dc4480e8eb95"
        features(1).getID must be equalTo "5ad94a63c273eac62689c636ea1ba408"
      }

    }

    "parse geojson and wkt geometries" >> {

      "transform wkt strings to points" >> {
        val jsonStr =
          """ {
            |    DataSource: { name: "myjson" },
            |    Features: [
            |      {
            |        id: 1,
            |        number: 123,
            |        color: "red",
            |        "geometry": "Point (55 56)"
            |      },
            |      {
            |        id: 2,
            |        number: 456,
            |        color: "blue",
                     "geometry": "Point (101 89)"
            |      }
            |    ]
            | }
          """.stripMargin

        val parserConf = ConfigFactory.parseString(
          """
            | {
            |   type         = "json"
            |   id-field     = "$id"
            |   feature-path = "$.Features[*]"
            |   fields = [
            |     { name = "id",      json-type = "integer",  path = "$.id",       transform = "toString($0)" }
            |     { name = "number",  json-type = "integer",  path = "$.number",                              }
            |     { name = "color",   json-type = "string",   path = "$.color",    transform = "trim($0)"     }
            |     { name = "geom",    json-type = "string",   path = "$.geometry", transform = "point($0)"     }
            |   ]
            | }
          """.stripMargin)

        val pt1 = new Point(new Coordinate(55, 56), new PrecisionModel(PrecisionModel.FIXED), 4326)
        val pt2 = new Point(new Coordinate(101, 89), new PrecisionModel(PrecisionModel.FIXED), 4326)

        WithClose(SimpleFeatureConverter(sft, parserConf)) { converter =>
          val features = WithClose(converter.process(new ByteArrayInputStream(jsonStr.getBytes(StandardCharsets.UTF_8))))(_.toList)
          features must haveLength(2)
          features(0).getAttribute("number").asInstanceOf[Integer] mustEqual 123
          features(0).getAttribute("color").asInstanceOf[String] mustEqual "red"
          features(0).getDefaultGeometry must be equalTo pt1

          features(1).getAttribute("number").asInstanceOf[Integer] mustEqual 456
          features(1).getAttribute("color").asInstanceOf[String] mustEqual "blue"
          features(1).getDefaultGeometry must be equalTo pt2
        }
      }

      "allow specific sft geom and cast into it" >> {
        val jsonStr =
          """ {
            |    DataSource: { name: "myjson" },
            |    Features: [
            |      {
            |        id: 1,
            |        number: 123,
            |        color: "red",
            |        "geometry": "LineString (55 56, 56 57)"
            |      },
            |      {
            |        id: 2,
            |        number: 456,
            |        color: "blue",
                     "geometry": "LineString ( 101 89, 102 88)"
            |      }
            |    ]
            | }
          """.stripMargin

        val parserConf = ConfigFactory.parseString(
          """
            | {
            |   type         = "json"
            |   id-field     = "$id"
            |   feature-path = "$.Features[*]"
            |   fields = [
            |     { name = "id",      json-type = "integer",  path = "$.id",       transform = "toString($0)" }
            |     { name = "number",  json-type = "integer",  path = "$.number",                              }
            |     { name = "color",   json-type = "string",   path = "$.color",    transform = "trim($0)"     }
            |     { name = "geom",    json-type = "geometry", path = "$.geometry",                            }
            |   ]
            | }
          """.stripMargin)

        WithClose(SimpleFeatureConverter(sft, parserConf)) { converter =>
          val features = WithClose(converter.process(new ByteArrayInputStream(jsonStr.getBytes(StandardCharsets.UTF_8))))(_.toList)
          features must haveLength(2)
          features(0).getDefaultGeometry must beAnInstanceOf[LineString]
          features(0).getDefaultGeometry mustEqual WKTUtils.read("LineString (55 56, 56 57)")

          features(1).getDefaultGeometry must beAnInstanceOf[LineString]
          features(1).getDefaultGeometry mustEqual WKTUtils.read("LineString (101 89, 102 88)")
        }
      }

      "parse points" >> {
        val jsonStr =
          """ {
                |    DataSource: { name: "myjson" },
                |    Features: [
                |      {
                |        id: 1,
                |        number: 123,
                |        color: "red",
                |        "geometry": {"type": "Point", "coordinates": [55, 56]}
                |      },
                |      {
                |        id: 2,
                |        number: 456,
                |        color: "blue",
                         "geometry": "Point ( 101 89)"
                |      }
                |    ]
                | }
              """.stripMargin

        val parserConf = ConfigFactory.parseString(
          """
            | {
            |   type         = "json"
            |   id-field     = "$id"
            |   feature-path = "$.Features[*]"
            |   fields = [
            |     { name = "id",      json-type = "integer",  path = "$.id",       transform = "toString($0)" }
            |     { name = "number",  json-type = "integer",  path = "$.number",                              }
            |     { name = "color",   json-type = "string",   path = "$.color",    transform = "trim($0)"     }
            |     { name = "geom",    json-type = "geometry", path = "$.geometry", transform = "point($0)"     }
            |   ]
            | }
          """.stripMargin)

        val pt1 = new Point(new Coordinate(55, 56), new PrecisionModel(PrecisionModel.FIXED), 4326)
        val pt2 = new Point(new Coordinate(101, 89), new PrecisionModel(PrecisionModel.FIXED), 4326)

        WithClose(SimpleFeatureConverter(sft, parserConf)) { converter =>
          val features = WithClose(converter.process(new ByteArrayInputStream(jsonStr.getBytes(StandardCharsets.UTF_8))))(_.toList)
          features must haveLength(2)
          features(0).getAttribute("number").asInstanceOf[Integer] mustEqual 123
          features(0).getAttribute("color").asInstanceOf[String] mustEqual "red"
          features(0).getDefaultGeometry must be equalTo pt1

          features(1).getAttribute("number").asInstanceOf[Integer] mustEqual 456
          features(1).getAttribute("color").asInstanceOf[String] mustEqual "blue"
          features(1).getDefaultGeometry must be equalTo pt2
        }
      }

      "parse mixed geometry" >> {
        val jsonStr =
          """ {
            |    DataSource: { name: "myjson" },
            |    Features: [
            |      {
            |        id: 1,
            |        number: 123,
            |        color: "red",
            |        physical: {
            |          weight: 127.5,
            |          height: "5'11"
            |        },
            |        "geometry": {"type": "Point", "coordinates": [55, 56]}
            |      },
            |      {
            |        id: 2,
            |        number: 456,
            |        color: "blue",
            |        physical: {
            |          weight: 150,
            |          height: "5'11"
            |        },
            |        "geometry": {
            |          "type": "LineString",
            |          "coordinates": [
            |            [102.0, 0.0], [103.0, 1.0], [104.0, 0.0], [105.0, 1.0]
            |          ]
            |        }
            |      },
            |      {
            |        id: 3,
            |        number: 789,
            |        color: "green",
            |        physical: {
            |          weight: 185,
            |          height: "6'2"
            |        },
            |        "geometry": {
            |           "type": "Polygon",
            |           "coordinates": [
            |             [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0],
            |               [100.0, 1.0], [100.0, 0.0] ]
            |             ]
            |         }
            |      }
            |    ]
            | }
          """.stripMargin

        val parserConf = ConfigFactory.parseString(
          """
            | {
            |   type         = "json"
            |   id-field     = "$id"
            |   feature-path = "$.Features[*]"
            |   fields = [
            |     { name = "id",     json-type = "integer",  path = "$.id",               transform = "toString($0)"      }
            |     { name = "number", json-type = "integer",  path = "$.number",                                           }
            |     { name = "color",  json-type = "string",   path = "$.color",            transform = "trim($0)"          }
            |     { name = "weight", json-type = "double",   path = "$.physical.weight",                                  }
            |     { name = "geom",   json-type = "geometry",   path = "$.geometry",                                       }
            |   ]
            | }
          """.stripMargin)

        val geoFac = new GeometryFactory()
        val pt1 = geoFac.createPoint(new Coordinate(55, 56))
        val lineStr1 = geoFac.createLineString(Seq((102, 0), (103, 1), (104, 0), (105, 1)).map{ case (x,y) => new Coordinate(x, y)}.toArray)
        val poly1 = geoFac.createPolygon(Seq((100, 0), (101, 0), (101, 1), (100, 1), (100, 0)).map{ case (x,y) => new Coordinate(x, y)}.toArray)

        WithClose(SimpleFeatureConverter(sft, parserConf)) { converter =>
          val features = WithClose(converter.process(new ByteArrayInputStream(jsonStr.getBytes(StandardCharsets.UTF_8))))(_.toList)
          features must haveLength(3)
          features(0).getAttribute("number").asInstanceOf[Integer] mustEqual 123
          features(0).getAttribute("color").asInstanceOf[String] mustEqual "red"
          features(0).getAttribute("weight").asInstanceOf[Double] mustEqual 127.5
          features(0).getDefaultGeometry must beAnInstanceOf[Point]
          features(0).getDefaultGeometry must be equalTo pt1

          features(1).getAttribute("number").asInstanceOf[Integer] mustEqual 456
          features(1).getAttribute("color").asInstanceOf[String] mustEqual "blue"
          features(1).getAttribute("weight").asInstanceOf[Double] mustEqual 150
          features(1).getDefaultGeometry must beAnInstanceOf[LineString]
          features(1).getDefaultGeometry must be equalTo lineStr1

          features(2).getAttribute("number").asInstanceOf[Integer] mustEqual 789
          features(2).getAttribute("color").asInstanceOf[String] mustEqual "green"
          features(2).getAttribute("weight").asInstanceOf[Double] mustEqual 185
          features(2).getDefaultGeometry must be equalTo poly1
          features(2).getDefaultGeometry must beAnInstanceOf[Polygon]
        }
      }

      "parse time in seconds" >> {
        val sft = SimpleFeatureTypes.createType("json-seconds", "number:Integer,dtg:Date,*geom:Point:srid=4326")
        val jsonStr = "{ id: 1, number: 123, secs: 1000, lat: 0, lon: 0 }"
        val parserConf = ConfigFactory.parseString(
          """
            | {
            |   type         = "json"
            |   id-field     = "$id"
            |   fields = [
            |     { name = "id",     json-type = "integer", path = "$.id", transform = "toString($0)" }
            |     { name = "number", json-type = "integer", path = "$.number" }
            |     { name = "secs",   json-type = "integer", path = "$.secs", transform = "toString($0)" }
            |     { name = "millis", transform = "concat($secs, '000')" }
            |     { name = "dtg",    transform = "millisToDate($millis::long)" }
            |     { name = "lat",    json-type = "double",  path = "$.lat",    }
            |     { name = "lon",    json-type = "double",  path = "$.lon",    }
            |     { name = "geom",    transform = "point($lon, $lat)" }
            |   ]
            | }
          """.stripMargin)

        WithClose(SimpleFeatureConverter(sft, parserConf)) { converter =>
          val features = WithClose(converter.process(new ByteArrayInputStream(jsonStr.getBytes(StandardCharsets.UTF_8))))(_.toList)
          features must haveLength(1)
          val f = features.head
          f.getAttribute("number") mustEqual 123
          f.getAttribute("dtg") mustEqual new Date(1000000)
          f.getDefaultGeometry.toString mustEqual "POINT (0 0)"
        }
      }
    }

    "foobar null geo" >> {
      val jsonStr =
        """ {
          |    DataSource: { name: "myjson" },
          |    Features: [
          |      {
          |        id: 1,
          |        number: 123,
          |        color: "red",
          |        "geometry": null
          |      }
          |    ]
          | }
        """.stripMargin

      val parserConf = ConfigFactory.parseString(
        """
          | {
          |   type         = "json"
          |   id-field     = "$id"
          |   feature-path = "$.Features[*]"
          |   fields = [
          |     { name = "id",      json-type = "integer",  path = "$.id",       transform = "toString($0)" }
          |     { name = "number",  json-type = "integer",  path = "$.number",                              }
          |     { name = "color",   json-type = "string",   path = "$.color",    transform = "trim($0)"     }
          |     { name = "geom",    json-type = "geometry", path = "$.geometry", transform = "point($0)"     }
          |   ]
          | }
        """.stripMargin)

      WithClose(SimpleFeatureConverter(sft, parserConf)) { converter =>
        val ec = converter.createEvaluationContext()
        val features = WithClose(converter.process(new ByteArrayInputStream(jsonStr.getBytes(StandardCharsets.UTF_8)), ec))(_.toList)
        features must haveLength(0)
        ec.success.getCount mustEqual 0
        ec.failure.getCount mustEqual 1
      }
    }

    "parse and convert json arrays into lists" >> {

      val adv = ConfigFactory.parseString(
        """{
          |  type-name = "adv"
          |  attributes = [
          |    { name = "id",    type = "Integer"       }
          |    { name = "sList", type = "List[String]"  }
          |    { name = "iList", type = "List[Integer]" }
          |    { name = "dList", type = "List[Double]"  }
          |    { name = "uList", type = "List[UUID]"    }
          |    { name = "nList", type = "List[String]"  }
          |    { name = "geom",  type = "Point"         }
          |  ]
          |}
        """.stripMargin)

      val advSft = SimpleFeatureTypes.createType(adv)

      val nestedJson =
        """ {
          |    DataSource: { name: "myjson" },
          |    Features: [
          |      {
          |        "id": 1,
          |        "geometry": {"type": "Point", "coordinates": [55, 56]},
          |        "things": [
          |          {
          |            "s": "s1",
          |            "d": 1.1,
          |            "u": "12345678-1234-1234-1234-123456781234"
          |          },
          |          {
          |            "s": "s2",
          |            "i": 2,
          |            "d": 2.2,
          |            "u": "00000000-0000-0000-0000-000000000000"
          |          },
          |        ]
          |      }
          |    ]
          | }
        """.stripMargin

      val simpleJson =
        """ {
          |    DataSource: { name: "myjson" },
          |    Features: [
          |      {
          |        "id": 1,
          |        "geometry": {"type": "Point", "coordinates": [55, 56]},
          |        "i": [2],
          |        "d": [1.1, 2.2],
          |        "s": ["s1", "s2"],
          |        "u": ["12345678-1234-1234-1234-123456781234", "00000000-0000-0000-0000-000000000000"]
          |      }
          |    ]
          | }
        """.stripMargin

      val nestedConf = ConfigFactory.parseString(
        """
          | {
          |   type         = "json"
          |   id-field     = "$id"
          |   feature-path = "$.Features[*]"
          |   fields = [
          |     { name = "id",    json-type = "integer",  path = "$.id",          transform = "toString($0)"            }
          |     { name = "sList", json-type = "array",    path = "$.things[*].s", transform = "jsonList('string', $0)"  }
          |     { name = "iList", json-type = "array",    path = "$.things[*].i", transform = "jsonList('integer', $0)" }
          |     { name = "dList", json-type = "array",    path = "$.things[*].d", transform = "jsonList('double', $0)"  }
          |     { name = "uList", json-type = "array",    path = "$.things[*].u", transform = "jsonList('UUID', $0)"    }
          |     { name = "nList", json-type = "array",    path = "$.things[*].n", transform = "jsonList('string', $0)"  }
          |     { name = "geom",  json-type = "geometry", path = "$.geometry",    transform = "point($0)"               }
          |   ]
          | }
        """.stripMargin)

      val simpleConf = ConfigFactory.parseString(
        """
          | {
          |   type         = "json"
          |   id-field     = "$id"
          |   feature-path = "$.Features[*]"
          |   fields = [
          |     { name = "id",    json-type = "integer",  path = "$.id",       transform = "toString($0)"            }
          |     { name = "sList", json-type = "array",    path = "$.s",        transform = "jsonList('string', $0)"  }
          |     { name = "iList", json-type = "array",    path = "$.i",        transform = "jsonList('integer', $0)" }
          |     { name = "dList", json-type = "array",    path = "$.d",        transform = "jsonList('double', $0)"  }
          |     { name = "uList", json-type = "array",    path = "$.u",        transform = "jsonList('UUID', $0)"    }
          |     { name = "nList", json-type = "array",    path = "$.n",        transform = "jsonList('string', $0)"  }
          |     { name = "geom",  json-type = "geometry", path = "$.geometry", transform = "point($0)"               }
          |   ]
          | }
        """.stripMargin)

      forall(List((nestedJson, nestedConf), (simpleJson, simpleConf))) { case (json, conf) =>
        WithClose(SimpleFeatureConverter(advSft, conf)) { converter =>


          val ec = converter.createEvaluationContext()
          val features = WithClose(converter.process(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)), ec))(_.toList)
          features must haveLength(1)
          ec.success.getCount mustEqual 1L
          ec.failure.getCount mustEqual 0L

          val f = features.head

          f.getAttribute("sList") must beAnInstanceOf[java.util.List[String]]
          f.getAttribute("sList").asInstanceOf[java.util.List[String]].asScala must containTheSameElementsAs(Seq("s1", "s2"))

          f.getAttribute("iList") must beAnInstanceOf[java.util.List[Integer]]
          f.getAttribute("iList").asInstanceOf[java.util.List[Integer]].asScala must containTheSameElementsAs(Seq(2))

          f.getAttribute("dList") must beAnInstanceOf[java.util.List[Double]]
          f.getAttribute("dList").asInstanceOf[java.util.List[Double]].asScala must containTheSameElementsAs(Seq(1.1, 2.2))

          f.getAttribute("uList") must beAnInstanceOf[java.util.List[UUID]]
          f.getAttribute("uList").asInstanceOf[java.util.List[UUID]].asScala must containTheSameElementsAs(
            Seq(UUID.fromString("12345678-1234-1234-1234-123456781234"),
              UUID.fromString("00000000-0000-0000-0000-000000000000")))

          if (json eq simpleJson) {
            f.getAttribute("nList") must beNull
          } else {
            f.getAttribute("nList") must beAnInstanceOf[java.util.List[String]]
            f.getAttribute("nList").asInstanceOf[java.util.List[String]].asScala must beEmpty
          }
        }
      }
    }

    "parse and convert json arrays into objects" >> {

      val sftConf = ConfigFactory.parseString(
        """{
          |  type-name = "json-obj"
          |  attributes = [
          |    { name = "id",   type = "Integer"              }
          |    { name = "json", type = "String", json = true  }
          |    { name = "geom", type = "Point"                }
          |  ]
          |}
        """.stripMargin)

      val sft = SimpleFeatureTypes.createType(sftConf)

      val json =
        """{
          |  "id": 1,
          |  "geometry": {"type": "Point", "coordinates": [55, 56]},
          |  "things": [
          |    { "foo": "bar", "baz": 1.1 },
          |    { "blu": true }
          |  ]
          |}
        """.stripMargin

      val conf = ConfigFactory.parseString(
        """
          | {
          |   type         = "json"
          |   id-field     = "$id"
          |   fields = [
          |     { name = "id",   json-type = "integer",  path = "$.id",       transform = "toString($0)"            }
          |     { name = "json", json-type = "array",    path = "$.things",   transform = "toString(jsonArrayToObject($0))"  }
          |     { name = "geom", json-type = "geometry", path = "$.geometry", transform = "point($0)"               }
          |   ]
          | }
        """.stripMargin)

      WithClose(SimpleFeatureConverter(sft, conf)) { converter =>
        val ec = converter.createEvaluationContext()
        val features = WithClose(converter.process(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)), ec))(_.toList)
        features must haveLength(1)
        ec.success.getCount mustEqual 1L
        ec.failure.getCount mustEqual 0L

        val f = features.head

        f.getAttribute("json") must beAnInstanceOf[String]
        f.getAttribute("json") mustEqual """{"0":{"foo":"bar","baz":1.1},"1":{"blu":true}}"""
      }
    }

    "parse and convert maps" >> {

      val mapSftConf = ConfigFactory.parseString(
        """{
          |  type-name = "adv"
          |  attributes = [
          |    { name = "id",   type = "Integer"            }
          |    { name = "map1", type = "Map[String,String]" }
          |    { name = "map2", type = "Map[String,String]" }
          |    { name = "map3", type = "Map[Int,Boolean]"   }
          |    { name = "geom", type = "Point"              }
          |  ]
          |}
        """.stripMargin)
      val mapSft = SimpleFeatureTypes.createType(mapSftConf)

      val json =
        """ {
          |    DataSource: { name: "myjson" },
          |    Features: [
          |      {
          |        "id": 1,
          |        "geometry": {"type": "Point", "coordinates": [55, 56]},
          |        "map1": {
          |          "a": "val1",
          |          "b": "val2"
          |        },
          |        "map2": {
          |          "a": 1.0,
          |          "b": "foobar",
          |          "c": false
          |        },
          |        "map3": {
          |          "1": true,
          |          "2": false,
          |          "3": true
          |        }
          |      }
          |    ]
          | }
        """.stripMargin

      val mapConf = ConfigFactory.parseString(
        """
          | {
          |   type         = "json"
          |   id-field     = "$id"
          |   feature-path = "$.Features[*]"
          |   fields = [
          |     { name = "id",    json-type = "integer",  path = "$.id",       transform = "toString($0)"                   }
          |     { name = "map1",  json-type = "map",      path = "$.map1",     transform = "jsonMap('string','string', $0)" }
          |     { name = "map2",  json-type = "map",      path = "$.map2",     transform = "jsonMap('string','string', $0)" }
          |     { name = "map3",  json-type = "map",      path = "$.map3",     transform = "jsonMap('int','boolean', $0)"   }
          |     { name = "geom",  json-type = "geometry", path = "$.geometry", transform = "point($0)"                      }
          |   ]
          | }
        """.stripMargin)

      WithClose(SimpleFeatureConverter(mapSft, mapConf)) { converter =>
        import org.locationtech.geomesa.utils.geotools.Conversions.RichSimpleFeature

        import java.util.{Map => JMap}

        val ec = converter.createEvaluationContext()
        val features = WithClose(converter.process(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)), ec))(_.toList)
        features must haveLength(1)
        ec.success.getCount mustEqual 1
        ec.failure.getCount mustEqual 0


        val f = features.head

        val m = f.get[JMap[String,String]]("map1")
        m must beAnInstanceOf[JMap[String,String]]
        m.size() mustEqual 2
        m.get("a") mustEqual "val1"
        m.get("b") mustEqual "val2"

        val m2 = f.get[JMap[String,String]]("map2")
        m2 must beAnInstanceOf[JMap[String,String]]
        m2.size mustEqual 3
        m2.get("a") mustEqual "1.0"
        m2.get("b") mustEqual "foobar"
        m2.get("c") mustEqual "false"

        val m3 = f.get[JMap[Int,Boolean]]("map3")
        m3 must beAnInstanceOf[JMap[Int,Boolean]]
        m3.size mustEqual 3
        m3.get(1) mustEqual true
        m3.get(2) mustEqual false
        m3.get(3) mustEqual true
      }
    }

    "parse user data" >> {
      val jsonStr =
        """ {
          |    DataSource: { name: "myjson" },
          |    Features: [
          |      {
          |        id: 1,
          |        number: 123,
          |        color: "red",
          |        physical: {
          |          weight: 127.5,
          |          height: "5'11"
          |        },
          |        lat: 0,
          |        lon: 0
          |      }
          |    ]
          | }
        """.stripMargin

      val parserConf = ConfigFactory.parseString(
        """
          | {
          |   type         = "json"
          |   id-field     = "$id"
          |   feature-path = "$.Features[*]"
          |   user-data    = {
          |     my.user.key = "$color"
          |   }
          |   fields = [
          |     { name = "id",     json-type = "integer", path = "$.id",               transform = "toString($0)"      }
          |     { name = "number", json-type = "integer", path = "$.number",                                           }
          |     { name = "color",  json-type = "string",  path = "$.color",            transform = "trim($0)"          }
          |     { name = "weight", json-type = "double",  path = "$.physical.weight",                                  }
          |     { name = "lat",    json-type = "double",  path = "$.lat",                                              }
          |     { name = "lon",    json-type = "double",  path = "$.lon",                                              }
          |     { name = "geom",                                                       transform = "point($lon, $lat)" }
          |   ]
          | }
        """.stripMargin)

      val pt1 = new Point(new Coordinate(0, 0), new PrecisionModel(PrecisionModel.FIXED), 4326)

      WithClose(SimpleFeatureConverter(sft, parserConf)) { converter =>
        val features = WithClose(converter.process(new ByteArrayInputStream(jsonStr.getBytes(StandardCharsets.UTF_8))))(_.toList)
        features must haveLength(1)
        features(0).getAttribute("number").asInstanceOf[Integer] mustEqual 123
        features(0).getAttribute("color").asInstanceOf[String] mustEqual "red"
        features(0).getAttribute("weight").asInstanceOf[Double] mustEqual 127.5
        features(0).getDefaultGeometry must be equalTo pt1
        features(0).getUserData.get("my.user.key") mustEqual "red"
      }
    }

    "parse longs, booleans, int, double, float" >> {
      val sftConf = ConfigFactory.parseString(
        """{
          |  type-name = "foo"
          |  attributes = [
          |    { name = "i", type = "int"     }
          |    { name = "l", type = "long"    }
          |    { name = "d", type = "double"  }
          |    { name = "f", type = "float"   }
          |    { name = "b", type = "boolean" }
          |  ]
          |}
        """.stripMargin)
      val typeSft = SimpleFeatureTypes.createType(sftConf)

      val json =
        """ {
          |    DataSource: { name: "myjson" },
          |    Features: [
          |      {
          |        "i": 1,
          |        "l": 9223372036854775807,
          |        "d": 1.7976931348623157E8,
          |        "f": 1.023,
          |        "b": false
          |      }
          |    ]
          | }
        """.stripMargin

      val typeConf = ConfigFactory.parseString(
        """
          | {
          |   type         = "json"
          |   id-field     = "md5(string2bytes(json2string($0)))"
          |   feature-path = "$.Features[*]"
          |   fields = [
          |     { name = "i", json-type = "integer",  path = "$.i"}
          |     { name = "l", json-type = "long",     path = "$.l"}
          |     { name = "d", json-type = "double",   path = "$.d"}
          |     { name = "f", json-type = "float",    path = "$.f"}
          |     { name = "b", json-type = "boolean",  path = "$.b"}
          |   ]
          | }
        """.stripMargin)

      WithClose(SimpleFeatureConverter(typeSft, typeConf)) { converter =>
        val ec = converter.createEvaluationContext()
        val features = WithClose(converter.process(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)), ec))(_.toList)
        features must haveLength(1)
        ec.success.getCount mustEqual 1
        ec.failure.getCount mustEqual 0
        val f = features.head

        import org.locationtech.geomesa.utils.geotools.Conversions.RichSimpleFeature
        f.get[Int]("i") mustEqual 1
        f.get[Long]("l") mustEqual Long.MaxValue
        f.get[Double]("d") mustEqual 1.7976931348623157E8
        f.get[Float]("f") mustEqual 1.023f
        f.get[Boolean]("b") mustEqual false
      }
    }

    "parse missing values as null" >> {
      val sft = SimpleFeatureTypes.createType("foo", "name:String,*geom:Point:srid=4326")
      val json = Seq(
        """{ "lat": 0, "lon": 0, "properties": { "name": "name1" } }""",
        """{ "lat": 0, "lon": 0, "properties": { "name": null } }""",
        """{ "lat": 0, "lon": 0, "properties": {} }""",
        """{ "lat": 0, "lon": 0 }"""
      ).mkString("\n")

      val parserConf = ConfigFactory.parseString(
        """
          | {
          |   type = "json"
          |   options = {
          |     line-mode = "single"
          |   }
          |   fields = [
          |     { name = "name", json-type = "string", path = "$.properties.name", }
          |     { name = "lat",  json-type = "double", path = "$.lat",             }
          |     { name = "lon",  json-type = "double", path = "$.lon",             }
          |     { name = "geom", transform = "point($lon, $lat)"                   }
          |   ]
          | }
        """.stripMargin)

      WithClose(SimpleFeatureConverter(sft, parserConf)) { converter =>
        val in = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))
        val features = WithClose(converter.process(in))(_.toList)
        features must haveLength(4)
        features.map(_.getAttribute("name")) mustEqual Seq("name1", null, null, null)
      }
    }

    "handle invalid input" >> {
      val typeSft = SimpleFeatureTypes.createType("foo", "i:Long")

      val json = "{ foobarbaz"

      val typeConf = ConfigFactory.parseString(
        """
          | {
          |   type         = "json"
          |   id-field     = "md5(string2bytes(json2string($0)))"
          |   fields = [
          |     { name = "i", json-type = "integer",  path = "$.i", transform = "$0::long" }
          |   ]
          | }
        """.stripMargin)

      WithClose(SimpleFeatureConverter(typeSft, typeConf)) { converter =>
        val ec = converter.createEvaluationContext()
        val iter = converter.process(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)), ec)
        val features = iter.toList
        features must haveLength(0)
        ec.success.getCount mustEqual 0
        ec.failure.getCount mustEqual 1
      }
    }

    "parse geojson geometries" >> {
      // geojson examples from wikipedia
      val sft = SimpleFeatureTypes.createType("geojson", "*geom:Geometry:srid=4326")
      val input = Seq(
        """{ "type":"Feature","geometry":{"type":"Point","coordinates":[30,10]}}""",
        """{ "type":"Feature","geometry":{"type":"LineString","coordinates":[[30,10],[10,30],[40,40]]}}""",
        """{ "type":"Feature","geometry":{"type":"Polygon","coordinates":[[[30,10],[40,40],[20,40],[10,20],[30,10]]]}}""",
        """{ "type":"Feature","geometry":{"type":"Polygon","coordinates":[[[35,10],[45,45],[15,40],[10,20],[35,10]],[[20,30],[35,35],[30,20],[20,30]]]}}""",
        """{ "type":"Feature","geometry":{"type":"MultiPoint","coordinates":[[10,40],[40,30],[20,20],[30,10]]}}""",
        """{ "type":"Feature","geometry":{"type":"MultiLineString","coordinates":[[[10,10],[20,20],[10,40]],[[40,40],[30,30],[40,20],[30,10]]]}}""",
        """{ "type":"Feature","geometry":{"type":"MultiPolygon","coordinates":[[[[30,20],[45,40],[10,40],[30,20]]],[[[15,5],[40,10],[10,20],[5,10],[15,5]]]]}}""",
        """{ "type":"Feature","geometry":{"type":"MultiPolygon","coordinates":[[[[40,40],[20,45],[45,30],[40,40]]],[[[20,35],[10,30],[10,10],[30,5],[45,20],[20,35]],[[30,20],[20,15],[20,25],[30,20]]]]}}"""
      ).mkString("\n")

      val conf = ConfigFactory.parseString(
        """
          | {
          |   type = "json"
          |   id-field = "md5(string2bytes(json2string($0)))"
          |   fields = [
          |     { name = "geom", json-type = "geometry",  path = "$.geometry" }
          |   ]
          | }
        """.stripMargin)
      WithClose(SimpleFeatureConverter(sft, conf)) { converter =>
        val ec = converter.createEvaluationContext()
        val in = new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8))
        val features = WithClose(converter.process(in, ec))(_.toList)
        features must haveLength(8)
        forall(features)(_.getDefaultGeometry must not(beNull))
      }
    }

    "work with different evaluation contexts" >> {
      val sft = SimpleFeatureTypes.createType("global", "foo:String,bar:String,baz:String")

      val conf = ConfigFactory.parseString(
        """
          | {
          |   type = "json"
          |   id-field = "md5(string2bytes(json2string($0)))"
          |   fields = [
          |     { name = "foo", transform = "${gp.foo}" }
          |     { name = "bar", transform = "${gp.bar}" }
          |     { name = "baz", transform = "${gp.baz}" }
          |   ]
          | }
        """.stripMargin)
      val in = "{}".getBytes(StandardCharsets.UTF_8)

      WithClose(SimpleFeatureConverter(sft, conf)) { converter =>
        foreach(Seq("foo", "bar", "baz")) { prop =>
          val ec = converter.createEvaluationContext(Map(s"gp.$prop" -> prop))
          val features = WithClose(converter.process(new ByteArrayInputStream(in), ec))(_.toList)
          features must haveLength(1)
          features.head.getAttribute(prop) mustEqual prop
          features.head.getAttributes.asScala.filter(_ == null) must haveLength(2)
        }
      }
    }

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 275e53813f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 84bc7e0e2a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ac2d5a925f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 849693a129 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7abafecb19 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> afc081190b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7a3d004ad1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> f7f5195405 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9b21f252a6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 71edb3b56e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1a21a3c300 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dd5d6434b0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0d80bae0c6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 45dca3e079 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> dc17df6c0b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 374ba65da3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 149b7a7809 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5d898c0c52 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> ee3aea1cdc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> eb01a83daf (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 8caee74520 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9814eb9768 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0166e9455b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8037623548 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e734e4d064 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ca22354dd7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 25e967804c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 4a47c538bc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a2ac294bf3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 44b15e96d9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 2d758409cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0cdf3f2672 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f9397984eb (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> cb7d3570f5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7ea07a654a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6ac55e1ef7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 686339d050 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 374ba65da3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0166e9455b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8037623548 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2d758409cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee3aea1cdc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0cdf3f2672 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f9397984eb (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6ac55e1ef7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> eb01a83daf (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f7f5195405 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> dc17df6c0b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f3a49e0825 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 3cb02b7b01 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 0da1bb22c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6289007008 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 374ba65da3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> ee3aea1cdc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> eb01a83daf (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 8037623548 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e734e4d064 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 2d758409cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 849693a129 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7abafecb19 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7a3d004ad1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 275e53813f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
>>>>>>> 7abafecb19 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7a3d004ad1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 149b7a7809 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 7ea07a654a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9b21f252a6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 45dca3e07 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5d898c0c52 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> ca22354dd7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> afc081190b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ee3aea1cdc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0f4c829f25 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> f8f49130b1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 455aae09d3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0cdf3f2672 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 425a920afa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 55b43ae566 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f9397984eb (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> cb7d3570f5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7ea07a654a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6ac55e1ef7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> eb01a83daf (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 2d758409cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7a3d004ad1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 8caee74520 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> ac2d5a925f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 3cb02b7b0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9814eb9768 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0166e9455b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 8037623548 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e734e4d064 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ca22354dd7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 2d758409cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 25e967804c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> f8f49130b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 4a47c538bc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a2ac294bf3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 0da1bb22c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 44b15e96d9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 4623d9a687 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> eccc16ddf9 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0da1bb22c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 71edb3b56e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9b21f252a6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> f7f5195405 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 275e53813f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 84bc7e0e2a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ac2d5a925f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 849693a129 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7abafecb19 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> afc081190b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7a3d004ad1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
    "create json object attributes" >> {
      val sft = SimpleFeatureTypes.createType("objects", "foobar:String:json=true")

      val conf = ConfigFactory.parseString(
        """
          | {
          |   type = "json"
          |   id-field = "md5(string2bytes(json2string($0)))"
          |   fields = [
          |     { name = "foo", path = "$.foo", json-type = "String" }
          |     { name = "bar", path = "$.bar", json-type = "Array" }
          |     { name = "foobar", transform = "toString(newJsonObject('string',$foo,'array',$bar))" }
          |   ]
          | }
        """.stripMargin)
      val in = """{"foo":"foo","bar":["bar1","bar2"]}""".getBytes(StandardCharsets.UTF_8)

      WithClose(SimpleFeatureConverter(sft, conf)) { converter =>
        val features = WithClose(converter.process(new ByteArrayInputStream(in)))(_.toList)
        features must haveLength(1)
        features.head.getAttribute("foobar") mustEqual """{"string":"foo","array":["bar1","bar2"]}"""
      }
    }

    "create json object attributes" >> {
      val sft =
        SimpleFeatureTypes.createType("objects",
          "foo:String,fooNull:String,bar:String,barNull:String,baz:String,bazNull:String")

      val conf = ConfigFactory.parseString(
        """
          | {
          |   type = "json"
          |   id-field = "md5(string2bytes(json2string($0)))"
          |   fields = [
          |     { name = "fooElem", path = "$.foo", json-type = "Object" }
          |     { name = "foo", transform = "toString($fooElem)" }
          |     { name = "fooNull", transform = "toString(emptyJsonToNull($fooElem))" }
          |     { name = "barElem", path = "$.bar", json-type = "Object" }
          |     { name = "bar", transform = "toString($barElem)" }
          |     { name = "barNull", transform = "toString(emptyJsonToNull($barElem))" }
          |     { name = "bazElem", path = "$.baz", json-type = "Array" }
          |     { name = "baz", transform = "toString($bazElem)" }
          |     { name = "bazNull", transform = "toString(emptyJsonToNull($bazElem))" }
          |   ]
          | }
        """.stripMargin)
      val in = """{"foo":{},"bar":{"bar1":null,"bar2":null},"baz":[]}""".getBytes(StandardCharsets.UTF_8)

      WithClose(SimpleFeatureConverter(sft, conf)) { converter =>
        val features = WithClose(converter.process(new ByteArrayInputStream(in)))(_.toList)
        features must haveLength(1)
        features.head.getAttribute("foo") mustEqual "{}"
        features.head.getAttribute("fooNull") must beNull
        features.head.getAttribute("bar") mustEqual "{}" // note: toString drops out the null elements
        features.head.getAttribute("barNull") must beNull
        features.head.getAttribute("baz") mustEqual "[]"
        features.head.getAttribute("bazNull") must beNull
      }
    }

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1a21a3c300 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dd5d6434b0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 686339d050 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 149b7a7809 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5d898c0c52 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ee3aea1cdc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> cb7d3570f5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7ea07a654a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6ac55e1ef7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> eb01a83daf (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 8caee74520 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9814eb9768 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0166e9455b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8037623548 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> ca22354dd7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2d758409cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 25e967804c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 4a47c538bc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a2ac294bf3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 44b15e96d9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> afc081190b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7a3d004ad1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> e734e4d064 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 71edb3b56e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9b21f252a6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f7f5195405 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 275e53813f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 84bc7e0e2a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ac2d5a925f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 849693a129 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7abafecb19 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> dc17df6c0b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6289007008 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 374ba65da3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 45dca3e07 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 455aae09d3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 0cdf3f2672 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 55b43ae566 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> f9397984eb (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 45dca3e07 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
>>>>>>> 3cb02b7b0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 595c43086a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0d80bae0c6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> eccc16ddf9 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 0da1bb22c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 45dca3e07 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
>>>>>>> 3cb02b7b0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 45dca3e079 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 275e53813f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ac2d5a925f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 849693a129 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7abafecb19 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> afc081190b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7a3d004ad1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> f7f5195405 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9b21f252a6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> dc17df6c0b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6289007008 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 374ba65da3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 5d898c0c52 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> ee3aea1cdc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> eb01a83daf (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9814eb9768 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0166e9455b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8037623548 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ca22354dd7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 2d758409cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 455aae09d3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0cdf3f2672 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 55b43ae566 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f9397984eb (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7ea07a654a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6ac55e1ef7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 686339d050 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ee3aea1cdc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6ac55e1ef7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> eb01a83daf (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0166e9455b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8037623548 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 2d758409cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f7f5195405 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> dc17df6c0b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> f3a49e0825 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 3cb02b7b01 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6289007008 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 374ba65da3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> cb7d3570f5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 149b7a7809 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> e734e4d064 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ca22354dd7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 45dca3e07 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5d898c0c52 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> eb01a83daf (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 2d758409cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ee3aea1cdc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0f4c829f25 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> f8f49130b1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 455aae09d3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 0cdf3f2672 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 425a920afa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 55b43ae566 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> f9397984eb (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> cb7d3570f5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 45dca3e07 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7ea07a654a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6ac55e1ef7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> eb01a83daf (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 2d758409cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 8caee74520 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 3cb02b7b0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 9814eb9768 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 0166e9455b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 8037623548 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> e734e4d064 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 45dca3e07 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> ca22354dd7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 2d758409cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 25e967804c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f8f49130b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 4a47c538bc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> eccc16ddf9 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0da1bb22c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a2ac294bf3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 275e53813f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 849693a129 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7abafecb19 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 7a3d004ad1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 44b15e96d9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 4623d9a687 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> eccc16ddf9 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 0da1bb22c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 71edb3b56e (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
<<<<<<< HEAD
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> afc081190b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 45dca3e07 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 9b21f252a6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f7f5195405 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7a3d004ad1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 275e53813f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 7a3d004ad1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 84bc7e0e2a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 3cb02b7b0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> ac2d5a925f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 849693a129 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 7abafecb19 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 45dca3e07 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> afc081190b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7a3d004ad1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
    "infer schema from geojson files" >> {
      val json =
        """{
          |  "type": "FeatureCollection",
          |  "features": [
          |    {
          |      "type": "Feature",
          |      "geometry": {
          |        "type": "Point",
          |        "coordinates": [41.0, 51.0]
          |      },
          |      "properties": {
          |        "name": "name1",
          |        "demographics": {
          |          "age": 1
          |        }
          |      }
          |    },
          |    {
          |      "type": "Feature",
          |      "geometry": {
          |        "type": "Point",
          |        "coordinates": [42.0, 52.0]
          |      },
          |      "properties": {
          |        "name": "name2"
          |      }
          |    },
          |    {
          |      "type": "Feature",
          |      "geometry": {
          |        "type": "Point",
          |        "coordinates": [43.0, 53.0]
          |      },
          |      "properties": {
          |        "name": "name3",
          |        "demographics": {
          |          "age": 3
          |        }
          |      }
          |    }
          |  ]
          |}
        """.stripMargin

      def bytes = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))

      val inferred = new JsonConverterFactory().infer(bytes, None, Map.empty[String, AnyRef])

      inferred must beASuccessfulTry

      val sft = inferred.get._1
      sft.getAttributeDescriptors.asScala.map(d => (d.getLocalName, d.getType.getBinding)) mustEqual
          Seq(("name", classOf[String]), ("demographics_age", classOf[Integer]), ("geom", classOf[Point]))

      WithClose(SimpleFeatureConverter(sft, inferred.get._2)) { converter =>
        converter must not(beNull)

        val features = WithClose(converter.process(bytes))(_.toList)
        features must haveLength(3)

        val expected = Seq(
          Seq("name1", 1, WKTUtils.read("POINT (41 51)")),
          Seq("name2", null, WKTUtils.read("POINT (42 52)")),
          Seq("name3", 3, WKTUtils.read("POINT (43 53)"))
        )
        features.map(_.getAttributes.asScala) must containTheSameElementsAs(expected)
      }
    }

    "infer schemas with empty attributes" in {
      val json = Seq(
        """{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"Point","coordinates":[164.2,-48.6732]},"properties":{"A":"foo"}}]}""",
        """{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"Point","coordinates":[154.3,-38.6832]},"properties":{"A":""}}]}""",
        """{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"Point","coordinates":[152.3,-38.7832]},"properties":{"A":"bar"}}]}"""
      ).mkString("\n")

      def bytes = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))

      val inferred = new JsonConverterFactory().infer(bytes, None, Map.empty[String, AnyRef])

      inferred must beASuccessfulTry

      val sft = inferred.get._1
      sft.getAttributeDescriptors.asScala.map(d => (d.getLocalName, d.getType.getBinding)) mustEqual
          Seq(("A", classOf[String]), ("geom", classOf[Point]))

      WithClose(SimpleFeatureConverter(sft, inferred.get._2)) { converter =>
        converter must not(beNull)

        val features = WithClose(converter.process(bytes))(_.toList)
        features must haveLength(3)

        val expected = Seq(
          Seq("foo", WKTUtils.read("POINT (164.2 -48.6732)")),
          Seq("",    WKTUtils.read("POINT (154.3 -38.6832)")),
          Seq("bar", WKTUtils.read("POINT (152.3 -38.7832)"))
        )
        features.map(_.getAttributes.asScala) must containTheSameElementsAs(expected)
      }
    }

    "infer schemas with all empty attributes" in {
      val json = Seq(
        """{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"Point","coordinates":[164.2,-48.6732]},"properties":{"A":""}}]}""",
        """{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"Point","coordinates":[154.3,-38.6832]},"properties":{"A":""}}]}""",
        """{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"Point","coordinates":[152.3,-38.7832]},"properties":{"A":""}}]}"""
      ).mkString("\n")

      def bytes = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))

      val inferred = new JsonConverterFactory().infer(bytes, None, Map.empty[String, AnyRef])

      inferred must beASuccessfulTry

      val sft = inferred.get._1
      sft.getAttributeDescriptors.asScala.map(d => (d.getLocalName, d.getType.getBinding)) mustEqual
          Seq(("A", classOf[String]), ("geom", classOf[Point]))

      WithClose(SimpleFeatureConverter(sft, inferred.get._2)) { converter =>
        converter must not(beNull)

        val features = WithClose(converter.process(bytes))(_.toList)
        features must haveLength(3)

        val expected = Seq(
          Seq("", WKTUtils.read("POINT (164.2 -48.6732)")),
          Seq("",    WKTUtils.read("POINT (154.3 -38.6832)")),
          Seq("", WKTUtils.read("POINT (152.3 -38.7832)"))
        )
        features.map(_.getAttributes.asScala) must containTheSameElementsAs(expected)
      }
    }

    "infer schemas with three dimensional points" in {
      val json = Seq(
        """{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"Point","coordinates":[164.2,-48.6732]},"properties":{"A":"foo"}}]}""",
        """{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"Point","coordinates":[154.3,-38.6832,500.2]},"properties":{"A":"bar"}}]}""",
        """{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"Point","coordinates":[152.3,-38.7832]},"properties":{"A":"baz"}}]}"""
      ).mkString("\n")

      def bytes = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))

      val inferred = new JsonConverterFactory().infer(bytes, None, Map.empty[String, AnyRef])

      inferred must beASuccessfulTry

      val sft = inferred.get._1
      sft.getAttributeDescriptors.asScala.map(d => (d.getLocalName, d.getType.getBinding)) mustEqual
          Seq(("A", classOf[String]), ("geom", classOf[Point]))

      WithClose(SimpleFeatureConverter(sft, inferred.get._2)) { converter =>
        converter must not(beNull)

        val features = WithClose(converter.process(bytes))(_.toList)
        features must haveLength(3)

        val expected = Seq(
          Seq("foo", WKTUtils.read("POINT (164.2 -48.6732)")),
          Seq("bar", WKTUtils.read("POINT (154.3 -38.6832 500.2)")),
          Seq("baz", WKTUtils.read("POINT (152.3 -38.7832)"))
        )
        features.map(_.getAttributes.asScala) must containTheSameElementsAs(expected)
        foreach(features.zip(expected)) { case (f, e) =>
          f.getAttribute(1).asInstanceOf[Point].getCoordinate.equals3D(e(1).asInstanceOf[Point].getCoordinate) must beTrue
        }
      }
    }

    "infer schema from non-geojson files" >> {
      verifyInferredSchema((f, is) => f.infer(is, None, Map.empty[String, AnyRef]))
    }

    "infer schema from non-geojson files using deprecated API" >> {
      verifyInferredSchema((f, is) => f.infer(is).fold[Try[(SimpleFeatureType, Config)]](Failure(null))(Success(_)))
    }

    def verifyInferredSchema(
        infer: (JsonConverterFactory, InputStream) => Try[(SimpleFeatureType, Config)]): MatchResult[Any] = {
      val json =
        """{ "name": "name1", "demographics": { "age": 1 }, "files":[1], "geom": "POINT (41 51)" }
          |{ "name": "name2", "files":[2,3], "geom": "POINT (42 52)" }
          |{ "name": "name3", "demographics": { "age": 3 }, "files":[4,5,6], "geom": "POINT (43 53)" }
        """.stripMargin

      def bytes = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))

      val factory = new JsonConverterFactory()
      val inferred = infer(factory, bytes)
      inferred must beASuccessfulTry

      val sft = inferred.get._1
      sft.getAttributeDescriptors.asScala.map(_.getLocalName) mustEqual
          Seq("name", "demographics_age", "files", "geom")
      sft.getAttributeDescriptors.asScala.map(_.getType.getBinding) mustEqual
          Seq(classOf[String], classOf[Integer], classOf[java.util.List[_]], classOf[Point])
      WithClose(SimpleFeatureConverter(sft, inferred.get._2)) { converter =>
        converter must not(beNull)

        val features = WithClose(converter.process(bytes))(_.toList)
        features must haveLength(3)

        val expected = Seq(
          Seq("name1", 1, Seq("1").asJava, WKTUtils.read("POINT (41 51)")),
          Seq("name2", null, Seq("2", "3").asJava, WKTUtils.read("POINT (42 52)")),
          Seq("name3", 3, Seq("4", "5", "6").asJava, WKTUtils.read("POINT (43 53)"))
        )
        features.map(_.getAttributes.asScala) must containTheSameElementsAs(expected)
      }
    }
    "infer schema from non-geojson files with feature path" >> {
      val json =
        """[{ "name": "name1", "demographics": { "age": 1 }, "geom": "POINT (41 51)" },
          |{ "name": "name2", "geom": "POINT (42 52)" },
          |{ "name": "name3", "demographics": { "age": 3 }, "geom": "POINT (43 53)" }]
          """.stripMargin

      def bytes = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))

      val inferred = new JsonConverterFactory().infer(bytes, None, Map[String, AnyRef]("featurePath" -> "$.[*]"))

      inferred must beASuccessfulTry

      val sft = inferred.get._1
      sft.getAttributeDescriptors.asScala.map(d => (d.getLocalName, d.getType.getBinding)) mustEqual
          Seq(("name", classOf[String]), ("demographics_age", classOf[Integer]), ("geom", classOf[Point]))

      WithClose(SimpleFeatureConverter(sft, inferred.get._2)) { converter =>
        converter must not(beNull)

        val features = WithClose(converter.process(bytes))(_.toList)
        features must haveLength(3)

        val expected = Seq(
          Seq("name1", 1, WKTUtils.read("POINT (41 51)")),
          Seq("name2", null, WKTUtils.read("POINT (42 52)")),
          Seq("name3", 3, WKTUtils.read("POINT (43 53)"))
        )
        features.map(_.getAttributes.asScala) must containTheSameElementsAs(expected)
      }
    }

    "infer schema from non-geojson arrays with implicit feature path" >> {
      val json =
        """[{ "name": "name1", "demographics": { "age": 1 }, "geom": "POINT (41 51)" },
          |{ "name": "name2", "geom": "POINT (42 52)" },
          |{ "name": "name3", "demographics": { "age": 3 }, "geom": "POINT (43 53)" }]
          """.stripMargin

      def bytes = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))

      val inferred = new JsonConverterFactory().infer(bytes, None, Map.empty[String, AnyRef])

      inferred must beASuccessfulTry

      val sft = inferred.get._1
      sft.getAttributeDescriptors.asScala.map(d => (d.getLocalName, d.getType.getBinding)) mustEqual
          Seq(("name", classOf[String]), ("demographics_age", classOf[Integer]), ("geom", classOf[Point]))

      WithClose(SimpleFeatureConverter(sft, inferred.get._2)) { converter =>
        converter must not(beNull)

        val features = WithClose(converter.process(bytes))(_.toList)
        features must haveLength(3)

        val expected = Seq(
          Seq("name1", 1, WKTUtils.read("POINT (41 51)")),
          Seq("name2", null, WKTUtils.read("POINT (42 52)")),
          Seq("name3", 3, WKTUtils.read("POINT (43 53)"))
        )
        features.map(_.getAttributes.asScala) must containTheSameElementsAs(expected)
      }
    }
  }
}
