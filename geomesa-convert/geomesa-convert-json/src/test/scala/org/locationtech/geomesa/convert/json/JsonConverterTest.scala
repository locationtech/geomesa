package org.locationtech.geomesa.convert.json

import com.typesafe.config.ConfigFactory
import com.vividsolutions.jts.geom.{PrecisionModel, Coordinate, Point}
import org.junit.runner.RunWith
import org.locationtech.geomesa.convert.SimpleFeatureConverters
import org.locationtech.geomesa.convert.Transformers.EvaluationContext
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class JsonConverterTest extends Specification {

  val sftConf = ConfigFactory.parseString(
    """{ type-name = "jsonFeatureType"
      |  attributes = [
      |    { name = "number", type = "Integer" }
      |    { name = "color",  type = "String"  }
      |    { name = "weight", type = "Double"  }
      |    { name = "geom",   type = "Point"   }
      |  ]
      |}
    """.stripMargin)

  val sft = SimpleFeatureTypes.createType(sftConf)
  implicit val ec = new EvaluationContext(null, null)

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
          | converter = {
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

      val converter = SimpleFeatureConverters.build[String](sft, parserConf)
      val features = converter.processInput(Iterator(jsonStr)).toList
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
          | converter = {
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

      val converter = SimpleFeatureConverters.build[String](sft, parserConf)
      val features = converter.processInput(Iterator(jsonStr)).toList
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
          | converter = {
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

      val converter = SimpleFeatureConverters.build[String](sft, parserConf)
      val features = converter.processInput(Iterator(jsonStr)).toList
      features(0).getID must be equalTo "a159e39826218d193761dc4480e8eb95"
      features(1).getID must be equalTo "5ad94a63c273eac62689c636ea1ba408"

    }

  }
}
