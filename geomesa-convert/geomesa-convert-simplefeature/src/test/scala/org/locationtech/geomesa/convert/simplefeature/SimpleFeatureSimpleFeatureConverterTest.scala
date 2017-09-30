package org.locationtech.geomesa.convert.simplefeature

import com.typesafe.config.ConfigFactory
import org.specs2.mutable.Specification

class SimpleFeatureSimpleFeatureConverterTest extends Specification {

  "SimpleFeature2SimpleFeature" should {
    "convert one SF to another SF" >> {
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

      val parserConf = ConfigFactory.parseString(
        """
          | {
          |   type         = "simplefeature"
          |   id-field     = "$id"
          |   input-sft    = "inputsftname"
          |   fields = [
          |     { name = "id",     path = "$.id",               transform = "toString($0)"      }
          |     { name = "number", json-type = "integer", path = "$.number",                                           }
          |     { name = "color",  json-type = "string",  path = "$.color",            transform = "trim($0)"          }
          |     { name = "weight", json-type = "double",  path = "$.physical.weight",                                  }
          |     { name = "lat",    json-type = "double",  path = "$.lat",                                              }
          |     { name = "lon",    json-type = "double",  path = "$.lon",                                              }
          |     { name = "geom",                                                       transform = "point($lon, $lat)" }
          |   ]
          | }
        """.stripMargin)

      val conf = ConfigFactory.parseString(
        """
          |{
        """.stripMargin)
    }
  }
}
