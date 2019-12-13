/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.json

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets

import com.typesafe.config.ConfigFactory
import org.junit.runner.RunWith
import org.locationtech.geomesa.convert2.SimpleFeatureConverter
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class JsonCompositeConverterTest extends Specification {

  sequential

  val sft = SimpleFeatureTypes.createType(
    ConfigFactory.parseString(
      """{ type-name = "jsonFeatureType"
        |  attributes = [
        |    { name = "number", type = "Integer" }
        |    { name = "color",  type = "String"  }
        |    { name = "weight", type = "Double"  }
        |    { name = "geom",   type = "Point"   }
        |  ]
        |}
      """.stripMargin)
  )

  "JsonCompositeConverter" should {

    "select a parser based on a predicate" >> {
      val jsonStr =
        """{"type":"a","id":"1","number":123,"color":"red","physical":{"weight":127.5,"height":"5'11"},"lat":0,"lon":0}
          |{"type":"b","fid":"2","num":1234,"geometry":{"type":"Point","coordinates":[30,10]}}
        """.stripMargin

      val parserConf = ConfigFactory.parseString(
        """
          | {
          |   type = "composite-json"
          |   options = { verbose = true }
          |   converters = [
          |     {
          |       predicate = "jsonPath('$.type', $0) == 'a'",
          |       id-field  = "jsonPath('$.id', $0)",
          |       fields = [
          |         { name = "number", json-type = "integer", path = "$.number",                        }
          |         { name = "color",  json-type = "string",  path = "$.color", transform = "trim($0)", }
          |         { name = "weight", json-type = "double",  path = "$.physical.weight",               }
          |         { name = "lat",    json-type = "double",  path = "$.lat",                           }
          |         { name = "lon",    json-type = "double",  path = "$.lon",                           }
          |         { name = "geom",   transform = "point($lon, $lat)",                                 }
          |       ]
          |     },
          |     {
          |       predicate = "jsonPath('$.type', $0) == 'b'",
          |       id-field  = "jsonPath('$.fid', $0)",
          |       fields = [
          |         { name = "number", json-type = "integer", path = "$.num",        }
          |         { name = "geom",   json-type = "geometry",  path = "$.geometry", }
          |       ]
          |     }
          |   ]
          | }
        """.stripMargin)

      WithClose(SimpleFeatureConverter(sft, parserConf)) { converter =>
        val features = WithClose(converter.process(new ByteArrayInputStream(jsonStr.getBytes(StandardCharsets.UTF_8))))(_.toList)
        features must haveLength(2)
        features(0).getID mustEqual "1"
        features(0).getAttribute("number") mustEqual 123
        features(0).getAttribute("color") mustEqual "red"
        features(0).getAttribute("weight") mustEqual 127.5
        features(0).getDefaultGeometry  mustEqual WKTUtils.read("POINT (0 0)")
        features(1).getID mustEqual "2"
        features(1).getAttribute("number") mustEqual 1234
        features(1).getAttribute("color") must beNull
        features(1).getAttribute("weight") must beNull
        features(1).getDefaultGeometry  mustEqual WKTUtils.read("POINT (30 10)")
      }
    }
  }
}
