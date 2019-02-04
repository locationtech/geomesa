/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.xml

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
class XmlCompositeConverterTest extends Specification {

  sequential

  val sft = SimpleFeatureTypes.createType(
    ConfigFactory.parseString(
      """{ type-name = "xmlFeatureType"
        |  attributes = [
        |    { name = "number", type = "Integer" }
        |    { name = "color",  type = "String"  }
        |    { name = "weight", type = "Double"  }
        |    { name = "geom",   type = "Point"   }
        |  ]
        |}
      """.stripMargin)
  )

  "XmlCompositeConverter" should {

    "select a parser based on a predicate" >> {
      val xmlStr =
        """<doc><type>a</type><id>1</id><number>123</number><color>red</color><physical><weight>127.5</weight><height>5'11"</height></physical><lat>0</lat><lon>0</lon></doc>
          |<doc><type>b</type><fid>2</fid><num>1234</num><geometry>POINT (30 10)</geometry></doc>
        """.stripMargin

      val parserConf = ConfigFactory.parseString(
        """
          | {
          |   type = "composite-xml"
          |   options = {
          |     line-mode  = "single"
          |     verbose = true
          |   }
          |   converters = [
          |     {
          |       predicate = "xpath('type', $0) == 'a'",
          |       id-field  = "$fid",
          |       fields = [
          |         { name = "number", path = "number",          transform = "$0::int"    }
          |         { name = "color",  path = "color",           transform = "trim($0)",  }
          |         { name = "weight", path = "physical/weight", transform = "$0::double" }
          |         { name = "lat",    path = "lat",                                      }
          |         { name = "lon",    path = "lon",                                      }
          |         { name = "geom",   transform = "point($lon::double, $lat::double)",   }
          |         { name = "fid",    path = "id" }
          |       ]
          |     },
          |     {
          |       predicate = "xpath('type', $0) == 'b'",
          |       id-field  = "$fid",
          |       fields = [
          |         { name = "number", path = "num",      transform = "$0::int"   }
          |         { name = "geom",   path = "geometry", transform = "point($0)" }
          |         { name = "fid",    path = "fid"                               }
          |       ]
          |     }
          |   ]
          | }
        """.stripMargin)

      WithClose(SimpleFeatureConverter(sft, parserConf)) { converter =>
        val features = WithClose(converter.process(new ByteArrayInputStream(xmlStr.getBytes(StandardCharsets.UTF_8))))(_.toList)
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
