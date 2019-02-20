/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.shp

import java.nio.file.Paths

import com.typesafe.config.ConfigFactory
import org.locationtech.jts.geom.MultiPolygon
import org.junit.runner.RunWith
import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.convert2.SimpleFeatureConverter
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ShapefileConverterTest extends Specification {

  val spec = "name:String,abbr:String,area:Long,*geom:MultiPolygon"

  val sft = SimpleFeatureTypes.createType("states", spec)

  lazy val shp = this.getClass.getClassLoader.getResource("cb_2017_us_state_20m.shp")
  lazy val shpFile = Paths.get(shp.toURI).toFile.getAbsolutePath

  // fields in the shapefile:

  val shpSpec = "*the_geom:MultiPolygon,STATEFP:String,STATENS:String,AFFGEOID:String,GEOID:String,STUSPS:String," +
      "NAME:String,LSAD:String,ALAND:Long,AWATER:Long"

  "ShapefileConverter" should {

    "parse shapefiles" in {
      val conf = ConfigFactory.parseString(
        """
          | {
          |   type         = "shp",
          |   id-field     = "shpFeatureId()",
          |   fields = [
          |     { name = "name", transform = "$7" }, // example of lookup by field number
          |     { name = "abbr", transform = "shp('STUSPS')" }, // example of lookup by name
          |     { name = "area", transform = "add(shp('ALAND'),shp('AWATER'))" },
          |     { name = "geom", transform = "shp('the_geom')" },
          |   ]
          | }
        """.stripMargin)

      WithClose(SimpleFeatureConverter(sft, conf)) { converter =>
        converter must not(beNull)

        // shapefile converter requires the input file path in order to load the related files
        val ec = converter.createEvaluationContext(EvaluationContext.inputFileParam(shpFile))
        val res = SelfClosingIterator(converter.process(shp.openStream(), ec)).toList

        res must haveLength(52) // 50 states, DC, and puerto rico

        foreach(res) { state =>
          state.getDefaultGeometry must beAnInstanceOf[MultiPolygon]
          state.getAttribute("name") must not(beNull)
          state.getAttribute("abbr") must not(beNull)
          state.getAttribute("area") must not(beNull)
        }

        res.map(_.getAttribute("name")) must containAllOf(Seq("Alaska", "California", "New York", "Virginia"))
        res.map(_.getAttribute("abbr")) must containAllOf(Seq("AK", "CA", "NY", "VA"))
      }
    }

    "infer converters" in {
      val inferred = ShapefileConverterFactory.infer(shpFile, None)
      inferred must beSome

      val (sft, conf) = inferred.get

      sft.getAttributeCount mustEqual 10
      SimpleFeatureTypes.encodeType(sft) mustEqual shpSpec

      WithClose(SimpleFeatureConverter(sft, conf)) { converter =>
        converter must not(beNull)

        // shapefile converter requires the input file path in order to load the related files
        val ec = converter.createEvaluationContext(EvaluationContext.inputFileParam(shpFile))
        val res = SelfClosingIterator(converter.process(shp.openStream(), ec)).toList

        res must haveLength(52) // 50 states, DC, and puerto rico

        foreach(res) { state =>
          state.getDefaultGeometry must beAnInstanceOf[MultiPolygon]
          state.getAttribute("NAME") must not(beNull)
          state.getAttribute("STUSPS") must not(beNull)
          state.getAttribute("ALAND") must not(beNull)
        }

        res.map(_.getAttribute("NAME")) must containAllOf(Seq("Alaska", "California", "New York", "Virginia"))
        res.map(_.getAttribute("STUSPS")) must containAllOf(Seq("AK", "CA", "NY", "VA"))
      }
    }
  }
}
