/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert2.simplefeature

import com.typesafe.config.ConfigFactory
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.geometry.jts.JTSFactoryFinder
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.{SimpleFeatureTypeLoader, SimpleFeatureTypes}
import org.locationtech.jts.geom.Coordinate
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FeatureToFeatureConverterTest extends Specification {

  "FeatureToFeatureConverter" should {
    "convert one SF to another SF" >> {
      val outConfPoint = ConfigFactory.parseString(
        """
          |{
          |  type-name = "outtype"
          |  attributes = [
          |    { name = "number",   type = "Integer" }
          |    { name = "color",    type = "String"  }
          |    { name = "weight",   type = "Double"  }
          |    { name = "numberx2", type = "Integer" }
          |    { name = "geom",     type = "Point"   }
          |  ]
          |}
        """.stripMargin)

      val parserConf = ConfigFactory.parseString(
        """
          |{
          |  type      = "simple-feature"
          |  input-sft = "intype"
          |  fields = [
          |    { name = "number",   transform = "$number" }
          |    { name = "color" ,   transform = "$color"  }
          |    { name = "weight",   transform = "$weight" }
          |    { name = "geom",     transform = "$geom"   }
          |    { name = "numberx2", transform = "add($number, $number)::int" }
          |  ]
          |}
        """.stripMargin)

      val insft  = SimpleFeatureTypeLoader.sftForName("intype").get
      val outsft = SimpleFeatureTypes.createType(outConfPoint)

      val converter = FeatureToFeatureConverter(outsft, parserConf)
      converter must not(beNull)

      val gf = JTSFactoryFinder.getGeometryFactory
      val pt = gf.createPoint(new Coordinate(10, 10))
      val builder = new SimpleFeatureBuilder(insft)
      builder.reset()
      builder.addAll(Array(1, "blue", 10.0, pt).asInstanceOf[Array[AnyRef]])
      val sf = builder.buildFeature("1")

      val ec = converter.createEvaluationContext()
      val res = converter.convert(CloseableIterator.single(sf), ec).toSeq

      res must haveLength(1)
      res.head.getAttribute("numberx2") mustEqual 2
    }

    "copy default fields" >> {
      // we drop the weight field and add numberx2 field
      val outConfPoint = ConfigFactory.parseString(
        """
          |{
          |  type-name = "outtype"
          |  attributes = [
          |    { name = "number",   type = "Integer" }
          |    { name = "color",    type = "String"  }
          |    { name = "numberx2", type = "Integer" }
          |    { name = "geom",     type = "Point"   }
          |  ]
          |}
        """.stripMargin)

      val parserConf = ConfigFactory.parseString(
        """
          |{
          |  type      = "simple-feature"
          |  input-sft = "intype"
          |  fields = [
          |    { name = "numberx2", transform = "add($number, $number)::int" }
          |  ]
          |}
        """.stripMargin)

      val insft  = SimpleFeatureTypeLoader.sftForName("intype").get
      val outsft = SimpleFeatureTypes.createType(outConfPoint)

      val converter = FeatureToFeatureConverter(outsft, parserConf)
      converter must not(beNull)

      val gf = JTSFactoryFinder.getGeometryFactory
      val pt = gf.createPoint(new Coordinate(10, 10))
      val builder = new SimpleFeatureBuilder(insft)
      builder.reset()
      builder.addAll(Array(1, "blue", 10.0, pt).asInstanceOf[Array[AnyRef]])
      val sf = builder.buildFeature("1")

      val ec = converter.createEvaluationContext()
      val res = converter.convert(CloseableIterator.single(sf), ec).toSeq

      res must haveLength(1)
      res.head.getAttribute("numberx2") mustEqual 2
      res.head.getAttribute("color") mustEqual "blue"
    }
  }
}
