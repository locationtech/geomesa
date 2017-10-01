package org.locationtech.geomesa.convert.simplefeature

import com.typesafe.config.ConfigFactory
import com.vividsolutions.jts.geom.Coordinate
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.geometry.jts.JTSFactoryFinder
import org.locationtech.geomesa.convert.SimpleFeatureConverters
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.locationtech.geomesa.utils.geotools.{SimpleFeatureTypeLoader, SimpleFeatureTypes}
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mutable.Specification

class SimpleFeatureSimpleFeatureConverterTest extends Specification {

  "SimpleFeature2SimpleFeature" should {
    "convert one SF to another SF" >> {
      val outConfPoint = ConfigFactory.parseString(
        """{ type-name = "outtype"
          |  attributes = [
          |    { name = "number"   , type = "Integer" }
          |    { name = "color"    , type = "String"  }
          |    { name = "weight"   , type = "Double"  }
          |    { name = "numberx2" , type = "Integer" }
          |    { name = "geom"     , type = "Point"   }
          |  ]
          |}
        """.stripMargin)

      val parserConf = ConfigFactory.parseString(
        """
          | {
          |   type         = "simple-feature"
          |   input-sft    = "intype"
          |   fields = [
          |     { name = "number"    , attribute = "number"  }
          |     { name = "color"     , attribute = "color"   }
          |     { name = "weight"    , attribute = "weight"  }
          |     { name = "geom"      , attribute = "geom"    }
          |     { name = "numberx2"  , attribute = "number"   , transform = "add($0, $0)::int" }
          |   ]
          | }
        """.stripMargin)

      val insft  = SimpleFeatureTypeLoader.sftForName("intype").get
      val outsft = SimpleFeatureTypes.createType(outConfPoint)

      val conv = SimpleFeatureConverters.build[SimpleFeature](outsft, parserConf)

      val gf = JTSFactoryFinder.getGeometryFactory
      val pt = gf.createPoint(new Coordinate(10, 10))
      val builder = new SimpleFeatureBuilder(insft)
      builder.reset()
      builder.addAll(Array(1, "blue", 10.0, pt).asInstanceOf[Array[AnyRef]])
      val sf = builder.buildFeature("1")

      val ec = conv.createEvaluationContext()
      val res = conv.processSingleInput(sf, ec)

      "must not be null" >> { res must not beNull }
      "numberx2 should be 2" >> { res.head.get[Int]("numberx2") must be equalTo 2 }
    }

    "copy default fields" >> {
      val outConfPoint = ConfigFactory.parseString(
        """{ type-name = "outtype"
          |  attributes = [
          |    { name = "number"   , type = "Integer" }
          |    { name = "color"    , type = "String"  }
          |    { name = "weight"   , type = "Double"  }
          |    { name = "numberx2" , type = "Integer" }
          |    { name = "geom"     , type = "Point"   }
          |  ]
          |}
        """.stripMargin)

      val parserConf = ConfigFactory.parseString(
        """
          | {
          |   type         = "simple-feature"
          |   input-sft    = "intype"
          |   fields = [
          |     { name = "numberx2"  , attribute = "number"   , transform = "add($0, $0)::int" }
          |   ]
          | }
        """.stripMargin)

      val insft  = SimpleFeatureTypeLoader.sftForName("intype").get
      val outsft = SimpleFeatureTypes.createType(outConfPoint)

      val conv = SimpleFeatureConverters.build[SimpleFeature](outsft, parserConf)

      val gf = JTSFactoryFinder.getGeometryFactory
      val pt = gf.createPoint(new Coordinate(10, 10))
      val builder = new SimpleFeatureBuilder(insft)
      builder.reset()
      builder.addAll(Array(1, "blue", 10.0, pt).asInstanceOf[Array[AnyRef]])
      val sf = builder.buildFeature("1")

      val ec = conv.createEvaluationContext()
      val res = conv.processSingleInput(sf, ec)

      "must not be null" >> { res must not beNull }
      "numberx2 should be 2" >> { res.head.get[Int]("numberx2") must be equalTo 2 }
      "color must be blue" >> { res.head.get[String]("color") must be equalTo "blue" }
    }

  }
}
