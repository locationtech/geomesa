/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.filter.factory

import org.geotools.api.filter.expression.Literal
import org.geotools.api.filter.spatial.BBOX
import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.text.ecql.ECQL
import org.geotools.util.factory.Hints
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.filter.expression.{FastPropertyIsEqualTo, FastPropertyName, OrHashEquality, OrSequentialEquality}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.execute.Result
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FastFilterFactoryTest extends Specification {

  "FastFilterFactory" should {
    "be loadable via hints" >> {
      val hints = new Hints()
      hints.put(Hints.FILTER_FACTORY, classOf[FastFilterFactory])
      val ff = CommonFactoryFinder.getFilterFactory(hints)
      ff must beAnInstanceOf[FastFilterFactory]
    }
    "be loadable via system hints" >> {
      skipped("This might work if property is set globally at jvm startup...")
      System.setProperty("org.geotools.api.filter.FilterFactory", classOf[FastFilterFactory].getName)
      try {
        Hints.scanSystemProperties()
        val ff = CommonFactoryFinder.getFilterFactory
        ff must beAnInstanceOf[FastFilterFactory]
      } finally {
        System.clearProperty("org.geotools.api.filter.FilterFactory")
        Hints.scanSystemProperties()
      }
    }
    "create fast property names via ECQL" >> {
      val sft = SimpleFeatureTypes.createType("test", "geom:Point:srid=4326")
      val bbox = FastFilterFactory.toFilter(sft, "bbox(geom, -179, -89, 179, 89)")
      bbox.asInstanceOf[BBOX].getExpression1 must beAnInstanceOf[FastPropertyName]
    }
    "create sequential OR filter" >> {
      val sft = SimpleFeatureTypes.createType("test", "name:String,*geom:Point:srid=4326")
      val or = FastFilterFactory.toFilter(sft, "name = 'foo' OR name = 'bar' OR name = 'baz'")
      or must beAnInstanceOf[OrSequentialEquality]
      foreach(Seq("foo", "bar", "baz")) { name =>
        or.evaluate(ScalaSimpleFeature.create(sft, "1", name, "POINT (45 55)")) must beTrue
      }
      foreach(Seq("blu", "qux")) { name =>
        or.evaluate(ScalaSimpleFeature.create(sft, "1", name, "POINT (45 55)")) must beFalse
      }
    }
    "create hash OR filter for > 4 values" >> {
      val sft = SimpleFeatureTypes.createType("test", "name:String,*geom:Point:srid=4326")
      val or = FastFilterFactory.toFilter(sft, "name = 'foo' OR name = 'bar' OR name = 'baz' OR name = 'foo2' " +
          "OR name = 'bar2' OR name = 'baz2' OR name = 'foo3'")
      or must beAnInstanceOf[OrHashEquality]
      foreach(Seq("foo", "bar", "baz", "foo2", "bar2", "baz2", "foo3")) { name =>
        or.evaluate(ScalaSimpleFeature.create(sft, "1", name, "POINT (45 55)")) must beTrue
      }
      foreach(Seq("blu", "qux")) { name =>
        or.evaluate(ScalaSimpleFeature.create(sft, "1", name, "POINT (45 55)")) must beFalse
      }
    }
    "fall back to standard OR filters" >> {
      val sft = SimpleFeatureTypes.createType("test", "name:String,*geom:Point:srid=4326")
      val or = FastFilterFactory.toFilter(sft, "name = 'foo' OR name = 'bar' OR bbox(geom,-120,45,-115,50)")
      or must not(beAnInstanceOf[OrHashEquality])
      or must not(beAnInstanceOf[OrSequentialEquality])
      foreach(Seq("foo", "bar")) { name =>
        or.evaluate(ScalaSimpleFeature.create(sft, "1", name, "POINT (45 55)")) must beTrue
      }
      foreach(Seq("blu", "qux")) { name =>
        or.evaluate(ScalaSimpleFeature.create(sft, "1", name, "POINT (45 55)")) must beFalse
      }
    }
    "kick out bad filters from optimize" >> {
      val sft = SimpleFeatureTypes.createType("test", "name:String,*geom:Point:srid=4326")
      FastFilterFactory.toFilter(sft, "name ilike '%abc\\'") must throwA[IllegalArgumentException]
      FastFilterFactory.toFilter(sft, "name like '%abc\\'") must throwA[IllegalArgumentException]
      ok
    }
<<<<<<< HEAD
    "handle logical erasure of entire clauses" >> {
      val sft = SimpleFeatureTypes.createType("test", "a:Integer")
      val ecql = "(a = 0 AND a = 0) OR (a = 0 AND a = 1) OR (a = 0 AND a = 2)"
      val filter = FastFilterFactory.toFilter(sft, ecql)
      filter must beAnInstanceOf[FastPropertyIsEqualTo]
      filter.asInstanceOf[FastPropertyIsEqualTo].getExpression1 must beAnInstanceOf[FastPropertyName]
      filter.asInstanceOf[FastPropertyIsEqualTo].getExpression1.asInstanceOf[FastPropertyName].getPropertyName mustEqual "a"
      filter.asInstanceOf[FastPropertyIsEqualTo].getExpression2 must beAnInstanceOf[Literal]
      filter.asInstanceOf[FastPropertyIsEqualTo].getExpression2.asInstanceOf[Literal].getValue mustEqual 0
    }
=======
>>>>>>> 7acc3ea465 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
    "create filter containing functions with embedded expressions" >> {
      val sft = SimpleFeatureTypes.createType("test", "f0:Integer,f1:Integer")
      val ecql = "min(f0 + 2, 4) < min(f1, 5)"
      val filterGeoMesa = FastFilterFactory.toFilter(sft, ecql)
      val filterGeoTools = ECQL.toFilter(ecql)
      Result.foreach(1 to 10) { i =>
<<<<<<< HEAD
        Result.foreach(1 to 10) { j =>
=======
        Result.foreach(1 to 10){ j =>
>>>>>>> 7acc3ea465 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
          val sf = ScalaSimpleFeature.create(sft, s"${i}_${j}", i, j)
          filterGeoMesa.evaluate(sf) mustEqual filterGeoTools.evaluate(sf)
        }
      }
    }
<<<<<<< HEAD
    "comparison operators should have consistent semantics" >> {
      val sft = SimpleFeatureTypes.createType("test", "s:String")
      val sf = ScalaSimpleFeature.create(sft, "id", "5")
      FastFilterFactory.toFilter(sft, "s > '100'").evaluate(sf) must beTrue
      FastFilterFactory.toFilter(sft, "s >= '100'").evaluate(sf) must beTrue
      FastFilterFactory.toFilter(sft, "s < '100'").evaluate(sf) must beFalse
      FastFilterFactory.toFilter(sft, "s <= '100'").evaluate(sf) must beFalse
    }
=======
>>>>>>> 7acc3ea465 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
  }
}
