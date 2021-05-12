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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
import org.opengis.filter.PropertyIsEqualTo
<<<<<<< HEAD
>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d9ed077cd1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
import org.opengis.filter.PropertyIsEqualTo
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6d9a5b626c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
import org.opengis.filter.expression.Literal
import org.opengis.filter.spatial.BBOX
>>>>>>> 759250a5978 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
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
>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f36b0e8b9a (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> cdb4102515 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e3a63fee18 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 0e68e9f4cc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 67b361c856 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9d708b240a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 3333975e77 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 76618c8da3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9638dc731e (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 97ec7d864a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> ad3cedc4db (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> bc49131942 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> bdbced26d4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 0dbede148d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 22dcc8170f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b009fd23f4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a63cb7e74 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> c3b0a1b6e3 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e6e1f4bdd5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 7f922b040c (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> a910ae135f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> a63cb7e74 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 8fb0607344 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 240977229 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 45ad5d11f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f36b0e8b9 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a63cb7e740 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 9d708b240a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c3b0a1b6e3 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 22dcc8170f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e6e1f4bdd5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8fb0607344 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> ad3cedc4db (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 240977229 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 67b361c856 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 908ad4071d (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> b4320946b4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 907fb0b08a (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d9ed077cd1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f36b0e8b9 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> e182e6e99c (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 9d708b240a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> ad3cedc4db (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 22dcc8170f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e3a63fee1 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 3333975e77 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> a910ae135f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 0e68e9f4c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 76618c8da3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> b009fd23f4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a63cb7e74 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c3b0a1b6e3 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 908ad4071 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9638dc731e (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> bdbced26d4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> b4320946b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 97ec7d864a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7bb5f26c3e (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6d9a5b626c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f36b0e8b9 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> e5ed68fdd3 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> e6e1f4bdd5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> e3a63fee1 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 7f922b040c (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> a910ae135f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> a63cb7e74 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 8fb0607344 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> ad3cedc4db (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 908ad4071 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> bc49131942 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> bdbced26d4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 87bf55340 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 0dbede148d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 240977229 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 22dcc8170f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 45ad5d11f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> b009fd23f4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
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
>>>>>>> 7acc3ea465 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 7acc3ea46 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> f36b0e8b9a (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> a63cb7e740 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 3333975e77 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 76618c8da3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c3b0a1b6e3 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9638dc731e (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 97ec7d864a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 7f922b040c (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> a910ae135f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8fb0607344 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> ad3cedc4db (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> bc49131942 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> bdbced26d4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 0dbede148d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 22dcc8170f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 45ad5d11f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> b009fd23f4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 67b361c856 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e182e6e99c (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 9d708b240a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0e68e9f4c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a63cb7e74 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b4320946b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e5ed68fdd3 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> e6e1f4bdd5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0e68e9f4c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
>>>>>>> a63cb7e74 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b4320946b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 87bf55340 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 240977229 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 45ad5d11f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 7acc3ea46 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
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
>>>>>>> cdb4102515 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 7acc3ea46 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> e3a63fee18 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 7acc3ea46 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 0e68e9f4cc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 67b361c856 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e182e6e99c (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 9d708b240a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 76618c8da3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 97ec7d864a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> ad3cedc4db (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> bdbced26d4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0dbede148d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 22dcc8170f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b009fd23f4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a63cb7e74 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> c3b0a1b6e3 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e5ed68fdd3 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> e6e1f4bdd5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a910ae135f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> a63cb7e74 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 8fb0607344 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 240977229 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 45ad5d11f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 7acc3ea46 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> f36b0e8b9 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
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
>>>>>>> a63cb7e740 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> c3b0a1b6e3 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8fb0607344 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> ad3cedc4db (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 22dcc8170f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 67b361c856 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 7acc3ea46 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 908ad4071d (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 7acc3ea46 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> b4320946b4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 7acc3ea46 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 907fb0b08a (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 7acc3ea46 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d9ed077cd1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e182e6e99c (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 9d708b240a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 7f922b040c (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 7acc3ea46 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> e3a63fee1 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 3333975e77 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 0e68e9f4c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 76618c8da3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> b009fd23f4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a63cb7e74 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c3b0a1b6e3 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> ad3cedc4db (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> bc49131942 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 7acc3ea46 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 908ad4071 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9638dc731e (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> b4320946b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 97ec7d864a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 7acc3ea46 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 7bb5f26c3e (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 7acc3ea46 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6d9a5b626c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e5ed68fdd3 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> e6e1f4bdd5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 7f922b040c (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 0e68e9f4c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> a910ae135f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> a63cb7e74 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 8fb0607344 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> ad3cedc4db (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> bc49131942 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> b4320946b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> bdbced26d4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 87bf55340 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 0dbede148d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 240977229 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 22dcc8170f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 45ad5d11f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> b009fd23f4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
    "create filter containing functions with embedded expressions" >> {
      val sft = SimpleFeatureTypes.createType("test", "f0:Integer,f1:Integer")
      val ecql = "min(f0 + 2, 4) < min(f1, 5)"
      val filterGeoMesa = FastFilterFactory.toFilter(sft, ecql)
      val filterGeoTools = ECQL.toFilter(ecql)
      Result.foreach(1 to 10) { i =>
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
        Result.foreach(1 to 10) { j =>
=======
        Result.foreach(1 to 10){ j =>
>>>>>>> 7acc3ea465 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
        Result.foreach(1 to 10) { j =>
>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> cdb4102515 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0e68e9f4cc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> f36b0e8b9 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> a63cb7e740 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 3333975e77 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 76618c8da3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c3b0a1b6e3 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 9638dc731e (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 97ec7d864a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> bc49131942 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> bdbced26d4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 0dbede148d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 22dcc8170f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b009fd23f4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 7f922b040c (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> a910ae135f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8fb0607344 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> ad3cedc4db (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 240977229 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 45ad5d11f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f36b0e8b9 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 67b361c856 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b4320946b4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d9ed077cd1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> f36b0e8b9 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> e182e6e99c (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> f36b0e8b9 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 9d708b240a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> ad3cedc4db (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 22dcc8170f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e3a63fee1 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 3333975e77 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 0e68e9f4c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 76618c8da3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 8fb0607344 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> b009fd23f4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> f36b0e8b9 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> a63cb7e74 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c3b0a1b6e3 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 908ad4071 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9638dc731e (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> b4320946b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 97ec7d864a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 6d9a5b626c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> f36b0e8b9 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> e5ed68fdd3 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> f36b0e8b9 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> e6e1f4bdd5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> e3a63fee1 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 7f922b040c (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 0e68e9f4c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> a910ae135f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8fb0607344 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> ad3cedc4db (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 908ad4071 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> bc49131942 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> b4320946b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> bdbced26d4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 87bf55340 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 0dbede148d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 240977229 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 22dcc8170f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 45ad5d11f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> b009fd23f4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
        Result.foreach(1 to 10) { j =>
=======
        Result.foreach(1 to 10){ j =>
>>>>>>> 7acc3ea46 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
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
>>>>>>> f36b0e8b9a (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
        Result.foreach(1 to 10) { j =>
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> cdb4102515 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
        Result.foreach(1 to 10) { j =>
=======
        Result.foreach(1 to 10){ j =>
>>>>>>> 7acc3ea46 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> e3a63fee18 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
        Result.foreach(1 to 10) { j =>
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 0e68e9f4cc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 67b361c856 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9d708b240a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 3333975e77 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 76618c8da3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9638dc731e (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 97ec7d864a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> ad3cedc4db (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> bc49131942 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> bdbced26d4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0dbede148d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 22dcc8170f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b009fd23f4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a63cb7e74 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> c3b0a1b6e3 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e6e1f4bdd5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 7f922b040c (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> a910ae135f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> a63cb7e74 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 8fb0607344 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 240977229 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 45ad5d11f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
        Result.foreach(1 to 10) { j =>
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f36b0e8b9 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a63cb7e740 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 9d708b240a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c3b0a1b6e3 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 22dcc8170f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e6e1f4bdd5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8fb0607344 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> ad3cedc4db (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 240977229 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
        Result.foreach(1 to 10) { j =>
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 67b361c856 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
        Result.foreach(1 to 10) { j =>
=======
        Result.foreach(1 to 10){ j =>
>>>>>>> 7acc3ea46 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 908ad4071d (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
        Result.foreach(1 to 10) { j =>
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> b4320946b4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
        Result.foreach(1 to 10) { j =>
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
        Result.foreach(1 to 10) { j =>
=======
        Result.foreach(1 to 10){ j =>
>>>>>>> 7acc3ea46 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 907fb0b08a (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
        Result.foreach(1 to 10) { j =>
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d9ed077cd1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
        Result.foreach(1 to 10) { j =>
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f36b0e8b9 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> e182e6e99c (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 9d708b240a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> ad3cedc4db (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 22dcc8170f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e3a63fee1 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 3333975e77 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> a910ae135f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
        Result.foreach(1 to 10) { j =>
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 0e68e9f4c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 76618c8da3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> b009fd23f4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a63cb7e74 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c3b0a1b6e3 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 908ad4071 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9638dc731e (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> bdbced26d4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
        Result.foreach(1 to 10) { j =>
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> b4320946b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 97ec7d864a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
        Result.foreach(1 to 10) { j =>
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
        Result.foreach(1 to 10) { j =>
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
        Result.foreach(1 to 10) { j =>
=======
        Result.foreach(1 to 10){ j =>
>>>>>>> 7acc3ea46 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 7bb5f26c3e (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 0dbede148d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
        Result.foreach(1 to 10) { j =>
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 6d9a5b626c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
        Result.foreach(1 to 10) { j =>
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f36b0e8b9 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> e5ed68fdd3 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> e6e1f4bdd5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> e3a63fee1 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 7f922b040c (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> a910ae135f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> a63cb7e74 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 8fb0607344 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> ad3cedc4db (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 908ad4071 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> bc49131942 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> bdbced26d4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
        Result.foreach(1 to 10) { j =>
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 87bf55340 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 0dbede148d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 240977229 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 22dcc8170f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 45ad5d11f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> b009fd23f4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
          val sf = ScalaSimpleFeature.create(sft, s"${i}_${j}", i, j)
          filterGeoMesa.evaluate(sf) mustEqual filterGeoTools.evaluate(sf)
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
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 907fb0b08a (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7bb5f26c3e (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
    "comparison operators should have consistent semantics" >> {
      val sft = SimpleFeatureTypes.createType("test", "s:String")
      val sf = ScalaSimpleFeature.create(sft, "id", "5")
      FastFilterFactory.toFilter(sft, "s > '100'").evaluate(sf) must beTrue
      FastFilterFactory.toFilter(sft, "s >= '100'").evaluate(sf) must beTrue
      FastFilterFactory.toFilter(sft, "s < '100'").evaluate(sf) must beFalse
      FastFilterFactory.toFilter(sft, "s <= '100'").evaluate(sf) must beFalse
    }
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7acc3ea465 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 7acc3ea46 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> f36b0e8b9a (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 7acc3ea46 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> e3a63fee18 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 7acc3ea46 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 908ad4071d (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 7acc3ea46 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 907fb0b08a (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 7acc3ea46 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 7acc3ea46 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 7bb5f26c3e (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 7acc3ea46 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
  }
}
