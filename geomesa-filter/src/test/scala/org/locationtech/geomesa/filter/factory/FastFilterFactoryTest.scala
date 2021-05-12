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
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
import org.opengis.filter.PropertyIsEqualTo
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 12e3a588fc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
import org.opengis.filter.PropertyIsEqualTo
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> f0b9bd8121 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
import org.opengis.filter.PropertyIsEqualTo
<<<<<<< HEAD
>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 59a1fbb96e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d9ed077cd1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 810876750d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
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
>>>>>>> f302a54949 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a83fe4cf7c (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> a788dfac03 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a63cb7e74 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 125ffd0575 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 4c361f07da (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 20f6826d0f (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> a1d28116d4 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> d1fa26b39d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> ef55408e9a (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> f67dd3371e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a9884ff1f6 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> f7cef54062 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 49f2a1c310 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> a16b64a4d1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5430d59024 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 810876750d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 2571e49519 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> d206a68a13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f36b0e8b9a (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d1fa26b39d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> cdb4102515 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e3a63fee18 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 0e68e9f4cc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 67b361c856 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9d708b240a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
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
=======
>>>>>>> bc4062951d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> deb8fd68de (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 8adfab65bb (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 3254b186da (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 7b0539b808 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 982031ef44 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 0086b292ba (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cf57a2c189 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> e15d50d7d3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> bec3ba7d08 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 8de43cccf6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b1a7408547 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 12e3a588fc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c8e013ff91 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 1890b11217 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 912169b5e9 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 9c28193a91 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 1b60c2aeba (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> d294ce49e2 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> ed45e8551f (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 1df8328338 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> dace2085b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 238945d4b3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 516fe7e9c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 8a3e82747d (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 81ee66102b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9943e05ec9 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 53a3e72cbf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d15af7157e (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 947761ec9b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9813375fcc (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> d2eec4be68 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> d94c03a98e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e69b2aa6dc (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 67e4c1354f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 060ac2189c (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 8035cc5893 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
>>>>>>> 9abc630fdf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 570899511d (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 25063957fc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> a63cb7e74 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 8fb060734 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 38b3282770 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> a910ae135 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 07161eaaa6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 7f922b040 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 04f08c23c5 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> c8a325e13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> c9328eddce (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9638dc731 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 97ec7d864 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> ad3cedc4d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> bc4913194 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> bdbced26d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 240977229 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 45ad5d11f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> dace2085b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> c9328eddce (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9abc630fdf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> e182e6e99 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
<<<<<<< HEAD
>>>>>>> 9943e05ec9 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 9d708b240 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
>>>>>>> 53a3e72cbf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 3333975e7 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
<<<<<<< HEAD
>>>>>>> d15af7157e (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 76618c8da (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
>>>>>>> 947761ec9b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> a63cb7e74 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> c3b0a1b6e (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
<<<<<<< HEAD
>>>>>>> 9813375fcc (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> c8a325e13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> c9328eddce (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> ad3cedc4d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 9abc630fdf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d206a68a13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
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
>>>>>>> d206a68a13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> a63cb7e740 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 9d708b240a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
>>>>>>> c3b0a1b6e3 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 22dcc8170f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 238945d4b3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 7b0539b808 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cf57a2c189 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> e15d50d7d3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 1b60c2aeba (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> d294ce49e2 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9943e05ec9 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 3254b186da (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
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
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> dace2085b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 9813375fcc (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> c9328eddce (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 38b3282770 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 9abc630fdf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9d708b240 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 53a3e72cbf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c3b0a1b6e (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> c8a325e13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8fb060734 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> ad3cedc4d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 125ffd0575 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> d206a68a13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d206a68a13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
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
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 810876750d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d9ed077cd1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> c3c5dd0ebb (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f36b0e8b9 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
<<<<<<< HEAD
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
=======
>>>>>>> 7b0539b808 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 982031ef44 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> e15d50d7d3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 912169b5e9 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> d294ce49e2 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> dace2085b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 238945d4b3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 53a3e72cbf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d15af7157e (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 04f08c23c5 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 9abc630fdf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c8a325e13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> c9328eddce (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> ad3cedc4d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> f302a54949 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e3a63fee1 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 3333975e77 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> a910ae135f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 4c361f07da (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 0e68e9f4c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 76618c8da3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> b009fd23f4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cf57a2c189 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 9c28193a91 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 1b60c2aeba (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 238945d4b3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> bec3ba7d08 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> d294ce49e2 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> ed45e8551f (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 908ad4071 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9638dc731e (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> bdbced26d4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a788dfac03 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> b4320946b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
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
>>>>>>> 97ec7d864a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> bc4062951d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1df8328338 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d94c03a98e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 8de43cccf6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 25063957fc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
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
<<<<<<< HEAD
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
<<<<<<< HEAD
>>>>>>> b009fd23f4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> dace2085b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 238945d4b3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 516fe7e9c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> bc4062951d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 907fb0b08 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> 8a3e82747d (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> deb8fd68de (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d9ed077cd (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 81ee66102b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 8adfab65bb (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> e182e6e99 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> 9943e05ec9 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> 3254b186da (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
=======
>>>>>>> 9d708b240 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 53a3e72cbf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 7b0539b808 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
>>>>>>> e3a63fee1 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 3333975e7 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> d15af7157e (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> 982031ef44 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
=======
>>>>>>> 76618c8da (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 947761ec9b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 0086b292ba (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
>>>>>>> a63cb7e74 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> c3b0a1b6e (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> 9813375fcc (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> cf57a2c189 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
=======
>>>>>>> c8a325e13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> c9328eddce (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> e15d50d7d3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
>>>>>>> 908ad4071 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 9638dc731 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> d2eec4be68 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> bec3ba7d08 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
=======
>>>>>>> 97ec7d864 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> d94c03a98e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 8de43cccf6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> 7bb5f26c3 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> e69b2aa6dc (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> b1a7408547 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6d9a5b626 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 67e4c1354f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 12e3a588fc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
>>>>>>> e6e1f4bdd (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f36b0e8b9 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> e5ed68fdd (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> 060ac2189c (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> c8e013ff91 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
=======
=======
>>>>>>> c3c5dd0ebb (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> e6e1f4bdd (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 8035cc5893 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 1890b11217 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
>>>>>>> e3a63fee1 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 7f922b040 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> 04f08c23c5 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> 912169b5e9 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
=======
>>>>>>> a910ae135 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 07161eaaa6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 9c28193a91 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
>>>>>>> a63cb7e74 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 8fb060734 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> 38b3282770 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> 1b60c2aeba (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
=======
>>>>>>> ad3cedc4d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 9abc630fdf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> d294ce49e2 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
>>>>>>> 908ad4071 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> bc4913194 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> 570899511d (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> ed45e8551f (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
=======
>>>>>>> bdbced26d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 25063957fc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 1df8328338 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 74db66044b (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> f0b9bd8121 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f36b0e8b9 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> ef1a0521bd (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> c3c5dd0ebb (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> e3a63fee1 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 20f6826d0f (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 4c361f07da (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> a63cb7e74 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 125ffd0575 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> f302a54949 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 908ad4071 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> a83fe4cf7c (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> a788dfac03 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 32d312dc48 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 59a1fbb96e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a1d28116d4 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> d1fa26b39d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> e3a63fee18 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> ef55408e9a (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> f67dd3371e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 67b361c856 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f36b0e8b9 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> a63cb7e740 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> a9884ff1f6 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 67b361c856 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> f7cef54062 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 908ad4071d (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> 49f2a1c310 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> b4320946b4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> a16b64a4d1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5430d59024 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 810876750d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f36b0e8b9 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> e182e6e99c (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> 2571e49519 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 9d708b240a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d206a68a13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
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
>>>>>>> 59a1fbb96e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a1d28116d4 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> d1fa26b39d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> ef55408e9a (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> f67dd3371e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a9884ff1f6 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> f7cef54062 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 49f2a1c310 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> a16b64a4d1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5430d59024 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 810876750d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 2571e49519 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> d206a68a13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 7acc3ea465 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a1d28116d4 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 7acc3ea46 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> f36b0e8b9a (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> a63cb7e740 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 238945d4b3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> bc4062951d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> deb8fd68de (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 8adfab65bb (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 3254b186da (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 7b0539b808 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 982031ef44 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 0086b292ba (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cf57a2c189 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> e15d50d7d3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> bec3ba7d08 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 8de43cccf6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b1a7408547 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 12e3a588fc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c8e013ff91 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 1890b11217 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 912169b5e9 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 9c28193a91 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 1b60c2aeba (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> d294ce49e2 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> ed45e8551f (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 1df8328338 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 45ad5d11f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> b009fd23f4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> dace2085b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 516fe7e9c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 8a3e82747d (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 81ee66102b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9943e05ec9 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 53a3e72cbf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 947761ec9b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9813375fcc (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> c9328eddce (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d2eec4be68 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> d94c03a98e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e69b2aa6dc (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 67e4c1354f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 060ac2189c (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 8035cc5893 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 04f08c23c5 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 07161eaaa6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 38b3282770 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 9abc630fdf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 570899511d (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 25063957fc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 3333975e7 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> d15af7157e (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 76618c8da (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c3b0a1b6e (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> c8a325e13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9638dc731 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 97ec7d864 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 7f922b040 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> a910ae135 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8fb060734 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> ad3cedc4d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> bc4913194 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> bdbced26d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 20f6826d0f (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 4c361f07da (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 125ffd0575 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> f302a54949 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a83fe4cf7c (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> a788dfac03 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f7cef54062 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 67b361c856 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e182e6e99c (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 9d708b240a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a63cb7e74 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c9328eddce (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d94c03a98e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9abc630fdf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 25063957fc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> b4320946b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
>>>>>>> 87bf55340 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 240977229 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 45ad5d11f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> dace2085b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b4320946b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 516fe7e9c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d9ed077cd (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
>>>>>>> 81ee66102b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e182e6e99 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
<<<<<<< HEAD
>>>>>>> 9943e05ec9 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 9d708b240 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
>>>>>>> 53a3e72cbf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 76618c8da (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 947761ec9b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> a63cb7e74 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> c3b0a1b6e (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> 9813375fcc (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> c8a325e13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> c9328eddce (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> b4320946b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 97ec7d864 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> d94c03a98e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> a910ae135 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 07161eaaa6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> a63cb7e74 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 8fb060734 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> 38b3282770 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> ad3cedc4d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 9abc630fdf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> b4320946b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> bdbced26d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 25063957fc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a9884ff1f6 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> f7cef54062 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e182e6e99c (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 2571e49519 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> d206a68a13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
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
>>>>>>> a9884ff1f6 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> f7cef54062 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 2571e49519 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> d206a68a13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> cdb4102515 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 7acc3ea46 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> e3a63fee18 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> ef1a0521bd (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> c3c5dd0ebb (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
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
>>>>>>> 7acc3ea46 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0e68e9f4cc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> f302a54949 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a788dfac03 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a63cb7e74 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 125ffd0575 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 4c361f07da (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 67b361c856 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e182e6e99c (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
<<<<<<< HEAD
<<<<<<< HEAD
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
=======
>>>>>>> bc4062951d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8adfab65bb (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 3254b186da (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 7b0539b808 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0086b292ba (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cf57a2c189 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> e15d50d7d3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8de43cccf6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9c28193a91 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 1b60c2aeba (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> d294ce49e2 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 1df8328338 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> dace2085b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 238945d4b3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 516fe7e9c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 81ee66102b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9943e05ec9 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 53a3e72cbf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 947761ec9b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9813375fcc (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> d94c03a98e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 9abc630fdf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 25063957fc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> a63cb7e74 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 8fb060734 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 38b3282770 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> a910ae135 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 07161eaaa6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c8a325e13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> c9328eddce (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 97ec7d864 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> ad3cedc4d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> bdbced26d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 240977229 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 45ad5d11f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> dace2085b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> c9328eddce (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9abc630fdf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> e182e6e99 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
<<<<<<< HEAD
>>>>>>> 9943e05ec9 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 9d708b240 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
>>>>>>> 53a3e72cbf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 76618c8da (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 947761ec9b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a63cb7e74 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> c3b0a1b6e (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> c8a325e13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> ad3cedc4d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 2571e49519 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 9d708b240a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d206a68a13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
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
>>>>>>> 2571e49519 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> d206a68a13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
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
=======
>>>>>>> 3254b186da (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 7b0539b808 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cf57a2c189 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> e15d50d7d3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 1b60c2aeba (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> d294ce49e2 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> dace2085b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 238945d4b3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9943e05ec9 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 53a3e72cbf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> c9328eddce (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 38b3282770 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 9abc630fdf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c3b0a1b6e (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 9813375fcc (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> c8a325e13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8fb060734 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> ad3cedc4d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 125ffd0575 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
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
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5430d59024 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 7acc3ea46 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 907fb0b08a (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 810876750d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 7acc3ea46 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d9ed077cd1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e182e6e99c (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 9d708b240a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 3333975e77 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 982031ef44 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 0086b292ba (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9c28193a91 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c3b0a1b6e3 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> ad3cedc4db (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cf57a2c189 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 1b60c2aeba (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> d294ce49e2 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c8a325e13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> e15d50d7d3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> ad3cedc4d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> bc49131942 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 238945d4b3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> d294ce49e2 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> ed45e8551f (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 9638dc731 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> bec3ba7d08 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> bc4913194 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 7acc3ea46 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 908ad4071 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9638dc731e (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> bec3ba7d08 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 8de43cccf6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> ed45e8551f (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 1df8328338 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b4320946b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 97ec7d864a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> bc4062951d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
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
<<<<<<< HEAD
>>>>>>> b009fd23f4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> dace2085b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 238945d4b3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 516fe7e9c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> bc4062951d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> 7acc3ea46 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 907fb0b08 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> 8a3e82747d (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> deb8fd68de (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
=======
>>>>>>> d9ed077cd (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 81ee66102b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 8adfab65bb (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> e182e6e99 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> 9943e05ec9 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> 3254b186da (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
=======
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 9d708b240 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 53a3e72cbf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 7b0539b808 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 3333975e7 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> d15af7157e (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> 982031ef44 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
=======
=======
>>>>>>> 0e68e9f4c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 76618c8da (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 947761ec9b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 0086b292ba (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> c3b0a1b6e (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> 9813375fcc (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> cf57a2c189 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
=======
>>>>>>> c8a325e13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> c9328eddce (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> e15d50d7d3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 9638dc731 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> d2eec4be68 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> bec3ba7d08 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
=======
=======
>>>>>>> b4320946b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 97ec7d864 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> d94c03a98e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 8de43cccf6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
=======
>>>>>>> 7acc3ea46 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 7bb5f26c3 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> e69b2aa6dc (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> b1a7408547 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
=======
=======
>>>>>>> e5ed68fdd (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> e6e1f4bdd (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 7acc3ea46 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6d9a5b626 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 67e4c1354f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 12e3a588fc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
>>>>>>> e6e1f4bdd (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> ef1a0521bd (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> c3c5dd0ebb (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 7acc3ea46 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> f36b0e8b9 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e5ed68fdd (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> 060ac2189c (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> c8e013ff91 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
=======
=======
>>>>>>> f302a54949 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e6e1f4bdd (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 8035cc5893 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 1890b11217 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
>>>>>>> 20f6826d0f (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 7acc3ea46 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> e3a63fee1 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> 7f922b040 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> 04f08c23c5 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> 912169b5e9 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
=======
=======
>>>>>>> 0e68e9f4c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> a910ae135 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 07161eaaa6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 9c28193a91 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 8fb060734 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> 38b3282770 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> 1b60c2aeba (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
=======
>>>>>>> ad3cedc4d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 9abc630fdf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> d294ce49e2 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> bc4913194 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> 570899511d (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> ed45e8551f (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
=======
=======
>>>>>>> b4320946b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> bdbced26d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 25063957fc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 1df8328338 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 7acc3ea46 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 74db66044b (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 7acc3ea46 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> f0b9bd8121 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> ef1a0521bd (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> c3c5dd0ebb (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 20f6826d0f (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 0e68e9f4c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 4c361f07da (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> a63cb7e74 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> 125ffd0575 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> f302a54949 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 7acc3ea46 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 908ad4071 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> a83fe4cf7c (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> b4320946b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> a788dfac03 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 7acc3ea465 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 32d312dc48 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 59a1fbb96e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a1d28116d4 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
=======
>>>>>>> 7acc3ea46 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> cdb4102515 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> d1fa26b39d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 7acc3ea46 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> e3a63fee18 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> ef55408e9a (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
=======
>>>>>>> 7acc3ea46 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 0e68e9f4cc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> f67dd3371e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 67b361c856 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 7acc3ea46 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> f36b0e8b9 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> a63cb7e740 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> a9884ff1f6 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 67b361c856 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> f7cef54062 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 7acc3ea46 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 908ad4071d (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> 49f2a1c310 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
=======
>>>>>>> 7acc3ea46 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> b4320946b4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> a16b64a4d1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5430d59024 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 810876750d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> e182e6e99c (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 2571e49519 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> d206a68a13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
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
>>>>>>> 32d312dc48 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 59a1fbb96e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a1d28116d4 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> d1fa26b39d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> ef55408e9a (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> f67dd3371e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a9884ff1f6 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> f7cef54062 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 49f2a1c310 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> a16b64a4d1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5430d59024 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 810876750d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 2571e49519 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> d206a68a13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
        Result.foreach(1 to 10) { j =>
=======
        Result.foreach(1 to 10){ j =>
>>>>>>> 7acc3ea465 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
<<<<<<< HEAD
=======
        Result.foreach(1 to 10) { j =>
>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> cdb4102515 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0e68e9f4cc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a9884ff1f6 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> f36b0e8b9 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> a63cb7e740 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 238945d4b3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> deb8fd68de (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 8adfab65bb (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 3254b186da (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 7b0539b808 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 982031ef44 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 0086b292ba (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cf57a2c189 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> e15d50d7d3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> bec3ba7d08 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 8de43cccf6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b1a7408547 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 12e3a588fc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c8e013ff91 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 1890b11217 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 912169b5e9 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 9c28193a91 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 1b60c2aeba (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> d294ce49e2 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> ed45e8551f (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 1df8328338 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 516fe7e9c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> bc4062951d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 8a3e82747d (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 81ee66102b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9943e05ec9 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> a83fe4cf7c (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> a788dfac03 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
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
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> dace2085b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> d15af7157e (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 947761ec9b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9813375fcc (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> c9328eddce (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> d94c03a98e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e69b2aa6dc (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 67e4c1354f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 060ac2189c (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 8035cc5893 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 04f08c23c5 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 07161eaaa6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 38b3282770 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 9abc630fdf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 570899511d (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 25063957fc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 9638dc731 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> d2eec4be68 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 97ec7d864 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> bc4913194 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> bdbced26d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9d708b240 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 53a3e72cbf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 3333975e7 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 76618c8da (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c3b0a1b6e (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> c8a325e13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e6e1f4bdd (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 7f922b040 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> a910ae135 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8fb060734 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> ad3cedc4d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 20f6826d0f (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 4c361f07da (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 125ffd0575 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> f302a54949 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f36b0e8b9 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
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
<<<<<<< HEAD
=======
=======
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> ad3cedc4db (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 22dcc8170f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 7b0539b808 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 982031ef44 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> e15d50d7d3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 1890b11217 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 912169b5e9 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> d294ce49e2 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> dace2085b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 238945d4b3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 53a3e72cbf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d15af7157e (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 8035cc5893 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 04f08c23c5 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 9abc630fdf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c8a325e13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> c9328eddce (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> ad3cedc4d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f302a54949 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
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
<<<<<<< HEAD
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
>>>>>>> 9c28193a91 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 1b60c2aeba (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> c3b0a1b6e (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> cf57a2c189 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 8fb060734 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 125ffd0575 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> f36b0e8b9 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> a63cb7e74 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c3b0a1b6e3 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> cf57a2c189 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> e15d50d7d3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 1b60c2aeba (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> d294ce49e2 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 238945d4b3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 908ad4071 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9638dc731e (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> bec3ba7d08 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 8de43cccf6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> ed45e8551f (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 1df8328338 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b4320946b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> bc4062951d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> deb8fd68de (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
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
<<<<<<< HEAD
>>>>>>> b009fd23f4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> dace2085b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 238945d4b3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 516fe7e9c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> bc4062951d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 907fb0b08 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> 8a3e82747d (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> deb8fd68de (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
=======
>>>>>>> d9ed077cd (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 81ee66102b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 8adfab65bb (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
>>>>>>> f36b0e8b9 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> e182e6e99 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> 9943e05ec9 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> 3254b186da (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
=======
>>>>>>> 9d708b240 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 53a3e72cbf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 7b0539b808 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
>>>>>>> e3a63fee1 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 3333975e7 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> d15af7157e (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> 982031ef44 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
=======
>>>>>>> 76618c8da (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 947761ec9b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 0086b292ba (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> c3b0a1b6e (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> 9813375fcc (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> cf57a2c189 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
=======
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> c8a325e13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> c9328eddce (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> e15d50d7d3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 9638dc731 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> d2eec4be68 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> bec3ba7d08 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
=======
=======
>>>>>>> b4320946b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 97ec7d864 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> d94c03a98e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 8de43cccf6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 7bb5f26c3 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> e69b2aa6dc (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> b1a7408547 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
=======
>>>>>>> 6d9a5b626 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 67e4c1354f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 12e3a588fc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
>>>>>>> f36b0e8b9 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> e5ed68fdd (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> 060ac2189c (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> c8e013ff91 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
=======
>>>>>>> e6e1f4bdd (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 8035cc5893 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 1890b11217 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
>>>>>>> e3a63fee1 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 7f922b040 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> 04f08c23c5 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> 912169b5e9 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
=======
>>>>>>> a910ae135 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 07161eaaa6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 9c28193a91 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 8fb060734 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> 38b3282770 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> 1b60c2aeba (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
=======
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> ad3cedc4d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 9abc630fdf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> d294ce49e2 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> bc4913194 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> 570899511d (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> ed45e8551f (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
=======
=======
>>>>>>> b4320946b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> bdbced26d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 25063957fc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 1df8328338 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f0b9bd8121 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> f36b0e8b9 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> ef1a0521bd (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> f36b0e8b9 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> c3c5dd0ebb (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> e3a63fee1 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 20f6826d0f (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 0e68e9f4c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 4c361f07da (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 125ffd0575 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> f302a54949 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 908ad4071 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> a83fe4cf7c (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> b4320946b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> a788dfac03 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a1d28116d4 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> f67dd3371e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a9884ff1f6 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
=======
>>>>>>> f36b0e8b9 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 67b361c856 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> f7cef54062 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> b4320946b4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> a16b64a4d1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> d9ed077cd1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 810876750d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> f36b0e8b9 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> e182e6e99c (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 2571e49519 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> d206a68a13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
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
>>>>>>> a9884ff1f6 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> f7cef54062 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a16b64a4d1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 810876750d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 2571e49519 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> d206a68a13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> f36b0e8b9a (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> f302a54949 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a83fe4cf7c (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> a788dfac03 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a63cb7e74 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 125ffd0575 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 4c361f07da (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 20f6826d0f (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
        Result.foreach(1 to 10) { j =>
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> cdb4102515 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> cdb4102515 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d1fa26b39d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
        Result.foreach(1 to 10) { j =>
=======
        Result.foreach(1 to 10){ j =>
>>>>>>> 7acc3ea46 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> e3a63fee18 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
        Result.foreach(1 to 10) { j =>
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 0e68e9f4cc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 67b361c856 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9d708b240a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
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
=======
>>>>>>> bc4062951d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> deb8fd68de (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 8adfab65bb (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 3254b186da (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 7b0539b808 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 982031ef44 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 0086b292ba (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cf57a2c189 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> e15d50d7d3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> bec3ba7d08 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 8de43cccf6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b1a7408547 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 12e3a588fc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c8e013ff91 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 1890b11217 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 912169b5e9 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 9c28193a91 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 1b60c2aeba (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> d294ce49e2 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> ed45e8551f (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 1df8328338 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> dace2085b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 238945d4b3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 516fe7e9c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8a3e82747d (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 81ee66102b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9943e05ec9 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 53a3e72cbf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d15af7157e (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 947761ec9b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9813375fcc (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> d2eec4be68 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> d94c03a98e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e69b2aa6dc (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 67e4c1354f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 060ac2189c (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 8035cc5893 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
>>>>>>> 9abc630fdf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 570899511d (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 25063957fc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> a63cb7e74 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 8fb060734 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 38b3282770 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> a910ae135 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 07161eaaa6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 7f922b040 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 04f08c23c5 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> c8a325e13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> c9328eddce (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9638dc731 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 97ec7d864 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> ad3cedc4d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> bc4913194 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> bdbced26d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 240977229 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 45ad5d11f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> dace2085b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> c9328eddce (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9abc630fdf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> e182e6e99 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
<<<<<<< HEAD
>>>>>>> 9943e05ec9 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 9d708b240 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
>>>>>>> 53a3e72cbf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 3333975e7 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
<<<<<<< HEAD
>>>>>>> d15af7157e (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 76618c8da (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
>>>>>>> 947761ec9b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> a63cb7e74 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> c3b0a1b6e (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
<<<<<<< HEAD
>>>>>>> 9813375fcc (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> c8a325e13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> c9328eddce (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> ad3cedc4d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 9abc630fdf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a9884ff1f6 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 67b361c856 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f7cef54062 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d206a68a13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
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
>>>>>>> d206a68a13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> a63cb7e740 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 9d708b240a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
>>>>>>> c3b0a1b6e3 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 22dcc8170f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 238945d4b3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 7b0539b808 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cf57a2c189 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> e15d50d7d3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 1b60c2aeba (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> d294ce49e2 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9943e05ec9 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 3254b186da (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
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
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> dace2085b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 9813375fcc (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> c9328eddce (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 38b3282770 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 9abc630fdf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9d708b240 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 53a3e72cbf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c3b0a1b6e (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> c8a325e13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8fb060734 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> ad3cedc4d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 125ffd0575 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> a63cb7e740 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> f7cef54062 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d206a68a13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
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
>>>>>>> d206a68a13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
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
<<<<<<< HEAD
<<<<<<< HEAD
=======
        Result.foreach(1 to 10) { j =>
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
        Result.foreach(1 to 10) { j =>
=======
        Result.foreach(1 to 10){ j =>
>>>>>>> 7acc3ea46 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 907fb0b08a (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 810876750d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
        Result.foreach(1 to 10) { j =>
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d9ed077cd1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> c3c5dd0ebb (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
>>>>>>> 2571e49519 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
        Result.foreach(1 to 10) { j =>
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f36b0e8b9 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
=======
>>>>>>> 7b0539b808 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 982031ef44 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> e15d50d7d3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 912169b5e9 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> d294ce49e2 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> dace2085b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 238945d4b3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 53a3e72cbf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d15af7157e (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 04f08c23c5 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 9abc630fdf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c8a325e13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> c9328eddce (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> ad3cedc4d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
        Result.foreach(1 to 10) { j =>
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> f302a54949 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e3a63fee1 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 3333975e77 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> a910ae135f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 4c361f07da (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
        Result.foreach(1 to 10) { j =>
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 0e68e9f4c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 76618c8da3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> b009fd23f4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cf57a2c189 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 9c28193a91 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 1b60c2aeba (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 238945d4b3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> bec3ba7d08 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> d294ce49e2 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> ed45e8551f (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 908ad4071 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9638dc731e (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> bdbced26d4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a788dfac03 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
        Result.foreach(1 to 10) { j =>
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> b4320946b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
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
>>>>>>> 97ec7d864a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> bc4062951d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1df8328338 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d94c03a98e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 8de43cccf6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 25063957fc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
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
<<<<<<< HEAD
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
<<<<<<< HEAD
>>>>>>> b009fd23f4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> dace2085b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 238945d4b3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 516fe7e9c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> bc4062951d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
        Result.foreach(1 to 10) { j =>
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 907fb0b08 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> 8a3e82747d (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> deb8fd68de (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
=======
=======
        Result.foreach(1 to 10) { j =>
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d9ed077cd (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 81ee66102b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 8adfab65bb (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> e182e6e99 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> 9943e05ec9 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> 3254b186da (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
=======
>>>>>>> 9d708b240 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 53a3e72cbf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 7b0539b808 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
>>>>>>> e3a63fee1 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 3333975e7 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> d15af7157e (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> 982031ef44 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
=======
>>>>>>> 76618c8da (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 947761ec9b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 0086b292ba (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
>>>>>>> a63cb7e74 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> c3b0a1b6e (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> 9813375fcc (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> cf57a2c189 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
=======
>>>>>>> c8a325e13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> c9328eddce (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> e15d50d7d3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
>>>>>>> 908ad4071 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 9638dc731 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> d2eec4be68 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> bec3ba7d08 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
=======
>>>>>>> 97ec7d864 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> d94c03a98e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 8de43cccf6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
        Result.foreach(1 to 10) { j =>
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
=======
=======
        Result.foreach(1 to 10) { j =>
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> 7bb5f26c3 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> e69b2aa6dc (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> b1a7408547 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
=======
=======
        Result.foreach(1 to 10) { j =>
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6d9a5b626 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 67e4c1354f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 12e3a588fc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
>>>>>>> e6e1f4bdd (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
        Result.foreach(1 to 10) { j =>
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f36b0e8b9 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> e5ed68fdd (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> 060ac2189c (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> c8e013ff91 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
=======
=======
>>>>>>> c3c5dd0ebb (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
        Result.foreach(1 to 10) { j =>
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> e6e1f4bdd (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 8035cc5893 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 1890b11217 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
>>>>>>> e3a63fee1 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 7f922b040 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> 04f08c23c5 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> 912169b5e9 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
=======
>>>>>>> a910ae135 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 07161eaaa6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 9c28193a91 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
>>>>>>> a63cb7e74 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 8fb060734 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> 38b3282770 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> 1b60c2aeba (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
=======
>>>>>>> ad3cedc4d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 9abc630fdf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> d294ce49e2 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
>>>>>>> 908ad4071 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> bc4913194 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> 570899511d (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> ed45e8551f (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
=======
>>>>>>> bdbced26d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 25063957fc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 1df8328338 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
        Result.foreach(1 to 10) { j =>
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
        Result.foreach(1 to 10) { j =>
=======
        Result.foreach(1 to 10){ j =>
>>>>>>> 7acc3ea46 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 74db66044b (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
        Result.foreach(1 to 10) { j =>
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> f0b9bd8121 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
        Result.foreach(1 to 10) { j =>
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f36b0e8b9 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> ef1a0521bd (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> c3c5dd0ebb (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> e3a63fee1 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 20f6826d0f (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 4c361f07da (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> a63cb7e74 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 125ffd0575 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> f302a54949 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 908ad4071 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> a83fe4cf7c (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> a788dfac03 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
        Result.foreach(1 to 10) { j =>
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 32d312dc48 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
        Result.foreach(1 to 10) { j =>
>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 59a1fbb96e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f36b0e8b9a (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> a1d28116d4 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
<<<<<<< HEAD
=======
>>>>>>> f67dd3371e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> f36b0e8b9a (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
        Result.foreach(1 to 10) { j =>
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> cdb4102515 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> d1fa26b39d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
        Result.foreach(1 to 10) { j =>
=======
        Result.foreach(1 to 10){ j =>
>>>>>>> 7acc3ea46 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> e3a63fee18 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> ef55408e9a (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
=======
        Result.foreach(1 to 10) { j =>
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 0e68e9f4cc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> f67dd3371e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a63cb7e740 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> a9884ff1f6 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 67b361c856 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> f7cef54062 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
        Result.foreach(1 to 10) { j =>
=======
        Result.foreach(1 to 10){ j =>
>>>>>>> 7acc3ea46 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 908ad4071d (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 49f2a1c310 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> a16b64a4d1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
        Result.foreach(1 to 10) { j =>
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5430d59024 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 810876750d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e182e6e99c (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> 2571e49519 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 9d708b240a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d206a68a13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
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
>>>>>>> 5430d59024 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 907fb0b08a (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
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
=======
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b1a7408547 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 907fb0b08 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> deb8fd68de (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 7bb5f26c3 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> b1a7408547 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 74db66044b (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 32d312dc48 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5430d59024 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
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
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5430d59024 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ef55408e9a (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 49f2a1c310 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 7acc3ea465 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 7acc3ea46 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> f36b0e8b9a (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 7acc3ea46 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> e3a63fee18 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7acc3ea46 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 908ad4071d (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5430d59024 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 7acc3ea46 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 907fb0b08a (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
=======
=======
>>>>>>> deb8fd68de (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b1a7408547 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 7acc3ea46 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 7acc3ea46 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 907fb0b08 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> deb8fd68de (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> 7acc3ea46 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 7bb5f26c3 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> b1a7408547 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 7acc3ea46 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 74db66044b (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 7acc3ea46 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
=======
<<<<<<< HEAD
>>>>>>> 7acc3ea465 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
<<<<<<< HEAD
>>>>>>> 32d312dc48 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 7acc3ea46 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> f36b0e8b9a (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> a1d28116d4 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> ef55408e9a (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
=======
>>>>>>> 7acc3ea46 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 908ad4071d (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
>>>>>>> 49f2a1c310 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5430d59024 (GEOMESA-3067 Find attribute in Filters with Functions with embedded expressions correctly)
  }
}
