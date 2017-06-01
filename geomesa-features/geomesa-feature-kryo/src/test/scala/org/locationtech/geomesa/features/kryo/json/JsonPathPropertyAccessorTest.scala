/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.kryo.json

import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.expression.PropertyAccessors
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class JsonPathPropertyAccessorTest extends Specification {

  sequential

  val ff = CommonFactoryFinder.getFilterFactory2
  val sft = SimpleFeatureTypes.createType("json", "json:String:json=true,s:String,dtg:Date,*geom:Point:srid=4326")

  "JsonPathPropertyAccessor" should {
    "be available on the classpath" in {
      import scala.collection.JavaConverters._
      val accessors =
        PropertyAccessors.findPropertyAccessors(new ScalaSimpleFeature("", sft), "$.json.foo", classOf[String], null)
      accessors must not(beNull)
      accessors.asScala must contain(JsonPathPropertyAccessor)
    }
    "access json values in simple features" in {
      val property = ff.property("$.json.foo")
      val sf = new ScalaSimpleFeature("", sft)
      sf.setAttribute(0, """{ "foo" : "bar" }""")
      property.evaluate(sf) mustEqual "bar"
      sf.setAttribute(0, """{ "foo" : "baz" }""")
      property.evaluate(sf) mustEqual "baz"
    }

    "access json values in simple features with spaces in the json path" in {
      val property = ff.property("""$.json.['foo path']""")
      val sf = new ScalaSimpleFeature("", sft)
      sf.setAttribute(0, """{ "foo path" : "bar" }""")
      property.evaluate(sf) mustEqual "bar"
      sf.setAttribute(0, """{ "foo path" : "baz" }""")
      property.evaluate(sf) mustEqual "baz"
    }

    "access nested json values in simple features with a json path" in {
      val property = ff.property("""$.json.foo.bar""")
      val sf = new ScalaSimpleFeature("", sft)
      sf.setAttribute(0, """{ "foo" : { "bar" : 0 } }""")
      property.evaluate(sf) mustEqual 0
      sf.setAttribute(0, """{ "foo" : { "bar" : "baz" } }""")
      property.evaluate(sf) mustEqual "baz"
    }

    "access non-json strings in simple features" in {
      val property = ff.property("$.s.foo")
      val sf = new ScalaSimpleFeature("", sft)
      sf.setAttribute(1, """{ "foo" : "bar" }""")
      property.evaluate(sf) mustEqual "bar"
      sf.setAttribute(1, """{ "foo" : "baz" }""")
      property.evaluate(sf) mustEqual "baz"
    }

    "access json values in kryo serialized simple features" in {
      val property = ff.property("$.json.foo")
      val serializer = new KryoFeatureSerializer(sft)
      val sf = serializer.getReusableFeature
      sf.setBuffer(serializer.serialize(new ScalaSimpleFeature("", sft, Array("""{ "foo" : "bar" }""", null, null, null))))
      property.evaluate(sf) mustEqual "bar"
      sf.setBuffer(serializer.serialize(new ScalaSimpleFeature("", sft, Array("""{ "foo" : "baz" }""", null, null, null))))
      property.evaluate(sf) mustEqual "baz"
    }

    "access json values with spaces in kryo serialized simple features" in {
      val property = ff.property("$.json.['foo path']")
      val serializer = new KryoFeatureSerializer(sft)
      val sf = serializer.getReusableFeature
      sf.setBuffer(serializer.serialize(new ScalaSimpleFeature("", sft, Array("""{ "foo path" : "bar" }""", null, null, null))))
      property.evaluate(sf) mustEqual "bar"
      sf.setBuffer(serializer.serialize(new ScalaSimpleFeature("", sft, Array("""{ "foo path" : "baz" }""", null, null, null))))
      property.evaluate(sf) mustEqual "baz"
    }

    "accept json path in ECQL" in {
      val expression = ECQL.toFilter(""""$.json.foo" = 'bar'""")
      val sf = new ScalaSimpleFeature("", sft)
      sf.setAttribute(0, """{ "foo" : "bar" }""")
      expression.evaluate(sf) must beTrue
      sf.setAttribute(0, """{ "foo" : "baz" }""")
      expression.evaluate(sf) must beFalse
    }

    "return null for invalid paths" in {
      val sf0 = {
        val sf = new ScalaSimpleFeature("", sft)
        sf.setAttribute(0, """{ "foo" : "bar" }""")
        sf
      }
      val sf1 = {
        val serializer = new KryoFeatureSerializer(sft)
        val sf = serializer.getReusableFeature
        sf.setBuffer(serializer.serialize(sf0))
        sf
      }
      forall(Seq(sf0, sf1)) { sf =>
        forall(Seq("$baz", "$.baz", "baz", "$.baz/a")) { path =>
          ff.property(path).evaluate(sf) must beNull
          ECQL.toFilter(s""""$path" = 'bar'""").evaluate(sf) must beFalse
        }
      }
    }
  }
}
