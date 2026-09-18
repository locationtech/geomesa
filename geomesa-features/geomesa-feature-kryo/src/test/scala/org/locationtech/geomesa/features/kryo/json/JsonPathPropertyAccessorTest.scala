/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.features.kryo.json

import org.geotools.factory.CommonFactoryFinder
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class JsonPathPropertyAccessorTest extends Specification {

  private val filterFactory = CommonFactoryFinder.getFilterFactory
  val sft = SimpleFeatureTypes.createType("json", "json:String:json=true,s:String,dtg:Date,*geom:Point:srid=4326")

  "JsonPathPropertyAccessor" should {

    "access json values in kryo serialized simple features" in {
      val property = filterFactory.property("$.json.foo")
      val serializer = KryoFeatureSerializer(sft)
      val sf = serializer.getReusableFeature
      sf.setBuffer(serializer.serialize(new ScalaSimpleFeature(sft, "", Array("""{ "foo" : "bar" }""", null, null, null))))
      property.evaluate(sf) mustEqual "bar"
      sf.setBuffer(serializer.serialize(new ScalaSimpleFeature(sft, "", Array("""{ "foo" : "baz" }""", null, null, null))))
      property.evaluate(sf) mustEqual "baz"
    }

    "access json values with spaces in kryo serialized simple features" in {
      val property = filterFactory.property("$.json.['foo path']")
      val serializer = KryoFeatureSerializer(sft)
      val sf = serializer.getReusableFeature
      sf.setBuffer(serializer.serialize(new ScalaSimpleFeature(sft, "", Array("""{ "foo path" : "bar" }""", null, null, null))))
      property.evaluate(sf) mustEqual "bar"
      sf.setBuffer(serializer.serialize(new ScalaSimpleFeature(sft, "", Array("""{ "foo path" : "baz" }""", null, null, null))))
      property.evaluate(sf) mustEqual "baz"
    }

    "return null for invalid paths" in {
      val sf0 = {
        val sf = new SimpleFeatureBuilder(sft).buildFeature("")
        sf.setAttribute(0, """{ "foo" : "bar" }""")
        sf
      }
      val sf1 = {
        val serializer = KryoFeatureSerializer(sft)
        val sf = serializer.getReusableFeature
        sf.setBuffer(serializer.serialize(sf0))
        sf
      }
      forall(Seq(sf0, sf1)) { sf =>
        forall(Seq("$baz", "$.baz", "baz", "$.baz/a")) { path =>
          filterFactory.property(path).evaluate(sf) must beNull
          ECQL.toFilter(s""""$path" = 'bar'""").evaluate(sf) must beFalse
        }
      }
    }
  }
}
