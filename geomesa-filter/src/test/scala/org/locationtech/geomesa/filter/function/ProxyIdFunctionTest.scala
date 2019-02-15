/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.filter.function

import java.util.Collections

import org.geotools.filter.text.ecql.ECQL
import org.geotools.filter.visitor.SimplifyingFilterVisitor
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.Configs
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ProxyIdFunctionTest extends Specification {

  "ProxyIdFunction" should {
    "consistently and uniquely evaluate feature ids" >> {
      val sft = SimpleFeatureTypes.createType("foo", "name:String,dtg:Date,*geom:Point:srid=4326")
      val sf1 = ScalaSimpleFeature.create(sft, "fid0", "foo", "2017-01-01T00:00:00.000Z", "POINT (45 50)")
      val sf2 = ScalaSimpleFeature.create(sft, "fid1", "foo", "2017-01-01T00:00:00.000Z", "POINT (45 50)")
      val fn = new ProxyIdFunction()
      fn.setParameters(Collections.emptyList())
      val result = fn.evaluate(sf1)
      fn.evaluate(sf1) mustEqual result
      fn.evaluate(sf2) must not(beEqualTo(result))
    }
    "consistently and uniquely evaluate uuids" >> {
      val sft = SimpleFeatureTypes.createType("foo", s"name:String,dtg:Date,*geom:Point:srid=4326;${Configs.FID_UUID_KEY}=true")
      val sf1 = ScalaSimpleFeature.create(sft, "28a12c18-e5ae-4c04-ae7b-bf7cdbfaf234", "foo", "2017-01-01T00:00:00.000Z", "POINT (45 50)")
      val sf2 = ScalaSimpleFeature.create(sft, "28a12c18-e5ae-4c04-ae7b-bf7cdbfaf235", "foo", "2017-01-01T00:00:00.000Z", "POINT (45 50)")
      val fn = new ProxyIdFunction()
      fn.setParameters(Collections.emptyList())
      val result = fn.evaluate(sf1)
      fn.evaluate(sf1) mustEqual result
      fn.evaluate(sf2) must not(beEqualTo(result))
    }
    "fail for invalid uuids" >> {
      val sft = SimpleFeatureTypes.createType("foo", s"name:String,dtg:Date,*geom:Point:srid=4326;${Configs.FID_UUID_KEY}=true")
      val sf1 = ScalaSimpleFeature.create(sft, "not a uuid", "foo", "2017-01-01T00:00:00.000Z", "POINT (45 50)")
      val fn = new ProxyIdFunction()
      fn.setParameters(Collections.emptyList())
      fn.evaluate(sf1) must throwAn[IllegalArgumentException]
    }
    "work with simplifying filter visitor" >> {
      val filter = ECQL.toFilter("proxyId() = 20")
      val simplified = filter.accept(new SimplifyingFilterVisitor(), null)
      simplified mustEqual filter
    }
  }
}