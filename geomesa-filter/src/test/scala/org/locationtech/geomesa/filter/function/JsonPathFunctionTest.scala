/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.filter.function

import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class JsonPathFunctionTest extends Specification {

  "JsonPathFunction" should {
    "evaluate" >> {
      val json = """{ "name" : "john", "tags" : [ "foo", "bar" ], "physical" : { "weight" : 150, "height" : 60 } }"""

      val sft = SimpleFeatureTypes.createType("JsonPathTest", "json:String,*geom:Point:srid=4326")

      val sf = new ScalaSimpleFeature("id", sft)
      sf.setAttribute(0, json)

      ECQL.toFilter("jsonPath(json, '$.name') = 'john'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonExists(json, '$[?(@.name == \"john\")]') = true").evaluate(sf) must beTrue
      ECQL.toFilter("jsonExists(json, '$[?(@.name == \"fred\")]') = true").evaluate(sf) must beFalse
      ECQL.toFilter("jsonExists(json, '$[?(\"foo\" in @.tags && \"bar\" in @.tags)]') = true").evaluate(sf) must beTrue
      ECQL.toFilter("jsonExists(json, '$[?(\"foo\" in @.tags && \"baz\" in @.tags)]') = true").evaluate(sf) must beFalse
      ECQL.toFilter("jsonPath(json, '$.tags[0]') = 'foo'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath(json, '$.physical.weight') = 150").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath(json, '$.physical.weight') = 100").evaluate(sf) must beFalse
    }
  }
}
