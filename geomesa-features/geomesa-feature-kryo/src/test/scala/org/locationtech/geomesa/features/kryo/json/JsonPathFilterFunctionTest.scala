/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.kryo.json

import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class JsonPathFilterFunctionTest extends Specification {

  val json =
    """
      | {
      |   "foo" : "bar",
      |   "foo.foo" : "bar",
      |   "foo foo" : "bar",
      |   "foo_foo" : "bar",
      |   "bar" : {
      |     "boo" : "hiss",
      |     "boo.boo" : "hiss",
      |     "boo boo" : "hiss",
      |     "boo_boo" : "hiss",
      |     },
      |   "bar.bar" : {
      |     "boo" : "hiss",
      |     "boo.boo" : "hiss",
      |     "boo boo" : "hiss",
      |     "boo_boo" : "hiss",
      |     },
      |   "bar bar" : {
      |     "boo" : "hiss",
      |     "boo.boo" : "hiss",
      |     "boo boo" : "hiss",
      |     "boo_boo" : "hiss",
      |     },
      |   "bar_bar" : {
      |     "boo" : "hiss",
      |     "boo.boo" : "hiss",
      |     "boo boo" : "hiss",
      |     "boo_boo" : "hiss",
      |     },
      |   "bar (bar)" : {
      |     "boo (boo)" : "hiss",
      |     },
      |   "bar(bar)" : {
      |     "boo(boo)" : "hiss",
      |     },
      | }
    """.stripMargin
  val sft = SimpleFeatureTypes.createType("json", "json:String:json=true,s:String,dtg:Date,*geom:Point:srid=4326")
  val sf = new ScalaSimpleFeature(sft, "")
  sf.setAttribute(0, json)

  "Json Attr Function" should {
    "not parse invalid paths" in {
		  ECQL.toFilter("jsonPath('$.json.foo.foo') = 'bar'").evaluate(sf) must beFalse
		  ECQL.toFilter("jsonPath('$.json.foo foo') = 'bar'").evaluate(sf) must throwA[RuntimeException]
    }
    "extract root attribute from json" in {
      ECQL.toFilter("jsonPath('$.json.foo') = 'bar'").evaluate(sf) must beTrue
    }

    "extract root attribute with period from json" in {
      ECQL.toFilter("jsonPath('$.json.[''foo.foo'']') = 'bar'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json[''foo.foo'']') = 'bar'").evaluate(sf) must beTrue
    }

    "extract root attribute with space from json" in {
      ECQL.toFilter("jsonPath('$.json.[''foo foo'']') = 'bar'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json[''foo foo'']') = 'bar'").evaluate(sf) must beTrue
    }

    "extract root attribute with underscore from json" in {
      ECQL.toFilter("jsonPath('$.json.[''foo_foo'']') = 'bar'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json[''foo_foo'']') = 'bar'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json.foo_foo') = 'bar'").evaluate(sf) must beTrue
    }

    "extract sub attribute from json" in {
      ECQL.toFilter("jsonPath('$.json.bar.boo') = 'hiss'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json.bar.[''boo'']') = 'hiss'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json.bar[''boo'']') = 'hiss'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json.[''bar''].boo') = 'hiss'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json.[''bar''].[''boo'']') = 'hiss'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json.[''bar''][''boo'']') = 'hiss'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json[''bar''].boo') = 'hiss'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json[''bar''].[''boo'']') = 'hiss'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json[''bar''][''boo'']') = 'hiss'").evaluate(sf) must beTrue
    }

    "extract sub attribute from json with period in sub attribute name" in {
      ECQL.toFilter("jsonPath('$.json.bar.[''boo.boo'']') = 'hiss'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json.bar[''boo.boo'']') = 'hiss'").evaluate(sf) must beTrue
    }

    "extract sub attribute from json with space in sub attribute name" in {
      ECQL.toFilter("jsonPath('$.json.bar.[''boo boo'']') = 'hiss'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json.bar[''boo boo'']') = 'hiss'").evaluate(sf) must beTrue
    }

    "extract sub attribute from json with underscore in sub attribute name" in {
      ECQL.toFilter("jsonPath('$.json.bar.[''boo_boo'']') = 'hiss'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json.bar[''boo_boo'']') = 'hiss'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json.bar.boo_boo') = 'hiss'").evaluate(sf) must beTrue
    }

    "extract sub attribute from json with period in attribute name" in {
      ECQL.toFilter("jsonPath('$.json.[''bar.bar''].boo') = 'hiss'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json[''bar.bar''].boo') = 'hiss'").evaluate(sf) must beTrue
    }

    "extract sub attribute from json with period in attribute and period in sub attribute name" in {
      ECQL.toFilter("jsonPath('$.json.[''bar.bar''].[''boo.boo'']') = 'hiss'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json.[''bar.bar''][''boo.boo'']') = 'hiss'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json[''bar.bar''].[''boo.boo'']') = 'hiss'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json[''bar.bar''][''boo.boo'']') = 'hiss'").evaluate(sf) must beTrue
    }

    "extract sub attribute from json with period in attribute and space in sub attribute name" in {
      ECQL.toFilter("jsonPath('$.json.[''bar.bar''].[''boo boo'']') = 'hiss'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json.[''bar.bar''][''boo boo'']') = 'hiss'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json[''bar.bar''].[''boo boo'']') = 'hiss'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json[''bar.bar''][''boo boo'']') = 'hiss'").evaluate(sf) must beTrue
    }

    "extract sub attribute from json with period in attribute and underscore in sub attribute name" in {
      ECQL.toFilter("jsonPath('$.json.[''bar.bar''].[''boo_boo'']') = 'hiss'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json.[''bar.bar''][''boo_boo'']') = 'hiss'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json.[''bar.bar''].boo_boo') = 'hiss'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json[''bar.bar''].[''boo_boo'']') = 'hiss'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json[''bar.bar''][''boo_boo'']') = 'hiss'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json[''bar.bar''].boo_boo') = 'hiss'").evaluate(sf) must beTrue
    }

    "extract sub attribute from json with space in attribute name" in {
      ECQL.toFilter("jsonPath('$.json.[''bar bar''].boo') = 'hiss'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json[''bar bar''].boo') = 'hiss'").evaluate(sf) must beTrue
    }

    "extract sub attribute from json with space in attribute and period in sub attribute name" in {
      ECQL.toFilter("jsonPath('$.json.[''bar bar''].[''boo.boo'']') = 'hiss'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json.[''bar bar''][''boo.boo'']') = 'hiss'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json[''bar bar''].[''boo.boo'']') = 'hiss'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json[''bar bar''][''boo.boo'']') = 'hiss'").evaluate(sf) must beTrue
    }

    "extract sub attribute from json with space in attribute and space in sub attribute name" in {
      ECQL.toFilter("jsonPath('$.json.[''bar bar''].[''boo boo'']') = 'hiss'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json.[''bar bar''][''boo boo'']') = 'hiss'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json[''bar bar''].[''boo boo'']') = 'hiss'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json[''bar bar''][''boo boo'']') = 'hiss'").evaluate(sf) must beTrue
    }

    "extract sub attribute from json with space in attribute and underscore in sub attribute name" in {
      ECQL.toFilter("jsonPath('$.json.[''bar bar''].[''boo_boo'']') = 'hiss'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json.[''bar bar''][''boo_boo'']') = 'hiss'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json.[''bar bar''].boo_boo') = 'hiss'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json[''bar bar''].[''boo_boo'']') = 'hiss'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json[''bar bar''][''boo_boo'']') = 'hiss'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json[''bar bar''].boo_boo') = 'hiss'").evaluate(sf) must beTrue
    }

    "extract sub attribute from json with underscore in attribute name" in {
      ECQL.toFilter("jsonPath('$.json.[''bar_bar''].boo') = 'hiss'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json[''bar_bar''].boo') = 'hiss'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json.bar_bar.boo') = 'hiss'").evaluate(sf) must beTrue
    }

    "extract sub attribute from json with underscore in attribute and period in sub attribute name" in {
      ECQL.toFilter("jsonPath('$.json.[''bar_bar''].[''boo.boo'']') = 'hiss'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json.[''bar_bar''][''boo.boo'']') = 'hiss'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json[''bar_bar''].[''boo.boo'']') = 'hiss'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json[''bar_bar''][''boo.boo'']') = 'hiss'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json.bar_bar.[''boo.boo'']') = 'hiss'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json.bar_bar[''boo.boo'']') = 'hiss'").evaluate(sf) must beTrue
    }

    "extract sub attribute from json with underscore in attribute and space in sub attribute name" in {
      ECQL.toFilter("jsonPath('$.json.[''bar_bar''].[''boo boo'']') = 'hiss'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json.[''bar_bar''][''boo boo'']') = 'hiss'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json[''bar_bar''].[''boo boo'']') = 'hiss'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json[''bar_bar''][''boo boo'']') = 'hiss'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json.bar_bar.[''boo boo'']') = 'hiss'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json.bar_bar[''boo boo'']') = 'hiss'").evaluate(sf) must beTrue
    }

    "extract sub attribute from json with underscore in attribute and underscore in sub attribute name" in {
      ECQL.toFilter("jsonPath('$.json.[''bar_bar''].[''boo_boo'']') = 'hiss'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json.[''bar_bar''][''boo_boo'']') = 'hiss'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json.[''bar_bar''].boo_boo') = 'hiss'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json[''bar_bar''].[''boo_boo'']') = 'hiss'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json[''bar_bar''][''boo_boo'']') = 'hiss'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json[''bar_bar''].boo_boo') = 'hiss'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json.bar_bar.[''boo_boo'']') = 'hiss'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json.bar_bar[''boo_boo'']') = 'hiss'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json.bar_bar.boo_boo') = 'hiss'").evaluate(sf) must beTrue
    }

    "extract sub attribute from json with space and round brackets in attribute and space and round brackets in sub attribute name" in {
      ECQL.toFilter("jsonPath('$.json.[''bar (bar)''].[''boo (boo)'']') = 'hiss'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json.[''bar (bar)''][''boo (boo)'']') = 'hiss'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json[''bar (bar)''].[''boo (boo)'']') = 'hiss'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json[''bar (bar)''][''boo (boo)'']') = 'hiss'").evaluate(sf) must beTrue
    }

    "extract sub attribute from json with round brackets in attribute and round brackets in sub attribute name" in {
      ECQL.toFilter("jsonPath('$.json.[''bar(bar)''].[''boo(boo)'']') = 'hiss'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json.[''bar(bar)''][''boo(boo)'']') = 'hiss'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json[''bar(bar)''].[''boo(boo)'']') = 'hiss'").evaluate(sf) must beTrue
      ECQL.toFilter("jsonPath('$.json[''bar(bar)''][''boo(boo)'']') = 'hiss'").evaluate(sf) must beTrue
    }
  }
}
