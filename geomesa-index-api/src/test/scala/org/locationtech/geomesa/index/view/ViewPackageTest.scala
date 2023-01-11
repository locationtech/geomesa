/***********************************************************************
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.view

import org.geotools.filter.FunctionExpressionImpl
import org.geotools.filter.capability.FunctionNameImpl
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.filter.ExcludeFilter
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.util.Date

@RunWith(classOf[JUnitRunner])
class ViewPackageTest extends Specification with Mockito {

  "view" should {
    "merge date functions correctly" in {
      val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
      val filter = Some(ECQL.toFilter("dtg <= testDate()"))
      mergeFilter(sft, ECQL.toFilter("dtg = testDate()"), filter) must not(beAnInstanceOf[ExcludeFilter])
      ViewPackageTest.response = new Date(ViewPackageTest.response.getTime + 10000)
      mergeFilter(sft, ECQL.toFilter("dtg = testDate()"), filter) must not(beAnInstanceOf[ExcludeFilter])
    }
  }
}

object ViewPackageTest {

  private var response = new Date()

  class TestFunction extends FunctionExpressionImpl(new FunctionNameImpl("testDate", classOf[java.util.Date])) {
    override def evaluate(o: AnyRef): AnyRef = response
  }
}
