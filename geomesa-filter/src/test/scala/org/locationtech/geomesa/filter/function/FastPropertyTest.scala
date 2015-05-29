/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */


package org.locationtech.geomesa.filter.function

import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.spatial.BBOXImpl
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FastPropertyTest extends Specification {

  "FastProperty" should {
    "evaluate" >> {
      val filter = ECQL.toFilter("bbox(geom,-180,-90,180,90)")
      val sft = SimpleFeatureTypes.createType("FastPropertyTest", "name:String,*geom:Point:srid=4326")
      val sf = new ScalaSimpleFeature("id", sft)
      sf.setAttributes(Array[AnyRef]("myname", "POINT(45 45)"))
      filter.asInstanceOf[BBOXImpl].setExpression1(new FastProperty(1))
      filter.evaluate(sf)
      filter.evaluate(sf)
      success
    }
  }
}