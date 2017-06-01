/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.filter.factory

import org.geotools.factory.{CommonFactoryFinder, Hints}
import org.junit.runner.RunWith
import org.locationtech.geomesa.filter.expression.FastPropertyName
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.filter.spatial.BBOX
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FastFilterFactoryTest extends Specification {

  "FastFilterFactory" should {
    "create fast property names" >> {
      val sft = SimpleFeatureTypes.createType("test", "geom:Point:srid=4326")
      FastFilterFactory.toFilter(sft, "bbox(geom, -180, -90, 180, 90)").asInstanceOf[BBOX].getExpression1 must
          beAnInstanceOf[FastPropertyName]
    }
    "be loadable via hints" >> {
      val hints = new Hints()
      hints.put(Hints.FILTER_FACTORY, classOf[FastFilterFactory])
      val ff = CommonFactoryFinder.getFilterFactory2(hints)
      ff must beAnInstanceOf[FastFilterFactory]
    }
    "be loadable via system hints" >> {
      skipped("This might work if property is set globally at jvm startup...")
      System.setProperty("org.opengis.filter.FilterFactory", classOf[FastFilterFactory].getName)
      try {
        Hints.scanSystemProperties()
        val ff = CommonFactoryFinder.getFilterFactory2
        ff must beAnInstanceOf[FastFilterFactory]
      } finally {
        System.clearProperty("org.opengis.filter.FilterFactory")
        Hints.scanSystemProperties()
      }
    }
    "create fast property names via ECQL" >> {
      val sft = SimpleFeatureTypes.createType("test", "geom:Point:srid=4326")
      val bbox = FastFilterFactory.toFilter(sft, "bbox(geom, -180, -90, 180, 90)")
      bbox.asInstanceOf[BBOX].getExpression1 must beAnInstanceOf[FastPropertyName]
    }
  }
}