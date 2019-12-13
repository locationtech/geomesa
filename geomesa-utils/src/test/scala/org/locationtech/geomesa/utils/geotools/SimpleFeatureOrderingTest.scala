/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools

import java.util.Date

import org.geotools.feature.simple.SimpleFeatureImpl
import org.geotools.filter.identity.FeatureIdImpl
import org.junit.runner.RunWith
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class SimpleFeatureOrderingTest extends Specification with Mockito {

  val sft = SimpleFeatureTypes.createType("sort", "name:String,age:Int,opt:Int,dtg:Date,*geom:Point:srid=4326")

  val features = new Random(-1L).shuffle {
    (0 until 10).map { i =>
      val sf = new SimpleFeatureImpl(Array.ofDim[AnyRef](5), sft, new FeatureIdImpl(s"0$i"), false)
      sf.setAttribute(0, s"name${i % 2}")
      sf.setAttribute(1, Int.box(i + 20))
      if (i % 2 != 0) {
        sf.setAttribute(2, Int.box(i + 10))
      }
      sf.setAttribute(3, f"2017-02-20T00:00:$i%02d.000Z")
      sf.setAttribute(4, s"POINT(40 ${50 + i})")
      sf
    }
  }

  "SimpleFeatureOrdering" should {

    "sort by feature ID" >> {
      features.sorted(SimpleFeatureOrdering.fid) mustEqual features.sortBy(_.getID)
    }

    "sort by string attributes" >> {
      features.sorted(SimpleFeatureOrdering(0)) mustEqual features.sortBy(f => Option(f.getAttribute(0).asInstanceOf[String]))
    }

    "sort by int attributes" >> {
      features.sorted(SimpleFeatureOrdering(1)) mustEqual features.sortBy(_.getAttribute(1).asInstanceOf[Int])
    }

    "sort by null attributes" >> {
      features.sorted(SimpleFeatureOrdering(2)) mustEqual features.sortBy(f => Option(f.getAttribute(2).asInstanceOf[Integer]))
    }

    "sort by date attributes" >> {
      features.sorted(SimpleFeatureOrdering(3)) mustEqual features.sortBy(_.getAttribute(3).asInstanceOf[Date])
    }
  }
}
