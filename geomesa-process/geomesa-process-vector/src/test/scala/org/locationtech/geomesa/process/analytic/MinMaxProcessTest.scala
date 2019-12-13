/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.process.analytic

import org.geotools.data.collection.ListFeatureCollection
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MinMaxProcessTest extends Specification {

  val process = new MinMaxProcess

  val sft = SimpleFeatureTypes.createType("stats", "track:String,dtg:Date,*geom:Point:srid=4326")

  val fc = new ListFeatureCollection(sft)

  val features = (0 until 10).map { i =>
    val sf = new ScalaSimpleFeature(sft, i.toString)
    sf.setAttribute(0, s"t-${i % 2}")
    sf.setAttribute(1, s"2017-05-24T00:00:0$i.000Z")
    sf.setAttribute(2, s"POINT(45 5$i)")
    sf
  }

  step {
    features.foreach(fc.add)
  }

  "MinMaxProcess" should {
    "manually visit a feature collection for dates" in {
      val result = SelfClosingIterator(process.execute(fc, "dtg", null).features).toSeq
      result must haveLength(1)
      result.head.getAttribute(0) mustEqual """{"min":"2017-05-24T00:00:00.000Z","max":"2017-05-24T00:00:09.000Z","cardinality":10}"""
    }
    "manually visit a feature collection for strings" in {
      val result = SelfClosingIterator(process.execute(fc, "track", null).features).toSeq
      result must haveLength(1)
      result.head.getAttribute(0) mustEqual """{"min":"t-0","max":"t-1","cardinality":2}"""
    }
  }
}
